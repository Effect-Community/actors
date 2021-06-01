import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import type { Exit } from "@effect-ts/core/Effect/Exit"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as Sh from "@effect-ts/core/Effect/Schedule"
import * as Scope from "@effect-ts/core/Effect/Scope"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as OT from "@effect-ts/core/OptionT"
import { chainF } from "@effect-ts/core/Prelude"
import type { _A } from "@effect-ts/core/Utils"
import type { ZooError } from "@effect-ts/keeper"
import * as K from "@effect-ts/keeper"
import { KeeperClient } from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"
import { tuple } from "@effect-ts/system/Function"
import type { NoSuchElementException } from "@effect-ts/system/GlobalExceptions"

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
import * as AS from "../ActorSystem"
import type { Throwable } from "../common"
import type * as AM from "../Message"
import * as SUP from "../Supervisor"

export const ClusterConfigSym = Symbol()

const EO = pipe(OT.monad(T.Monad), (M) => ({
  map: M.map,
  chain: chainF(M),
  chainT:
    <R2, E2, A, B>(f: (a: A) => T.Effect<R2, E2, B>) =>
    <R, E>(fa: T.Effect<R, E, O.Option<A>>): T.Effect<R2 & R, E2 | E, O.Option<B>> =>
      chainF(M)((a: A) => T.map_(f(a), O.some))(fa)
}))

export interface ConfigInput {
  readonly host: string
  readonly port: number
}

export interface ClusterConfig extends ConfigInput {
  readonly [ClusterConfigSym]: typeof ClusterConfigSym
}

export function makeClusterConfig(_: { host: string; port: number }): ClusterConfig {
  return {
    [ClusterConfigSym]: ClusterConfigSym,
    ..._
  }
}

export function StaticClusterConfig(cfg: ConfigInput) {
  return L.fromEffect(ClusterConfig)(T.succeedWith(() => makeClusterConfig(cfg)))
}

export const ClusterConfig = tag<ClusterConfig>()

export const ClusterSym = Symbol()

export class HostPort extends Tagged("HostPort")<{
  readonly host: string
  readonly port: number
}> {}

export class ClusterException extends Tagged("ClusterException")<{
  readonly message: string
}> {}

export const makeCluster = M.gen(function* (_) {
  const { host, port } = yield* _(ClusterConfig)
  const cli = yield* _(K.KeeperClient)
  const system = yield* _(AS.ActorSystemTag)
  const clusterDir = `/cluster/${system.actorSystemName}`
  const membersDir = `${clusterDir}/members`

  yield* _(cli.mkdir(membersDir))

  const prefix = `${membersDir}/member_`

  const nodePath = yield* _(
    cli
      .create(prefix, {
        mode: "EPHEMERAL_SEQUENTIAL",
        data: Buffer.from(JSON.stringify({ host, port }))
      })
      ["|>"](M.make((p) => cli.remove(p)["|>"](T.orDie)))
  )

  const nodeId = `member_${nodePath.substr(prefix.length)}`

  const memberHostPort = yield* _(
    T.memoize(
      (member: string): T.Effect<unknown, K.ZooError, HostPort> =>
        pipe(
          cli.getData(`${membersDir}/${member}`),
          T.chain(
            O.fold(
              () =>
                T.die(
                  new ClusterException({
                    message: `cannot find metadata on path: ${membersDir}/${member}`
                  })
                ),
              (b) => T.succeed(b)
            )
          ),
          T.map((b) => new HostPort(JSON.parse(b.toString("utf8"))))
        )
    )
  )

  const members = pipe(cli.getChildren(membersDir), T.chain(T.forEach(memberHostPort)))

  const leader = pipe(
    cli.getChildren(membersDir),
    T.map(Chunk.head),
    EO.chainT(memberHostPort)
  )

  const leaderId = (scope: string) =>
    pipe(
      cli.getChildren(scope),
      T.map(Chunk.head),
      T.chain(T.getOrFail),
      T.retry(
        pipe(
          Sh.windowed(100),
          Sh.whileInput(
            (u: ZooError | NoSuchElementException) =>
              u._tag === "NoSuchElementException"
          )
        )
      ),
      T.catch("_tag", "NoSuchElementException", () =>
        T.fail(new ClusterException({ message: `cannot find a leader for ${scope}` }))
      )
    )

  function runOnLeader(scope: string) {
    return <R, E, R2, E2>(
      onLeader: T.Effect<R, E, never>,
      whileFollower: (leader: string) => T.Effect<R2, E2, never>
    ) => {
      return T.gen(function* (_) {
        const leader = yield* _(leaderId(scope))

        while (1) {
          if (leader === nodeId) {
            yield* _(onLeader)
          } else {
            yield* _(
              T.race_(cli.waitDelete(`${scope}/${leader}`), whileFollower(leader))
            )
          }
        }
      })
    }
  }

  return {
    [ClusterSym]: ClusterSym,
    nodeId,
    members,
    leader,
    clusterDir,
    memberHostPort,
    leaderId,
    runOnLeader
  } as const
})

export interface Cluster extends _A<typeof makeCluster> {}
export const Cluster = tag<Cluster>()
export const LiveCluster = L.fromManaged(Cluster)(makeCluster)

export interface Singleton<ID extends string, F1 extends AM.AnyMessage> {
  id: ID
  members: T.Effect<unknown, K.ZooError, Chunk.Chunk<string>>
  leader: T.Effect<unknown, K.ZooError, O.Option<string>>
  membersDir: string
  actor: ActorRef<F1>
}

export const makeSingleton =
  <ID extends string>(id: ID) =>
  <R, R2, E2, S, F1 extends AM.AnyMessage>(
    stateful: A.AbstractStateful<R, S, F1>,
    init: T.Effect<R2, E2, S>
  ) => {
    const tag_ = tag<Singleton<ID, F1>>()
    return {
      Tag: tag_,
      ask: <A extends F1>(fa: A) => T.accessServiceM(tag_)((_) => _.actor.ask(fa)),
      tell: <A extends F1>(fa: A) => T.accessServiceM(tag_)((_) => _.actor.tell(fa)),
      actor: T.accessService(tag_)((_) => _.actor),
      Live: L.fromManaged(tag_)(
        M.gen(function* (_) {
          const cluster = yield* _(Cluster)
          const system = yield* _(AS.ActorSystemTag)
          const cli = yield* _(KeeperClient)
          const membersDir = `${cluster.clusterDir}/singletons/${id}/members`

          yield* _(cli.mkdir(membersDir))

          yield* _(
            pipe(
              cli.create(`${membersDir}/${cluster.nodeId}`, { mode: "EPHEMERAL" }),
              M.make((p) => cli.remove(p)["|>"](T.orDie))
            )
          )

          const members = cli.getChildren(membersDir)

          const leader = pipe(cli.getChildren(membersDir), T.map(Chunk.head))

          const queue = yield* _(Q.makeUnbounded<A.PendingMessage<F1>>())

          yield* _(
            pipe(
              cluster.runOnLeader(membersDir)(
                M.gen(function* (_) {
                  const state = yield* _(init)
                  const ref: ActorRef<F1> = yield* _(
                    system.make(`singleton-${id}`, SUP.none, state, stateful)
                  )

                  return yield* _(
                    M.fromEffect(
                      T.gen(function* (_) {
                        const [a, p] = yield* _(Q.take(queue))

                        yield* _(ref.ask(a)["|>"](T.to(p)))
                      })["|>"](T.forever)
                    )
                  )
                })["|>"](M.useNow),
                (leader) =>
                  T.gen(function* (_) {
                    const [a, p] = yield* _(Q.take(queue))

                    yield* _(
                      pipe(
                        T.fail(
                          `cannot process: ${JSON.stringify(a)}, send to ${leader}`
                        ),
                        T.to(p)
                      )
                    )
                  })["|>"](T.forever)
              ),
              T.forkManaged
            )
          )

          const ask = <A extends F1>(fa: A) => {
            return pipe(
              P.make<Throwable, AM.ResponseOf<A>>(),
              T.tap((promise) => Q.offer_(queue, tuple(fa, promise))),
              T.chain(P.await)
            )
          }

          const tell = <A extends F1>(fa: A) => {
            return pipe(
              P.make<Throwable, any>(),
              T.chain((promise) => Q.offer_(queue, tuple(fa, promise))),
              T.zipRight(T.unit)
            )
          }

          const scope = yield* _(
            Scope.makeScope<Exit<unknown, unknown>>()["|>"](
              M.makeExit((_, ex) => _.close(ex))
            )
          )

          const actor = yield* _(
            pipe(
              system.make(
                `singleton/${id}`,
                SUP.none,
                0,
                A.stateful(
                  stateful.messages,
                  S.unknown
                )(
                  () => (msg) =>
                    pipe(
                      ask(new stateful.messages[msg._tag](msg.payload)),
                      T.chain((res) => msg.return(0, res))
                    )
                )
              ),
              T.overrideForkScope(scope.scope)
            )
          )

          return {
            id,
            members,
            leader,
            membersDir,
            actor: {
              ask,
              tell,
              messages: actor.messages,
              path: actor.path,
              stop: actor.stop
            }
          }
        })
      )
    }
  }
