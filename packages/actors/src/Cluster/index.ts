import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as Sh from "@effect-ts/core/Effect/Schedule"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as OT from "@effect-ts/core/OptionT"
import { chainF } from "@effect-ts/core/Prelude"
import { AtomicReference } from "@effect-ts/core/Support/AtomicReference"
import type { _A } from "@effect-ts/core/Utils"
import type { ZooError } from "@effect-ts/keeper"
import * as K from "@effect-ts/keeper"
import { KeeperClient } from "@effect-ts/keeper"
import { tuple } from "@effect-ts/system/Function"
import type { NoSuchElementException } from "@effect-ts/system/GlobalExceptions"

import type * as A from "../Actor"
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

  const leaderId = (scope: string) => pipe(cli.getChildren(scope), T.map(Chunk.head))

  function runOnLeader(scope: string) {
    return <R, E, R2, E2>(
      onLeader: T.Effect<R, E, never>,
      whileFollower: T.Effect<R2, E2, never>
    ) => {
      return T.gen(function* (_) {
        const leader = yield* _(
          pipe(
            leaderId(scope),
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
              T.die(
                new ClusterException({ message: `cannot find a leader for ${scope}` })
              )
            )
          )
        )

        while (1) {
          if (leader === nodeId) {
            yield* _(onLeader)
          } else {
            yield* _(T.race_(cli.waitDelete(`${scope}/${leader}`), whileFollower))
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

          const localRef = new AtomicReference(O.emptyOf<ActorRef<F1>>())

          const queue = yield* _(Q.makeUnbounded<A.PendingMessage<F1>>())

          yield* _(
            pipe(
              cluster.runOnLeader(membersDir)(
                pipe(
                  init,
                  T.chain((s) => system.make(id, SUP.none, s, stateful)),
                  T.bracket(
                    (ref) =>
                      T.gen(function* (_) {
                        const [a, p] = yield* _(Q.take(queue))

                        yield* _(ref.ask(a)["|>"](T.to(p)))
                      })["|>"](T.forever),
                    (_) =>
                      pipe(
                        T.succeedWith(() => localRef.set(O.none)),
                        T.zipRight(_.stop),
                        T.orDie
                      )
                  )
                ),
                T.gen(function* (_) {
                  const [a, p] = yield* _(Q.take(queue))

                  yield* _(
                    T.fail(`cannot process: ${JSON.stringify(a)}`)["|>"](T.to(p))
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

          const tell = (fa: F1) => {
            return pipe(
              P.make<Throwable, any>(),
              T.chain((promise) => Q.offer_(queue, tuple(fa, promise))),
              T.zipRight(T.unit)
            )
          }

          const actor: ActorRef<F1> = {
            messages: stateful.messages,
            path: T.die("Not Implemented"),
            stop: T.die("Cannot be stopped"),
            ask,
            tell
          }

          return {
            id,
            members,
            leader,
            membersDir,
            actor
          }
        })
      )
    }
  }
