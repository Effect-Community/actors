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

export const EO = pipe(OT.monad(T.Monad), (M) => ({
  map: M.map,
  chain: chainF(M),
  chainT:
    <R2, E2, A, B>(f: (a: A) => T.Effect<R2, E2, B>) =>
    <R, E>(fa: T.Effect<R, E, O.Option<A>>): T.Effect<R2 & R, E2 | E, O.Option<B>> =>
      chainF(M)((a: A) => T.map_(f(a), O.some))(fa)
}))

export const ClusterSym = Symbol()

export class HostPort extends Tagged("HostPort")<{
  readonly host: string
  readonly port: number
}> {}

export class ClusterException extends Tagged("ClusterException")<{
  readonly message: string
}> {}

export class ActorError extends Tagged("ActorError")<{
  readonly message: string
}> {}

export const makeCluster = M.gen(function* (_) {
  const cli = yield* _(K.KeeperClient)
  const system = yield* _(AS.ActorSystemTag)
  const clusterDir = `/cluster/${system.actorSystemName}`
  const membersDir = `${clusterDir}/members`

  if (O.isNone(system.remoteConfig)) {
    return yield* _(
      T.die(`actor system ${system.actorSystemName} doesn't support remoting`)
    )
  }

  yield* _(cli.mkdir(membersDir))

  const prefix = `${membersDir}/member_`

  const nodePath = yield* _(
    cli
      .create(prefix, {
        mode: "EPHEMERAL_SEQUENTIAL",
        data: Buffer.from(
          JSON.stringify({
            host: system.remoteConfig.value.host,
            port: system.remoteConfig.value.port
          })
        )
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
        while (1) {
          const leader = yield* _(leaderId(scope))
          const nodeMapped = yield* _(
            pipe(
              cli.getData(`${scope}/${leader}`),
              T.chain((data) => T.getOrFail(data)["|>"](T.orDie)),
              T.map((data) => data.toString("utf8"))
            )
          )
          if (nodeMapped === nodeId) {
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
    runOnLeader,
    system
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
  <R, R2, E2, S, F1 extends AM.AnyMessage, R3, E3>(
    stateful: A.AbstractStateful<R, S, F1>,
    init: T.Effect<R2, E2, S>,
    side: (self: ActorRef<F1>) => T.Effect<R3, E3, never>
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
              cli.create(`${membersDir}/worker_`, {
                mode: "EPHEMERAL_SEQUENTIAL",
                data: Buffer.from(cluster.nodeId)
              }),
              M.make((p) => cli.remove(p)["|>"](T.orDie))
            )
          )

          const members = cli.getChildren(membersDir)

          const leader = pipe(cli.getChildren(membersDir), T.map(Chunk.head))

          const queue = yield* _(Q.makeUnbounded<A.PendingMessage<F1>>())

          const ask = <A extends F1>(fa: A) => {
            return pipe(
              P.make<Throwable, AM.ResponseOf<A>>(),
              T.tap((promise) => Q.offer_(queue, tuple(fa, promise))),
              T.chain(P.await)
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
                `singleton/proxy/${id}`,
                SUP.none,
                A.stateful(
                  stateful.messages,
                  S.unknown
                )(() => (msg) => {
                  return pipe(
                    // @ts-expect-error
                    ask(msg.payload),
                    T.chain((res) => msg.return(0, res))
                  )
                }),
                0
              ),
              T.overrideForkScope(scope.scope)
            )
          )

          yield* _(
            pipe(
              cluster.runOnLeader(membersDir)(
                M.gen(function* (_) {
                  const state = yield* _(init)
                  const ref: ActorRef<F1> = yield* _(
                    system.make(`singleton/leader/${id}`, SUP.none, stateful, state)
                  )

                  return yield* _(
                    M.fromEffect(
                      T.gen(function* (_) {
                        const [a, p] = yield* _(Q.take(queue))

                        yield* _(ref.ask(a)["|>"](T.to(p)))
                      })["|>"](T.forever)
                    )
                  )
                })
                  ["|>"](M.useNow)
                  ["|>"](T.race(side(actor))),
                (leader) =>
                  T.gen(function* (_) {
                    const all = yield* _(Q.takeAll(queue))
                    const { host, port } = yield* _(cluster.memberHostPort(leader))

                    const recipient = `zio://${system.actorSystemName}@${host}:${port}/singleton/proxy/${id}`

                    for (const [a, p] of all) {
                      const act = yield* _(system.select(recipient))
                      yield* _(pipe(act.ask(a), T.to(p)))
                    }

                    if (all.length === 0) {
                      yield* _(T.sleep(5))
                    }
                  })["|>"](T.forever)
              ),
              T.race(cli.monitor),
              T.forkManaged
            )
          )

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
