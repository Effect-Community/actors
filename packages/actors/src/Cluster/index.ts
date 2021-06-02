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
import * as Ex from "@effect-ts/express"
import type { ZooError } from "@effect-ts/keeper"
import * as K from "@effect-ts/keeper"
import { KeeperClient } from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"
import * as Encoder from "@effect-ts/schema/Encoder"
import * as Parser from "@effect-ts/schema/Parser"
import { tuple } from "@effect-ts/system/Function"
import type { NoSuchElementException } from "@effect-ts/system/GlobalExceptions"
import * as exp from "express"

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
import { withSystem } from "../ActorRef"
import * as AS from "../ActorSystem"
import { ClusterConfig } from "../ClusterConfig"
import type { Throwable } from "../common"
import * as Envelope from "../Envelope"
import type * as AM from "../Message"
import * as SUP from "../Supervisor"

const EO = pipe(OT.monad(T.Monad), (M) => ({
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

export const CallPayload = S.props({
  _tag: S.prop(S.string),
  op: S.prop(S.literal("Ask", "Tell", "Stop")).opt(),
  path: S.prop(S.string),
  request: S.prop(S.unknown).opt()
})

export const decodePayload = CallPayload.Parser["|>"](S.condemnFail)
export const encodePayload = CallPayload.Encoder

export class ActorError extends Tagged("ActorError")<{
  readonly message: string
}> {}

export const makeCluster = M.gen(function* (_) {
  const { host, port, sysName } = yield* _(ClusterConfig)
  const cli = yield* _(K.KeeperClient)
  const system = yield* _(AS.make(sysName, O.none, O.some({ host, port })))
  const clusterDir = `/cluster/${system.actorSystemName}`
  const membersDir = `${clusterDir}/members`

  yield* _(cli.mkdir(membersDir))

  const prefix = `${membersDir}/member_`

  const p = yield* _(P.make<never, void>())

  yield* _(
    T.forkManaged(
      pipe(
        T.gen(function* (_) {
          yield* _(
            Ex.post("/cmd", Ex.classic(exp.json()), (req, res) =>
              T.gen(function* (_) {
                const body = req.body
                const payload = yield* _(decodePayload(body))

                const actor = yield* _(system.unsafeLookup(payload.path))

                if (actor._tag === "Some") {
                  switch (payload.op) {
                    case "Ask": {
                      const msgArgs = yield* _(
                        S.condemnFail((u) =>
                          withSystem(system)(() =>
                            Parser.for(
                              actor.value.messages[payload._tag].RequestSchema
                            )(u)
                          )
                        )(payload.request)
                      )

                      const msg = new actor.value.messages[payload._tag](msgArgs)

                      const resp = yield* _(
                        actor.value.ask(msg)["|>"](
                          T.mapError(
                            (s) =>
                              new ActorError({
                                message: `actor error: ${JSON.stringify(s)}`
                              })
                          )
                        )
                      )

                      res.send(
                        JSON.stringify({
                          response: Encoder.for(
                            actor.value.messages[payload._tag].ResponseSchema
                          )(resp)
                        })
                      )

                      break
                    }
                    case "Tell": {
                      const msgArgs = yield* _(
                        S.condemnFail((u) =>
                          withSystem(system)(() =>
                            Parser.for(
                              actor.value.messages[payload._tag].RequestSchema
                            )(u)
                          )
                        )(payload.request)
                      )

                      const msg = new actor.value.messages[payload._tag](msgArgs)

                      yield* _(
                        actor.value.tell(msg)["|>"](
                          T.mapError(
                            (s) =>
                              new ActorError({
                                message: `actor error: ${JSON.stringify(s)}`
                              })
                          )
                        )
                      )

                      res.send(JSON.stringify({ tell: true }))

                      break
                    }
                    case "Stop": {
                      const stops = yield* _(
                        actor.value.stop["|>"](
                          T.mapError(
                            (s) =>
                              new ActorError({
                                message: `actor error: ${JSON.stringify(s)}`
                              })
                          )
                        )
                      )

                      res.send(JSON.stringify({ stops: Chunk.toArray(stops) }))

                      break
                    }
                  }
                } else {
                  yield* _(
                    T.succeedWith(() => {
                      res.status(500).send({ message: `actor not found` })
                    })
                  )
                }
              })
                ["|>"](
                  T.catchTag("CondemnException", (s) =>
                    T.succeedWith(() => res.status(500).send({ message: s.message }))
                  )
                )
                ["|>"](
                  T.catchTag("ActorError", (s) =>
                    T.succeedWith(() => res.status(500).send({ message: s.message }))
                  )
                )
                ["|>"](
                  T.catchAll((s) =>
                    T.succeedWith(() =>
                      res.status(500).send({ message: `invalid actor path: ${s.path}` })
                    )
                  )
                )
            )
          )
          yield* _(P.succeed_(p, void 0))
          return yield* _(T.never)
        }),
        T.provideSomeLayer(Ex.LiveExpress("0.0.0.0", port))
      )
    )
  )

  yield* _(P.await(p))

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
        while (1) {
          const leader = yield* _(leaderId(scope))
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

  function ask(path: string) {
    return <F extends AM.AnyMessage>(
      msg: F
    ): T.Effect<unknown, unknown, AM.ResponseOf<F>> =>
      // @ts-expect-error
      system.runEnvelope({
        command: Envelope.ask(msg),
        recipient: path
      })
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
    system,
    host,
    port,
    ask
  } as const
})

export interface Cluster extends _A<typeof makeCluster> {}
export const Cluster = tag<Cluster>()
export const LiveCluster = L.fromManaged(Cluster)(makeCluster)[">+>"](
  L.fromEffect(AS.ActorSystemTag)(T.accessService(Cluster)((_) => _.system))
)

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
              cli.create(`${membersDir}/${cluster.nodeId}`, { mode: "EPHEMERAL" }),
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
                0,
                A.stateful(
                  stateful.messages,
                  S.unknown
                )(() => (msg) => {
                  return pipe(
                    // @ts-expect-error
                    ask(msg.payload),
                    T.chain((res) => msg.return(0, res))
                  )
                })
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
                    system.make(`singleton/leader/${id}`, SUP.none, state, stateful)
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
                      yield* _(pipe(cluster.ask(recipient)(a), T.to(p)))
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
