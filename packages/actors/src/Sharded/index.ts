import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HashMap from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import type { HasClock } from "@effect-ts/core/Effect/Clock"
import type { Exit } from "@effect-ts/core/Effect/Exit"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as Scope from "@effect-ts/core/Effect/Scope"
import { identity, pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import { KeeperClient } from "@effect-ts/keeper"

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
import * as AS from "../ActorSystem"
import { Cluster } from "../Cluster"
import type { Throwable } from "../common"
import type * as AM from "../Message"
import * as SUP from "../Supervisor"

export interface Sharded<N extends string, F1 extends AM.AnyMessage> {
  name: N
  messageToId: (_: F1) => string
  actor: ActorRef<F1>
}

export const makeSharded = <N extends string, R, S, F1 extends AM.AnyMessage>(
  name: N,
  stateful: A.AbstractStateful<R, S, F1>,
  init: S,
  messageToId: (_: F1) => string
) => {
  const tag_ = tag<Sharded<N, F1>>()
  return {
    Tag: tag_,
    actor: T.accessService(tag_)((_) => _.actor),
    Live: L.fromManaged(tag_)(
      M.gen(function* (_) {
        const system = yield* _(AS.ActorSystemTag)
        const runningMapRef = yield* _(
          REF.makeRef(HashMap.make<string, ActorRef<F1>>())
        )

        const cluster = yield* _(Cluster)
        const cli = yield* _(KeeperClient)

        const scope = yield* _(
          Scope.makeScope<Exit<unknown, unknown>>()["|>"](
            M.makeExit((_, ex) => _.close(ex))
          )
        )

        const sharded = yield* _(
          pipe(
            system.make(
              `sharded/proxy/${name}`,
              SUP.none,
              {},
              new ShardProxy(name, messageToId, stateful.messages, (queue) =>
                T.gen(function* (_) {
                  while (1) {
                    const all = yield* _(Q.takeBetween_(queue, 1, 100))

                    const slots: Record<string, Chunk.Chunk<A.PendingMessage<F1>>> = {}

                    for (const r of all) {
                      const id = messageToId(r[0])
                      if (!slots[id]) {
                        slots[id] = Chunk.empty()
                      }
                      slots[id] = Chunk.append_(slots[id], r)
                    }

                    yield* _(
                      T.forEachUnitPar_(Object.keys(slots), (id) =>
                        T.forEachUnit_(slots[id], ([a, p]) => {
                          return T.gen(function* (_) {
                            const runningMap = yield* _(REF.get(runningMapRef))
                            const isRunning = HashMap.get_(runningMap, id)

                            if (O.isSome(isRunning)) {
                              // we have the actor running
                              yield* _(isRunning.value.ask(a)["|>"](T.to(p)))
                            } else {
                              const membersDir = `${cluster.clusterDir}/sharded/${name}/${id}/members`

                              yield* _(cli.mkdir(membersDir))

                              const leader = yield* _(
                                pipe(cli.getChildren(membersDir), T.map(Chunk.head))
                              )

                              // there is a leader
                              if (O.isSome(leader)) {
                                // we are the leader but the actor was killed
                                if (leader.value === cluster.nodeId) {
                                  const running = yield* _(
                                    system.make(
                                      `sharded/leader/${id}`,
                                      SUP.none,
                                      init,
                                      stateful
                                    )
                                  )
                                  yield* _(
                                    REF.update_(runningMapRef, HashMap.set(id, running))
                                  )
                                  yield* _(running.ask(a)["|>"](T.to(p)))
                                } else {
                                  // we are not the leader, use cluster
                                  const { host, port } = yield* _(
                                    cluster.memberHostPort(leader.value)
                                  )
                                  const recipient = `zio://${system.actorSystemName}@${host}:${port}/sharded/proxy/${id}`
                                  yield* _(cluster.ask(recipient)(a)["|>"](T.to(p)))
                                }
                              } else {
                                // there is no leader, attempt to self elect
                                yield* _(
                                  cli.create(`${membersDir}/${cluster.nodeId}`, {
                                    mode: "EPHEMERAL"
                                  })
                                )
                                const leader = yield* _(
                                  pipe(cli.getChildren(membersDir), T.map(Chunk.head))
                                )
                                // this should never be the case
                                if (O.isNone(leader)) {
                                  yield* _(T.die("cannot elect a leader"))
                                } else {
                                  // we got the leadership
                                  if (leader.value === cluster.nodeId) {
                                    const running = yield* _(
                                      system.make(
                                        `sharded/leader/${id}`,
                                        SUP.none,
                                        init,
                                        stateful
                                      )
                                    )
                                    yield* _(
                                      REF.update_(
                                        runningMapRef,
                                        HashMap.set(id, running)
                                      )
                                    )
                                    yield* _(running.ask(a)["|>"](T.to(p)))
                                  } else {
                                    // someone else got the leadership first
                                    yield* _(
                                      cli.remove(`${membersDir}/${cluster.nodeId}`)
                                    )

                                    const { host, port } = yield* _(
                                      cluster.memberHostPort(leader.value)
                                    )
                                    const recipient = `zio://${system.actorSystemName}@${host}:${port}/sharded/proxy/${id}`
                                    yield* _(cluster.ask(recipient)(a)["|>"](T.to(p)))
                                  }
                                }
                              }
                            }
                          })
                        })
                      )
                    )
                  }

                  return yield* _(T.never)
                })
              )
            ),
            T.overrideForkScope(scope.scope)
          )
        )

        return yield* _(
          T.succeed(
            identity<Sharded<N, F1>>({
              actor: sharded,
              messageToId,
              name
            })
          )
        )
      })
    )
  }
}

export class ShardProxy<
  N extends string,
  ID extends string,
  R,
  F1 extends AM.AnyMessage,
  E
> extends A.AbstractStateful<R, {}, F1> {
  readonly _N!: () => N
  readonly _ID!: () => ID

  constructor(
    readonly name: N,
    readonly id: (_: F1) => ID,
    readonly messages: AM.MessageRegistry<F1>,
    readonly process: (_: Q.Queue<A.PendingMessage<F1>>) => T.Effect<R, E, never>
  ) {
    super()
  }

  defaultMailboxSize = 10000

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context<F1>,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (initial: {}) => T.RIO<R & HasClock, A.Actor<F1>> {
    return () => {
      return pipe(
        T.do,
        T.bind("queue", () => Q.makeBounded<A.PendingMessage<F1>>(mailboxSize)),
        T.tap((_) => pipe(this.process(_.queue), T.fork)),
        T.map((_) => new A.Actor(this.messages, _.queue, optOutActorSystem))
      )
    }
  }
}
