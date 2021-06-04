import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HashMap from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import type { Exit } from "@effect-ts/core/Effect/Exit"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as Scope from "@effect-ts/core/Effect/Scope"
import * as STM from "@effect-ts/core/Effect/Transactional/STM"
import * as TRef from "@effect-ts/core/Effect/Transactional/TRef"
import { identity, pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import { KeeperClient } from "@effect-ts/keeper"

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
import * as AS from "../ActorSystem"
import { Cluster, EO } from "../Cluster"
import type * as AM from "../Message"
import * as SUP from "../Supervisor"

export interface Distributed<N extends string, F1 extends AM.AnyMessage> {
  name: N
  messageToId: (_: { name: N; message: F1 }) => string
  actor: ActorRef<F1>
}

export function runner<R, E, F1 extends AM.AnyMessage>(
  factory: (id: string) => T.Effect<R, E, ActorRef<F1>>
) {
  return M.gen(function* (_) {
    const runningMapRef = yield* _(
      pipe(
        REF.makeRef(HashMap.make<string, ActorRef<F1>>()),
        M.make((ref) =>
          pipe(
            REF.get(ref),
            T.chain((hm) => T.forEachUnitPar_(hm, ([_, r]) => pipe(r.stop, T.orDie)))
          )
        )
      )
    )

    const gatesRef = yield* _(
      pipe(
        REF.makeRef(HashMap.make<string, TRef.TRef<boolean>>()),
        M.make((ref) =>
          pipe(
            REF.get(ref),
            T.chain((hm) => T.forEach_(hm, ([_, r]) => STM.commit(TRef.set_(r, false))))
          )
        )
      )
    )

    const statsRef = yield* _(REF.makeRef(HashMap.make<string, { inFlight: number }>()))

    function proxy(path: string, ref: ActorRef<F1>): ActorRef<F1>["ask"] {
      return (m) =>
        pipe(
          T.gen(function* (_) {
            yield* _(
              REF.update_(statsRef, (hm) => {
                const stat = HashMap.get_(hm, path)
                if (O.isSome(stat)) {
                  return HashMap.set_(hm, path, { inFlight: stat.value.inFlight + 1 })
                }
                return HashMap.set_(hm, path, { inFlight: 1 })
              })
            )
          }),
          T.bracket(
            () => ref.ask(m),
            () =>
              T.gen(function* (_) {
                yield* _(
                  REF.update_(statsRef, (hm) => {
                    const stat = HashMap.get_(hm, path)
                    if (O.isSome(stat)) {
                      return HashMap.set_(hm, path, {
                        inFlight: stat.value.inFlight - 1
                      })
                    }
                    return HashMap.set_(hm, path, { inFlight: 0 })
                  })
                )
              })
          )
        )
    }

    return {
      use:
        (path: string) =>
        <R2, E2, A2>(body: (ref: ActorRef<F1>["ask"]) => T.Effect<R2, E2, A2>) =>
          pipe(
            T.gen(function* (_) {
              const map = yield* _(REF.get(gatesRef))
              const gate = HashMap.get_(map, path)

              if (O.isSome(gate)) {
                yield* _(
                  STM.commit(
                    pipe(
                      TRef.get(gate.value),
                      STM.chain(STM.check),
                      STM.chain(() => TRef.set_(gate.value, false))
                    )
                  )
                )
                return gate.value
              } else {
                const gate = yield* _(TRef.makeCommit(false))
                yield* _(REF.update_(gatesRef, HashMap.set(path, gate)))
                return gate
              }
            }),
            T.bracket(
              () =>
                T.gen(function* (_) {
                  const isRunning = HashMap.get_(yield* _(REF.get(runningMapRef)), path)
                  if (O.isSome(isRunning)) {
                    return yield* _(body(proxy(path, isRunning.value)))
                  }
                  const ref = yield* _(factory(path))
                  yield* _(REF.update_(runningMapRef, HashMap.set(path, ref)))
                  return yield* _(body(proxy(path, ref)))
                }),
              (g) => STM.commit(TRef.set_(g, true))
            )
          )
    }
  })
}

export const makeDistributed = <N extends string, R, S, F1 extends AM.AnyMessage>(
  name: N,
  stateful: A.AbstractStateful<R, S, F1>,
  init: S,
  messageToId: (_: { name: N; message: F1 }) => string
) => {
  const tag_ = tag<Distributed<N, F1>>()
  return {
    Tag: tag_,
    actor: T.accessService(tag_)((_) => _.actor),
    Live: L.fromManaged(tag_)(
      M.gen(function* (_) {
        const system = yield* _(AS.ActorSystemTag)

        const factory = yield* _(
          runner((id) => system.make(id, SUP.none, init, stateful))
        )

        const cluster = yield* _(Cluster)
        const cli = yield* _(KeeperClient)

        const scope = yield* _(
          Scope.makeScope<Exit<unknown, unknown>>()["|>"](
            M.makeExit((_, ex) => _.close(ex))
          )
        )

        const distributed = yield* _(
          pipe(
            system.make(
              `distributed/proxy/${name}`,
              SUP.none,
              {},
              new A.ActorProxy(stateful.messages, (queue) =>
                T.gen(function* (_) {
                  while (1) {
                    const all = yield* _(Q.takeBetween_(queue, 1, 100))

                    const slots: Record<string, Chunk.Chunk<A.PendingMessage<F1>>> = {}

                    for (const r of all) {
                      const id = messageToId({ name, message: r[0] })
                      if (!slots[id]) {
                        slots[id] = Chunk.empty()
                      }
                      slots[id] = Chunk.append_(slots[id], r)
                    }

                    yield* _(
                      T.forEachUnitPar_(Object.keys(slots), (id) =>
                        T.forEachUnit_(slots[id], ([a, p]) => {
                          return T.gen(function* (_) {
                            const membersDir = `${cluster.clusterDir}/distributed/${name}/${id}/members`

                            yield* _(cli.mkdir(membersDir))

                            const leader = yield* _(
                              pipe(
                                cli.getChildren(membersDir),
                                T.map(Chunk.head),
                                EO.chain((s) => cli.getData(`${membersDir}/${s}`)),
                                EO.map((s) => s.toString("utf8"))
                              )
                            )

                            // there is a leader
                            if (O.isSome(leader)) {
                              if (leader.value === cluster.nodeId) {
                                // we are the leader
                                yield* _(
                                  factory.use(`distributed/leader/${id}`)((ask) =>
                                    ask(a)["|>"](T.to(p))
                                  )
                                )
                              } else {
                                // we are not the leader, use cluster
                                const { host, port } = yield* _(
                                  cluster.memberHostPort(leader.value)
                                )
                                const recipient = `zio://${system.actorSystemName}@${host}:${port}/distributed/proxy/${id}`
                                yield* _(cluster.ask(recipient)(a)["|>"](T.to(p)))
                              }
                            } else {
                              // there is no leader, attempt to self elect
                              const selfNode = yield* _(
                                cli.create(`${membersDir}/worker_`, {
                                  mode: "EPHEMERAL_SEQUENTIAL",
                                  data: Buffer.from(cluster.nodeId)
                                })
                              )

                              const leader = yield* _(
                                pipe(
                                  cli.getChildren(membersDir),
                                  T.map(Chunk.head),
                                  EO.chain((s) => cli.getData(`${membersDir}/${s}`)),
                                  EO.map((s) => s.toString("utf8"))
                                )
                              )

                              // this should never be the case
                              if (O.isNone(leader)) {
                                yield* _(T.die("cannot elect a leader"))
                              } else {
                                // we got the leadership
                                if (leader.value === cluster.nodeId) {
                                  yield* _(
                                    factory.use(`distributed/leader/${id}`)((ask) =>
                                      ask(a)["|>"](T.to(p))
                                    )
                                  )
                                } else {
                                  // someone else got the leadership first
                                  yield* _(cli.remove(selfNode))

                                  const { host, port } = yield* _(
                                    cluster.memberHostPort(leader.value)
                                  )
                                  const recipient = `zio://${system.actorSystemName}@${host}:${port}/distributed/proxy/${id}`
                                  yield* _(cluster.ask(recipient)(a)["|>"](T.to(p)))
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
            identity<Distributed<N, F1>>({
              actor: distributed,
              messageToId,
              name
            })
          )
        )
      })
    )
  }
}
