import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HashMap from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import * as Clock from "@effect-ts/core/Effect/Clock"
import type { Exit } from "@effect-ts/core/Effect/Exit"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as Sh from "@effect-ts/core/Effect/Schedule"
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
  runningMapRef: REF.Ref<HashMap.HashMap<string, ActorRef<F1>>>
}

export function runner<R, E, F1 extends AM.AnyMessage>(
  passivateAfter: number,
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

    const statsRef = yield* _(
      REF.makeRef(HashMap.make<string, { inFlight: number; last: number }>())
    )

    function proxy(path: string, ref: ActorRef<F1>): ActorRef<F1>["ask"] {
      return (m) =>
        pipe(
          T.gen(function* (_) {
            const last = yield* _(Clock.currentTime)
            yield* _(
              REF.update_(statsRef, (hm) => {
                const stat = HashMap.get_(hm, path)
                if (O.isSome(stat)) {
                  return HashMap.set_(hm, path, {
                    inFlight: stat.value.inFlight + 1,
                    last
                  })
                }
                return HashMap.set_(hm, path, { inFlight: 1, last })
              })
            )
          }),
          T.bracket(
            () => ref.ask(m),
            () =>
              T.gen(function* (_) {
                const last = yield* _(Clock.currentTime)
                yield* _(
                  REF.update_(statsRef, (hm) => {
                    const stat = HashMap.get_(hm, path)
                    if (O.isSome(stat)) {
                      return HashMap.set_(hm, path, {
                        inFlight: stat.value.inFlight - 1,
                        last
                      })
                    }
                    return HashMap.set_(hm, path, { inFlight: 0, last })
                  })
                )
              })
          )
        )
    }

    function passivate(now: number, _: Chunk.Chunk<string>) {
      return T.forEachUnitPar_(_, (path) =>
        locking(path)(
          T.gen(function* (_) {
            const runningMap = yield* _(REF.get(runningMapRef))
            const running = HashMap.get_(runningMap, path)
            if (O.isSome(running)) {
              const statsMap = yield* _(REF.get(statsRef))
              const stats = HashMap.get_(statsMap, path)

              if (O.isSome(stats)) {
                if (
                  stats.value.inFlight === 0 &&
                  now - stats.value.last >= passivateAfter
                ) {
                  const rem = yield* _(running.value.stop)
                  if (rem.length > 0) {
                    yield* _(T.die("Bug, we lost messages"))
                  }
                  yield* _(REF.update_(statsRef, HashMap.remove(path)))
                  yield* _(REF.update_(runningMapRef, HashMap.remove(path)))
                }
              }
            }
          })
        )
      )
    }

    yield* _(
      T.forkManaged(
        T.repeat(Sh.windowed(1_000))(
          T.gen(function* (_) {
            const now = yield* _(Clock.currentTime)
            const stats = yield* _(REF.get(statsRef))
            const toPassivate = Chunk.builder<string>()

            for (const [k, v] of stats) {
              if (v.inFlight === 0 && now - v.last >= passivateAfter) {
                toPassivate.append(k)
              }
            }

            yield* _(passivate(now, toPassivate.build()))
          })
        )
      )
    )

    const locking =
      (path: string) =>
      <R2, E2, A2>(effect: T.Effect<R2, E2, A2>) =>
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
            () => effect,
            (g) => STM.commit(TRef.set_(g, true))
          )
        )

    return {
      runningMapRef,
      statsRef,
      locking,
      gatesRef,
      use:
        (path: string) =>
        <R2, E2, A2>(body: (ref: ActorRef<F1>["ask"]) => T.Effect<R2, E2, A2>) =>
          locking(path)(
            T.gen(function* (_) {
              const isRunning = HashMap.get_(yield* _(REF.get(runningMapRef)), path)
              if (O.isSome(isRunning)) {
                return yield* _(body(proxy(path, isRunning.value)))
              }
              const ref = yield* _(factory(path))
              yield* _(REF.update_(runningMapRef, HashMap.set(path, ref)))
              return yield* _(body(proxy(path, ref)))
            })
          )
    }
  })
}

export const makeDistributed = <N extends string, R, S, F1 extends AM.AnyMessage>(
  name: N,
  stateful: A.AbstractStateful<R, S, F1>,
  init: S,
  messageToId: (_: { name: N; message: F1 }) => string,
  opts?: {
    passivateAfter?: number
  }
) => {
  const tag_ = tag<Distributed<N, F1>>()
  return {
    Tag: tag_,
    actor: T.accessService(tag_)((_) => _.actor),
    Live: L.fromManaged(tag_)(
      M.gen(function* (_) {
        const system = yield* _(AS.ActorSystemTag)

        const factory = yield* _(
          runner(opts?.passivateAfter ?? 60_000, (id) =>
            system.make(id, SUP.none, init, stateful)
          )
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
              name,
              runningMapRef: factory.runningMapRef
            })
          )
        )
      })
    )
  }
}
