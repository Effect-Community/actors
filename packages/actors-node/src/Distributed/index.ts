import * as A from "@effect-ts/actors/Actor"
import type { ActorRef } from "@effect-ts/actors/ActorRef"
import * as AS from "@effect-ts/actors/ActorSystem"
import type { Throwable } from "@effect-ts/actors/Common"
import type * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HashMap from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import * as Clock from "@effect-ts/core/Effect/Clock"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as Sh from "@effect-ts/core/Effect/Schedule"
import * as STM from "@effect-ts/core/Effect/Transactional/STM"
import * as TRef from "@effect-ts/core/Effect/Transactional/TRef"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import { KeeperClient } from "@effect-ts/keeper"

import { Cluster } from "../Cluster"
import { Shards } from "../Shards"

export interface Distributed<N extends string, F1 extends AM.AnyMessage> {
  name: N
  messageToId: (_: { name: N; message: F1 }) => string
  actor: ActorRef<F1>
  runningMapRef: REF.Ref<HashMap.HashMap<string, ActorRef<F1>>>
}

export function runner<R, E, R2, E2, F1 extends AM.AnyMessage>(
  factory: (id: string) => T.Effect<R, E, ActorRef<F1>>,
  postPassivation: (
    id: string,
    ref: REF.Ref<
      HashMap.HashMap<
        string,
        TRef.TRef<{
          listeners: number
          closing: boolean
        }>
      >
    >
  ) => T.Effect<R2, E2, void>,
  opts?: { passivateAfter?: number }
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
      REF.makeRef(
        HashMap.make<string, TRef.TRef<{ listeners: number; closing: boolean }>>()
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

    function passivate(now: number, _: Chunk.Chunk<string>, passivateAfter: number) {
      return T.forEachUnitPar_(_, (path) =>
        pipe(
          T.gen(function* (_) {
            const map = yield* _(REF.get(gatesRef))
            const gate = HashMap.get_(map, path)

            if (O.isSome(gate)) {
              yield* _(
                STM.commit(
                  pipe(
                    TRef.get(gate.value),
                    STM.tap((_) => STM.check(_.listeners === 0 && _.closing === false)),
                    STM.chain((_) =>
                      TRef.set_(gate.value, {
                        closing: true,
                        listeners: _.listeners
                      })
                    )
                  )
                )
              )
              return gate.value
            } else {
              const gate = yield* _(TRef.makeCommit({ listeners: 0, closing: true }))
              yield* _(REF.update_(gatesRef, HashMap.set(path, gate)))
              return gate
            }
          }),
          T.bracket(
            () =>
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
              }),
            (g) =>
              pipe(
                STM.commit(
                  TRef.update_(g, (_) => ({
                    closing: false,
                    listeners: _.listeners
                  }))
                ),
                T.zipRight(postPassivation(path, gatesRef))
              )
          )
        )
      )
    }

    if (opts?.passivateAfter != null) {
      const passivateAfter = opts?.passivateAfter
      yield* _(
        T.forkManaged(
          T.repeat(Sh.windowed(opts.passivateAfter))(
            T.gen(function* (_) {
              const now = yield* _(Clock.currentTime)
              const stats = yield* _(REF.get(statsRef))
              const toPassivate = Chunk.builder<string>()

              for (const [k, v] of stats) {
                if (v.inFlight === 0 && now - v.last >= passivateAfter) {
                  toPassivate.append(k)
                }
              }

              yield* _(passivate(now, toPassivate.build(), passivateAfter))
            })
          )
        )
      )
    }

    return {
      runningMapRef,
      statsRef,
      gatesRef,
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
                      STM.tap((_) => STM.check(_.closing === false)),
                      STM.chain((_) =>
                        TRef.set_(gate.value, {
                          closing: _.closing,
                          listeners: _.listeners + 1
                        })
                      )
                    )
                  )
                )
                return gate.value
              } else {
                const gate = yield* _(TRef.makeCommit({ listeners: 1, closing: false }))
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
              (g) =>
                STM.commit(
                  TRef.update_(g, (_) => ({
                    closing: _.closing,
                    listeners: _.listeners - 1
                  }))
                )
            )
          )
    }
  })
}

function electionFromNameAndId(name: string, id: string) {
  return `distributed-${name}-${id}`
}

export const distributed = <R, S, F1 extends AM.AnyMessage>(
  stateful: A.AbstractStateful<R, S, F1>,
  messageToId: (_: F1) => string,
  opts?: {
    passivateAfter?: number
    shards?: number
  }
) =>
  new A.ActorProxy(stateful.messages, (queue, context, initial: (id: string) => S) =>
    T.provide(opts?.shards ? Shards.of({ shards: opts.shards }) : {})(
      M.useNow(
        M.gen(function* (_) {
          const cluster = yield* _(Cluster)
          const cli = yield* _(KeeperClient)

          const name = yield* _(
            pipe(
              AS.resolvePath(context.address)["|>"](T.orDie),
              T.map(([_, __, ___, actorName]) => actorName.substr(1))
            )
          )

          const leadersRef = yield* _(
            REF.makeRef(
              HashMap.make<
                string,
                (
                  f: (
                    ask: ActorRef<F1>["ask"]
                  ) => T.Effect<R & T.DefaultEnv, Throwable, void>
                ) => T.Effect<R & T.DefaultEnv, Throwable, void>
              >()
            )
          )

          const leadersNodeRef = yield* _(
            REF.makeRef<HashMap.HashMap<string, string>>(HashMap.make())
          )

          const gate = yield* _(TRef.makeCommit(true))

          const factory = yield* _(
            runner(
              (id) => context.make<R, S, F1>(id, SUP.none, stateful, initial(id)),
              (id, ref) =>
                T.gen(function* (_) {
                  yield* _(
                    STM.commit(
                      pipe(
                        TRef.get(gate),
                        STM.chain(STM.check),
                        STM.chain(() => TRef.set_(gate, false))
                      )
                    )
                  )
                  const election = electionFromNameAndId(name, id)
                  const leaderMap = yield* _(REF.get(leadersNodeRef))
                  const leaderPath = HashMap.get_(leaderMap, election)
                  if (O.isSome(leaderPath)) {
                    yield* _(cluster.leave(leaderPath.value))
                  }
                  yield* _(REF.update_(leadersRef, HashMap.remove(election)))
                  yield* _(REF.update_(leadersNodeRef, HashMap.remove(election)))
                  yield* _(REF.update_(ref, HashMap.remove(id)))
                  yield* _(STM.commit(TRef.set_(gate, true)))
                }),
              opts
            )
          )

          while (1) {
            const all = yield* _(Q.takeBetween_(queue, 1, 100))

            yield* _(
              STM.commit(
                pipe(
                  TRef.get(gate),
                  STM.chain(STM.check),
                  STM.chain(() => TRef.set_(gate, false))
                )
              )
            )

            const slots: Record<string, Chunk.Chunk<A.PendingMessage<F1>>> = {}

            for (const r of all) {
              const id = messageToId(r[0])
              if (!slots[id]) {
                slots[id] = Chunk.empty()
              }
              slots[id] = Chunk.append_(slots[id], r)
            }

            yield* _(
              M.forEachUnitPar_(Object.keys(slots), (id) =>
                M.forEachUnit_(slots[id], ([a, p]) => {
                  return M.gen(function* (_) {
                    const leaders = yield* _(REF.get(leadersRef))
                    const election = electionFromNameAndId(name, id)
                    const cached = HashMap.get_(leaders, election)

                    if (O.isSome(cached)) {
                      yield* _(cached.value((ask) => ask(a)["|>"](T.to(p))))
                    } else {
                      yield* _(cluster.init(election))

                      const leader = yield* _(cluster.leaderId(election))

                      // there is a leader
                      if (O.isSome(leader)) {
                        if (leader.value === cluster.nodeId) {
                          // we are the leader
                          yield* _(
                            REF.update_(
                              leadersRef,
                              HashMap.set(election, (f) =>
                                factory.use(id)((ask) => f((a) => ask(a)))
                              )
                            )
                          )
                          yield* _(factory.use(id)((ask) => ask(a)["|>"](T.to(p))))
                        } else {
                          // we are not the leader, use cluster
                          const { host, port } = yield* _(
                            cluster.memberHostPort(leader.value)
                          )
                          const recipient = `zio://${context.actorSystem.actorSystemName}@${host}:${port}/${name}/${id}`
                          const act = yield* _(context.select(recipient))
                          yield* _(
                            REF.update_(
                              leadersRef,
                              HashMap.set(election, (f) => f((a) => act.ask(a)))
                            )
                          )
                          yield* _(
                            pipe(
                              cluster.watchLeader(election),
                              T.chain(() =>
                                REF.update_(leadersRef, HashMap.remove(election))
                              ),
                              T.fork
                            )
                          )
                          yield* _(act.ask(a)["|>"](T.to(p)))
                        }
                      } else {
                        // there is no leader, attempt to self elect
                        const selfNode = yield* _(cluster.join(election))

                        const leader = yield* _(cluster.leaderId(election))

                        // this should never be the case
                        if (O.isNone(leader)) {
                          yield* _(T.die("cannot elect a leader"))
                        } else {
                          // we got the leadership
                          if (leader.value === cluster.nodeId) {
                            yield* _(
                              REF.update_(
                                leadersRef,
                                HashMap.set(election, (f) =>
                                  factory.use(id)((ask) => f((a) => ask(a)))
                                )
                              )
                            )
                            yield* _(
                              REF.update_(
                                leadersNodeRef,
                                HashMap.set(election, selfNode)
                              )
                            )
                            yield* _(factory.use(id)((ask) => ask(a)["|>"](T.to(p))))
                          } else {
                            // someone else got the leadership first
                            yield* _(cli.remove(selfNode))

                            const { host, port } = yield* _(
                              cluster.memberHostPort(leader.value)
                            )
                            const recipient = `zio://${context.actorSystem.actorSystemName}@${host}:${port}/${name}/${id}`
                            const act = yield* _(context.select(recipient))
                            yield* _(
                              REF.update_(
                                leadersRef,
                                HashMap.set(election, (f) => f((a) => act.ask(a)))
                              )
                            )
                            yield* _(
                              pipe(
                                cluster.watchLeader(election),
                                T.chain(() =>
                                  REF.update_(leadersRef, HashMap.remove(election))
                                ),
                                T.fork
                              )
                            )
                            yield* _(act.ask(a)["|>"](T.to(p)))
                          }
                        }
                      }
                    }
                  })
                })
              )
            )

            yield* _(STM.commit(TRef.set_(gate, true)))
          }

          return yield* _(T.never)
        })
      )
    )
  )
