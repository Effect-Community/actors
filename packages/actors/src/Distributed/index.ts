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

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
import * as AS from "../ActorSystem"
import { Cluster } from "../Cluster"
import type * as AM from "../Message"
import * as SUP from "../Supervisor"

export interface Distributed<N extends string, F1 extends AM.AnyMessage> {
  name: N
  messageToId: (_: { name: N; message: F1 }) => string
  actor: ActorRef<F1>
  runningMapRef: REF.Ref<HashMap.HashMap<string, ActorRef<F1>>>
}

export function runner<R, E, F1 extends AM.AnyMessage>(
  factory: (id: string) => T.Effect<R, E, ActorRef<F1>>,
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
              STM.commit(
                TRef.update_(g, (_) => ({
                  closing: false,
                  listeners: _.listeners
                }))
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

export const distributed = <R, S, F1 extends AM.AnyMessage>(
  stateful: A.AbstractStateful<R, S, F1>,
  messageToId: (_: F1) => string,
  opts?: {
    passivateAfter?: number
  }
) =>
  new A.ActorProxy(stateful.messages, (queue, context, initial: S) =>
    M.useNow(
      M.gen(function* (_) {
        const factory = yield* _(
          runner((id) => context.make<R, S, F1>(id, SUP.none, stateful, initial), opts)
        )
        const name = yield* _(
          pipe(
            AS.resolvePath(context.address)["|>"](T.orDie),
            T.map(([_, __, ___, actorName]) => actorName.substr(1))
          )
        )
        const cluster = yield* _(Cluster)
        const cli = yield* _(KeeperClient)

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
            M.forEachUnitPar_(Object.keys(slots), (id) =>
              M.forEachUnit_(slots[id], ([a, p]) => {
                return M.gen(function* (_) {
                  const election = `distributed-${name}-${id}`

                  yield* _(cluster.init(election))

                  const leader = yield* _(cluster.leaderId(election))

                  // there is a leader
                  if (O.isSome(leader)) {
                    if (leader.value === cluster.nodeId) {
                      // we are the leader
                      yield* _(factory.use(id)((ask) => ask(a)["|>"](T.to(p))))
                    } else {
                      // we are not the leader, use cluster
                      const { host, port } = yield* _(
                        cluster.memberHostPort(leader.value)
                      )
                      const recipient = `zio://${context.actorSystem.actorSystemName}@${host}:${port}/${name}/${id}`
                      const act = yield* _(context.select(recipient))
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
                        yield* _(factory.use(id)((ask) => ask(a)["|>"](T.to(p))))
                      } else {
                        // someone else got the leadership first
                        yield* _(cli.remove(selfNode))

                        const { host, port } = yield* _(
                          cluster.memberHostPort(leader.value)
                        )
                        const recipient = `zio://${context.actorSystem.actorSystemName}@${host}:${port}/${name}/${id}`
                        const act = yield* _(context.select(recipient))
                        yield* _(act.ask(a)["|>"](T.to(p)))
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
  )
