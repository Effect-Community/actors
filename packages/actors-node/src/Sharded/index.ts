import type { AbstractStateful } from "@effect-ts/actors/Actor"
import * as Act from "@effect-ts/actors/Actor"
import type { ActorRef } from "@effect-ts/actors/ActorRef"
import { actorRef } from "@effect-ts/actors/ActorRef"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as SortedSet from "@effect-ts/core/Collections/Immutable/SortedSet"
import * as T from "@effect-ts/core/Effect"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import * as Ord from "@effect-ts/core/Ord"
import * as S from "@effect-ts/schema"
import * as Guard from "@effect-ts/schema/Guard"
import * as Th from "@effect-ts/schema/These"

import { Cluster } from "../Cluster"
import { makeSingleton } from "../Singleton"
import { transactional } from "../Transactional"

export class Join extends AM.Message(
  "Join",
  S.props({
    nodeId: S.prop(S.string),
    actor: S.prop(actorRef<AM.TypeOf<typeof Worker>>())
  }),
  S.props({
    workerId: S.prop(S.number)
  })
) {}

export const Coordinator = AM.messages(Join)

export class Assign extends AM.Message(
  "Assign",
  S.props({
    shard: S.prop(S.string)
  }),
  S.props({})
) {}

export const Worker = AM.messages(Assign)

function sortedSet<X extends S.SchemaUPI>(
  s: X,
  o: Ord.Ord<S.ParsedShapeOf<X>>
): S.Standard<SortedSet.SortedSet<S.ParsedShapeOf<X>>> {
  return S.chunk(s)[">>>"](
    pipe(
      S.identity(
        (_): _ is SortedSet.SortedSet<S.ParsedShapeOf<X>> =>
          _ instanceof SortedSet.SortedSet && SortedSet.every_(_, Guard.for(s))
      ),
      S.parser((_: Chunk.Chunk<S.ParsedShapeOf<X>>) => {
        let res = SortedSet.make(o)
        for (const k of _) {
          res = SortedSet.add_(res, k)
        }
        return Th.succeed(res)
      }),
      S.encoder((_) => Chunk.from(_))
    )
  )
}

export function sharded<R, S, F1 extends AM.AnyMessage>(
  stateful: AbstractStateful<R, S, F1>
) {
  return new Act.ActorProxy(stateful.messages, (queue, context) =>
    pipe(
      M.gen(function* (_) {
        const cluster = yield* _(Cluster)

        const worker = yield* _(
          pipe(
            context.make(
              cluster.nodeId,
              SUP.none,
              Act.stateful(
                Worker,
                S.unknown
              )(() => ({
                Assign: () => T.succeed({})
              })),
              {}
            ),
            M.make((_) => _.stop["|>"](T.orDie))
          )
        )

        console.log(worker.address)

        const coordinator = yield* _(
          pipe(
            context.make(
              "coordinator",
              SUP.none,
              makeSingleton(
                transactional(
                  Coordinator,
                  S.props({
                    nodes: S.prop(
                      sortedSet(
                        S.props({
                          workerId: S.prop(S.number),
                          nodeId: S.prop(S.string),
                          actor: S.prop(actorRef<AM.TypeOf<typeof Worker>>())
                        }),
                        Ord.contramap_(Ord.number, (_) => _.workerId)
                      )
                    ),
                    lastId: S.prop(S.number)
                  }),
                  O.none
                )(({ state }) => ({
                  Join: ({ actor, nodeId }) =>
                    T.gen(function* (_) {
                      const { lastId, nodes } = yield* _(state.get)
                      const workerId = lastId + 1
                      console.log(
                        JSON.stringify(Array.from(nodes).map((_) => _.actor.address))
                      )
                      yield* _(
                        state.set({
                          lastId: workerId,
                          nodes: SortedSet.add_(nodes, { nodeId, workerId, actor })
                        })
                      )
                      return { workerId }
                    })
                }))
              ),
              {
                lastId: 0,
                nodes: SortedSet.make(
                  Ord.contramap_(
                    Ord.number,
                    (_: {
                      workerId: number
                      nodeId: string
                      actor: ActorRef<AM.TypeOf<typeof Worker>>
                    }) => _.workerId
                  )
                )
              }
            ),
            M.makeInterruptible((_) => _.stop["|>"](T.orDie))
          )
        )

        console.log(coordinator.address)

        const { workerId } = yield* _(
          coordinator.ask(new Join({ actor: worker, nodeId: cluster.nodeId }))
        )

        console.log(workerId)

        return yield* _(T.never)
      }),
      M.useNow
    )
  )
}
