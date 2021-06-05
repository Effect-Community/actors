import "isomorphic-fetch"

import * as T from "@effect-ts/core/Effect"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as STM from "@effect-ts/core/Effect/Transactional/STM"
import * as TRef from "@effect-ts/core/Effect/Transactional/TRef"
import { pipe } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import type * as K from "@effect-ts/keeper"

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
import { ActorRefRemote } from "../ActorRef"
import * as AS from "../ActorSystem"
import { Cluster } from "../Cluster"
import type {
  ActorAlreadyExistsException,
  ErrorMakingActorException,
  InvalidActorName,
  InvalidActorPath,
  NoSuchActorException
} from "../common"
import type * as AM from "../Message"
import * as SUP from "../Supervisor"

export function makeSingleton<R, S, F1 extends AM.AnyMessage>(
  stateful: A.AbstractStateful<R, S, F1>
): A.ActorProxy<
  Has<Cluster> & T.DefaultEnv & R,
  S,
  F1,
  | K.ZooError
  | InvalidActorPath
  | ActorAlreadyExistsException
  | InvalidActorName
  | ErrorMakingActorException
  | NoSuchActorException
>
export function makeSingleton<R, S, F1 extends AM.AnyMessage, R3, E3>(
  stateful: A.AbstractStateful<R, S, F1>,
  side: (self: ActorRef<F1>) => T.Effect<R3, E3, never>
): A.ActorProxy<
  Has<Cluster> & T.DefaultEnv & R & R3,
  S,
  F1,
  | E3
  | K.ZooError
  | InvalidActorPath
  | ActorAlreadyExistsException
  | InvalidActorName
  | ErrorMakingActorException
  | NoSuchActorException
>
export function makeSingleton<R, S, F1 extends AM.AnyMessage, R3, E3>(
  stateful: A.AbstractStateful<R, S, F1>,
  side?: (self: ActorRef<F1>) => T.Effect<R3, E3, never>
): A.ActorProxy<
  Has<Cluster> & T.DefaultEnv & R & R3,
  S,
  F1,
  | E3
  | K.ZooError
  | InvalidActorPath
  | ActorAlreadyExistsException
  | InvalidActorName
  | ErrorMakingActorException
  | NoSuchActorException
> {
  return new A.ActorProxy(stateful.messages, (queue, context, initial: S) =>
    M.useNow(
      M.gen(function* (_) {
        const cluster = yield* _(Cluster)

        const name = yield* _(
          pipe(
            AS.resolvePath(context.address)["|>"](T.orDie),
            T.map(([_, __, ___, actorName]) => actorName.substr(1))
          )
        )

        const election = `singleton-${name}`

        yield* _(cluster.init(election))

        yield* _(
          pipe(
            cluster.join(election),
            M.make((p) => cluster.leave(p)["|>"](T.orDie))
          )
        )

        const gate = yield* _(TRef.makeCommit(O.emptyOf<ActorRef<F1>>()))

        return yield* _(
          pipe(
            T.gen(function* (_) {
              while (1) {
                const [a, p] = yield* _(Q.take(queue))
                const ref = (yield* _(
                  STM.commit(
                    pipe(
                      TRef.get(gate),
                      STM.tap((o) => STM.check(O.isSome(o)))
                    )
                  )
                )) as O.Some<ActorRef<F1>>
                yield* _(ref.value.ask(a)["|>"](T.to(p)))
              }

              return yield* _(T.never)
            }),
            T.race(
              cluster.runOnLeader(election)(
                M.gen(function* (_) {
                  const ref: ActorRef<F1> = yield* _(
                    context.make("leader", SUP.none, stateful, initial)
                  )

                  yield* _(
                    STM.commit(TRef.set_(gate, O.some(ref)))["|>"](
                      M.make(() => STM.commit(TRef.set_(gate, O.none)))
                    )
                  )

                  return yield* _(side ? side(ref) : T.never)
                })["|>"](M.useNow),
                (leader) =>
                  M.gen(function* (_) {
                    const { host, port } = yield* _(cluster.memberHostPort(leader))
                    const recipient = `zio://${context.actorSystem.actorSystemName}@${host}:${port}/${name}`
                    const ref = new ActorRefRemote<F1>(recipient, context.actorSystem)

                    yield* _(
                      STM.commit(TRef.set_(gate, O.some(ref)))["|>"](
                        M.make(() => STM.commit(TRef.set_(gate, O.none)))
                      )
                    )

                    return yield* _(T.never)
                  })["|>"](M.useNow)
              )
            )
          )
        )
      })
    )
  )
}
