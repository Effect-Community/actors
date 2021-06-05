import * as T from "@effect-ts/core/Effect"
import type { Clock } from "@effect-ts/core/Effect/Clock"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import type { Random } from "@effect-ts/core/Effect/Random"
import { identity, pipe } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import type * as K from "@effect-ts/keeper"

import * as A from "../Actor"
import type { ActorRef } from "../ActorRef"
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
  Has<Cluster> & Has<Clock> & Has<Random> & R,
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
  Has<Cluster> & Has<Clock> & Has<Random> & R & R3,
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
  Has<Cluster> & Has<Clock> & Has<Random> & R & R3,
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

        return yield* _(
          cluster.runOnLeader(election)(
            M.gen(function* (_) {
              const ref: ActorRef<F1> = yield* _(
                context.make("leader", SUP.none, stateful, initial)
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
              ["|>"](side ? T.race(side(yield* _(context.self))) : identity),
            (leader) =>
              T.gen(function* (_) {
                const all = yield* _(Q.takeBetween_(queue, 1, 100))
                const { host, port } = yield* _(cluster.memberHostPort(leader))

                const recipient = `zio://${context.actorSystem.actorSystemName}@${host}:${port}/${name}`

                for (const [a, p] of all) {
                  const act = yield* _(context.select(recipient))
                  yield* _(pipe(act.ask(a), T.to(p)))
                }
              })["|>"](T.forever)
          )
        )
      })
    )
  )
}
