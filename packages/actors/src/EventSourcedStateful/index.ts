import { pipe } from "@effect-ts/core"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import type { HasClock } from "@effect-ts/system/Clock"
import { tuple } from "@effect-ts/system/Function"

import * as A from "../Actor"
import type * as AS from "../ActorSystem"
import type { _Response, _ResponseOf, Throwable } from "../common"
import type * as SUP from "../Supervisor"

interface EventSourcedStatefulReply<S, EV, F1> {
  <A extends F1>(msg: A): (
    events: CH.Chunk<EV>,
    reply: (state: S) => _ResponseOf<A>
  ) => readonly [CH.Chunk<EV>, (s: S) => _ResponseOf<A>]
}

export class EventSourcedStateful<R, S, F1, EV> extends A.AbstractStateful<R, S, F1> {
  constructor(
    readonly receive: (
      state: S,
      msg: F1,
      context: AS.Context,
      replyTo: EventSourcedStatefulReply<S, EV, F1>
    ) => T.Effect<R, Throwable, readonly [CH.Chunk<EV>, (s: S) => _ResponseOf<F1>]>,
    readonly sourceEvent: (state: S, event: EV) => S
  ) {
    super()
  }

  defaultMailboxSize = 10000

  applyEvents(events: CH.Chunk<EV>, state: S) {
    return CH.reduce_(events, state, this.sourceEvent)
  }

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (initial: S) => T.RIO<R & HasClock, A.Actor<F1>> {
    const process = (
      msg: A.PendingMessage<F1>,
      state: REF.Ref<S>
    ): T.RIO<R & HasClock, void> => {
      return pipe(
        T.do,
        T.bind("s", () => REF.get(state)),
        T.let("fa", () => msg[0]),
        T.let("promise", () => msg[1]),
        T.let("receiver", (_) =>
          this.receive(_.s, _.fa, context, () => (s, r) => tuple(s, r))
        ),
        T.let("effectfulCompleter", (_) => (s: S, a: _ResponseOf<F1>) =>
          pipe(REF.set_(state, s), T.zipRight(P.succeed_(_.promise, a)), T.as(T.unit))
        ),
        T.let("idempotentCompleter", (_) => (a: _ResponseOf<F1>) =>
          pipe(P.succeed_(_.promise, a), T.zipRight(T.unit))
        ),
        T.let(
          "fullCompleter",
          (_) => ([ev, sa]: readonly [CH.Chunk<EV>, (s: S) => _ResponseOf<F1>]) =>
            CH.size(ev) === 0
              ? _.idempotentCompleter(sa(_.s))
              : pipe(
                  T.do,
                  T.let("updatedState", () => this.applyEvents(ev, _.s)),
                  T.tap((s) =>
                    _.effectfulCompleter(s.updatedState, sa(s.updatedState))
                  ),
                  T.zipRight(T.unit)
                )
        ),
        T.chain((_) =>
          T.foldM_(
            _.receiver,
            (e) =>
              pipe(
                supervisor.supervise(_.receiver, e),
                T.foldM((__) => P.fail_(_.promise, e), _.fullCompleter)
              ),
            _.fullCompleter
          )
        )
      )
    }

    return (initial) =>
      pipe(
        T.do,
        T.bind("state", () => REF.makeRef(initial)),
        T.bind("queue", () => Q.makeBounded<A.PendingMessage<F1>>(mailboxSize)),
        T.tap((_) =>
          pipe(
            Q.take(_.queue),
            T.chain((t) => process(t, _.state)),
            T.forever,
            T.fork
          )
        ),
        T.map((_) => new A.Actor(_.queue, optOutActorSystem))
      )
  }
}
