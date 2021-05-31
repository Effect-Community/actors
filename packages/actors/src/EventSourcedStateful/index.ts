import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as S from "@effect-ts/core/Effect/Experimental/Stream"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import { pipe } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import type { HasClock } from "@effect-ts/system/Clock"
import { tuple } from "@effect-ts/system/Function"
import type { IsEqualTo } from "@effect-ts/system/Utils"

import * as A from "../Actor"
import type * as AS from "../ActorSystem"
import type { Throwable } from "../common"
import type { Journal, PersistenceId } from "../Journal"
import { JournalFactory } from "../Journal"
import type * as AM from "../Message"
import type * as SUP from "../Supervisor"

type EventSourcedEnvelope<S, F1 extends AM.AnyMessage, EV> = {
  [Tag in AM.TagsOf<F1>]: {
    _tag: Tag
    payload: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
    return: (
      ev: CH.Chunk<EV>,
      r: IsEqualTo<AM.ResponseOf<AM.ExtractTagged<F1, Tag>>, void> extends true
        ? void
        : (state: S) => AM.ResponseOf<AM.ExtractTagged<F1, Tag>>
    ) => T.Effect<
      unknown,
      never,
      EventSourcedResponse<S, AM.ExtractTagged<F1, Tag>, EV>
    >
  }
}[AM.TagsOf<F1>]

type EventSourcedResponse<S, F1 extends AM.AnyMessage, EV> = {
  [Tag in AM.TagsOf<F1>]: readonly [
    CH.Chunk<EV>,
    (state: S) => AM.ResponseOf<AM.ExtractTagged<F1, Tag>>
  ]
}[AM.TagsOf<F1>]

export class EventSourcedStateful<
  R,
  S,
  F1 extends AM.AnyMessage,
  EV
> extends A.AbstractStateful<R & Has<JournalFactory>, S, F1> {
  constructor(
    readonly persistenceId: PersistenceId,
    readonly receive: (
      state: S,
      context: AS.Context
    ) => (
      msg: EventSourcedEnvelope<S, F1, EV>
    ) => T.Effect<R, Throwable, EventSourcedResponse<S, F1, EV>>,
    readonly sourceEvent: (state: S) => (event: EV) => S
  ) {
    super()
  }

  defaultMailboxSize = 10000

  applyEvents(events: CH.Chunk<EV>, state: S) {
    return CH.reduce_(events, state, (s, e) => this.sourceEvent(s)(e))
  }

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (
    initial: S
  ) => T.Effect<R & Has<JournalFactory> & HasClock, Throwable, A.Actor<F1>> {
    const process = (
      msg: A.PendingMessage<F1>,
      state: REF.Ref<S>,
      journal: Journal<S, EV>
    ): T.Effect<R & HasClock, Throwable, void> => {
      return pipe(
        T.do,
        T.bind("s", () => REF.get(state)),
        T.let("fa", () => msg[0]),
        T.let("promise", () => msg[1]),
        T.let("receiver", (_) =>
          this.receive(
            _.s,
            context
          )({
            _tag: _.fa._tag,
            payload: _.fa,
            return: (
              ev: CH.Chunk<EV>,
              r: (s: S) => AM.ResponseOf<F1> = (_) => undefined as any
            ) => T.succeed(tuple(ev, r))
          } as any)
        ),
        T.let(
          "effectfulCompleter",
          (_) => (s: S, a: AM.ResponseOf<F1>) =>
            pipe(
              REF.set_(state, s),
              T.chain(() => P.succeed_(_.promise, a)),
              T.zipRight(T.unit)
            )
        ),
        T.let(
          "idempotentCompleter",
          (_) => (a: AM.ResponseOf<F1>) =>
            pipe(P.succeed_(_.promise, a), T.zipRight(T.unit))
        ),
        T.let(
          "fullCompleter",
          (_) =>
            ([ev, sa]: readonly [CH.Chunk<EV>, (s: S) => AM.ResponseOf<F1>]) =>
              CH.size(ev) === 0
                ? _.idempotentCompleter(sa(_.s))
                : pipe(
                    T.do,
                    T.let("updatedState", () => this.applyEvents(ev, _.s)),
                    T.tap((_) =>
                      journal.persistEntry(
                        this.persistenceId,
                        ev,
                        O.some(_.updatedState)
                      )
                    ),
                    T.chain((s) =>
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
                T.foldM(
                  (__) => T.zipRight_(P.fail_(_.promise, e), T.unit),
                  _.fullCompleter
                )
              ),
            _.fullCompleter
          )
        )
      )
    }

    const retrieveJournal = pipe(
      T.service(JournalFactory),
      T.chain((jf) => jf.getJournal<S, EV>(context.actorSystem.actorSystemName))
    )

    return (initial) =>
      pipe(
        T.do,
        T.bind("journal", (_) => retrieveJournal),
        T.bind("state", () => REF.makeRef(initial)),
        T.bind("queue", () => Q.makeBounded<A.PendingMessage<F1>>(mailboxSize)),
        T.tap((_) =>
          pipe(
            // NOTE(mattiamanzati): We run the rehydration in the forked job,
            // so that the mailbox can start receiving messages an be queued up
            // until the actor can start rehydrating.
            T.do,
            T.bind("journalEntry", () => _.journal.getEntry(this.persistenceId)),
            T.let("persistedState", (_) => _.journalEntry[0]),
            T.let("persistedEvents", (_) => _.journalEntry[1]),
            T.tap((__) =>
              O.fold_(
                __.persistedState,
                () => T.unit,
                (s) => REF.set_(_.state, s)
              )
            ),
            T.tap((__) =>
              pipe(
                __.persistedEvents,
                S.mapM((ev) => REF.update_(_.state, (s) => this.sourceEvent(s)(ev))),
                S.runDrain
              )
            ),
            T.tap(() =>
              pipe(
                Q.take(_.queue),
                T.tap((t) => process(t, _.state, _.journal)),
                T.forever
              )
            ),
            T.fork
          )
        ),
        T.map((_) => new A.Actor(_.queue, optOutActorSystem))
      )
  }
}
