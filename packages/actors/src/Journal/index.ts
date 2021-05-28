import * as C from "@effect-ts/core/Case"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HM from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as S from "@effect-ts/core/Effect/Stream"
import { pipe, tuple } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"

import type { Throwable } from "../common"

export class PersistenceId extends C.Case<{ id: string }> {}

export interface Journal<S, EV> {
  persistEntry(
    persistenceId: PersistenceId,
    events: CH.Chunk<EV>,
    state: O.Option<S>
  ): T.Effect<unknown, Throwable, void>
  getEntry(
    persistenceId: PersistenceId
  ): T.Effect<unknown, Throwable, readonly [O.Option<S>, S.IO<Throwable, EV>]>
}

export interface JournalFactory {
  getJournal<S, EV>(
    actorSystemName: string
  ): T.Effect<unknown, Throwable, Journal<S, EV>>
}
export const JournalFactory = tag<JournalFactory>()

export class InMemJournal<S, EV> implements Journal<S, EV> {
  constructor(
    private readonly journalRef: REF.Ref<HM.HashMap<PersistenceId, CH.Chunk<EV>>>
  ) {}

  persistEntry(
    persistenceId: PersistenceId,
    events: CH.Chunk<EV>,
    state: O.Option<S>
  ): T.Effect<unknown, Throwable, void> {
    return pipe(
      REF.update_(
        this.journalRef,
        HM.modify(persistenceId, (a) =>
          O.some(
            CH.reduce_(
              events,
              O.getOrElse_(a, () => CH.empty<EV>()),
              (s, a) => CH.append_(s, a)
            )
          )
        )
      )
    )
  }

  getEntry(
    persistenceId: PersistenceId
  ): T.Effect<unknown, Throwable, readonly [O.Option<S>, S.IO<Throwable, EV>]> {
    return pipe(
      REF.get(this.journalRef),
      T.map((_) => O.getOrElse_(HM.get_(_, persistenceId), () => CH.empty<EV>())),
      T.map((e) => tuple(O.none as O.Option<S>, S.fromChunk(e)))
    )
  }
}

export class InMemJournalFactory implements JournalFactory {
  constructor(
    private readonly journalMap: REF.Ref<HM.HashMap<string, InMemJournal<any, any>>>
  ) {}

  getJournal<S, EV>(
    actorSystemName: string
  ): T.Effect<unknown, Throwable, Journal<S, EV>> {
    const currentJournal = pipe(
      REF.get(this.journalMap),
      T.map(HM.get(actorSystemName))
    )

    return pipe(
      currentJournal,
      T.chain(
        O.fold(
          () =>
            pipe(
              T.do,
              T.bind("journalRef", (_) =>
                REF.makeRef(HM.make<PersistenceId, CH.Chunk<EV>>())
              ),
              T.map((_) => new InMemJournal<S, EV>(_.journalRef)),
              T.tap((_) => REF.update_(this.journalMap, HM.set(actorSystemName, _)))
            ),
          T.succeed
        )
      )
    )
  }
}

export const makeInMemJournal = pipe(
  REF.makeRef<HM.HashMap<string, InMemJournal<any, any>>>(HM.make()),
  T.map((_) => new InMemJournalFactory(_))
)
