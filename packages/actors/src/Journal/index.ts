import * as C from "@effect-ts/core/Case"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HM from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import * as S from "@effect-ts/core/Effect/Experimental/Stream"
import * as REF from "@effect-ts/core/Effect/Ref"
import { pipe, tuple } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import type * as SCH from "@effect-ts/schema"

import type { Throwable } from "../Common"

export class PersistenceId extends C.Case<{ id: string }> {}

export interface Journal<S, EV> {
  persistEntry(
    persistenceId: PersistenceId,
    eventSchema: SCH.Standard<EV>,
    events: CH.Chunk<EV>,
    stateSchema: SCH.Standard<S>,
    state: O.Option<S>
  ): T.Effect<unknown, Throwable, void>
  getEntry(
    persistenceId: PersistenceId,
    eventSchema: SCH.Standard<EV>,
    stateSchema: SCH.Standard<S>
  ): T.Effect<
    unknown,
    Throwable,
    readonly [O.Option<S>, S.Stream<unknown, Throwable, EV>]
  >
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
    eventSchema: SCH.Standard<EV>,
    events: CH.Chunk<EV>,
    stateSchema: SCH.Standard<S>,
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
    persistenceId: PersistenceId,
    eventSchema: SCH.Standard<EV>,
    stateSchema: SCH.Standard<S>
  ): T.Effect<
    unknown,
    Throwable,
    readonly [O.Option<S>, S.Stream<unknown, Throwable, EV>]
  > {
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
