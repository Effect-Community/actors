import type { PendingMessage } from "@effect-ts/actors/Actor"
import { AbstractStateful, Actor } from "@effect-ts/actors/Actor"
import { withSystem } from "@effect-ts/actors/ActorRef"
import * as AS from "@effect-ts/actors/ActorSystem"
import type { Throwable } from "@effect-ts/actors/Common"
import type * as AM from "@effect-ts/actors/Message"
import type * as SUP from "@effect-ts/actors/Supervisor"
import { Chunk, pipe } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import { pretty } from "@effect-ts/core/Effect/Cause"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import type { Has } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import type * as SCH from "@effect-ts/schema"
import * as S from "@effect-ts/schema"
import * as Encoder from "@effect-ts/schema/Encoder"
import * as Parser from "@effect-ts/schema/Parser"
import { identity } from "@effect-ts/system/Function"

import { Persistence } from "../Persistence"

export type TransactionalEnvelope<F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: {
    _tag: Tag
    payload: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
    handle: <R, E>(
      _: T.Effect<R, E, AM.ResponseOf<AM.ExtractTagged<F1, Tag>>>
    ) => T.Effect<R, E, AM.ResponseOf<AM.ExtractTagged<F1, Tag>>>
  }
}[AM.TagsOf<F1>]

export function transactional<S, F1 extends AM.AnyMessage, Ev = never>(
  messages: AM.MessageRegistry<F1>,
  stateSchema: SCH.Standard<S>,
  eventSchema: O.Option<SCH.Standard<Ev>>
) {
  return <R>(
    receive: (
      dsl: {
        state: {
          get: T.UIO<S>
          set: (s: S) => T.UIO<void>
        }
        event: {
          emit: (e: Ev) => T.UIO<void>
        }
      },
      context: AS.Context<F1>
    ) => (
      msg: TransactionalEnvelope<F1>
    ) => T.Effect<R, Throwable, AM.ResponseOf<AM.ExtractTagged<F1, F1["_tag"]>>>
  ) => new Transactional<R, S, Ev, F1>(messages, stateSchema, eventSchema, receive)
}

export class Transactional<R, S, Ev, F1 extends AM.AnyMessage> extends AbstractStateful<
  R & Has<Persistence>,
  S,
  F1
> {
  private readonly dbStateSchema = S.props({
    current: S.prop(this.stateSchema)
  })

  readonly decodeState = (s: unknown, system: AS.ActorSystem) =>
    S.condemnDie((u) => withSystem(system)(() => Parser.for(this.dbStateSchema)(u)))(s)

  readonly encodeState = Encoder.for(this.dbStateSchema)

  readonly encodeEvent = O.map_(this.eventSchema, (s) =>
    Encoder.for(S.props({ event: S.prop(s) }))
  )

  readonly getState = (initial: S, system: AS.ActorSystem, actorName: string) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { get } = yield* _(Persistence)

      const state = yield* _(get(actorName))

      if (O.isSome(state)) {
        return [
          (yield* _(self.decodeState(state.value.state, system))).current,
          state.value.event_sequence
        ] as const
      }
      return [initial, 0] as const
    })
  }

  readonly setState = (current: S, actorName: string, sequence: number) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { set } = yield* _(Persistence)

      yield* _(set(actorName, self.encodeState({ current }), sequence))
    })
  }

  readonly emitEvent = (event: Ev, actorName: string, sequence: number) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { emit } = yield* _(Persistence)
      const encode = yield* _(self.encodeEvent)

      yield* _(emit(actorName, encode({ event }), sequence))
    })
  }

  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly stateSchema: SCH.Standard<S>,
    readonly eventSchema: O.Option<SCH.Standard<Ev>>,
    readonly receive: (
      dsl: {
        state: {
          get: T.UIO<S>
          set: (s: S) => T.UIO<void>
        }
        event: {
          emit: (e: Ev) => T.UIO<void>
        }
      },
      context: AS.Context<F1>
    ) => (
      msg: TransactionalEnvelope<F1>
    ) => T.Effect<R, Throwable, AM.ResponseOf<AM.ExtractTagged<F1, F1["_tag"]>>>
  ) {
    super()
  }

  defaultMailboxSize = 10000

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context<F1>,
    optOutActorSystem: () => T.Effect<T.DefaultEnv, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (initial: S) => T.RIO<R & T.DefaultEnv & Has<Persistence>, Actor<F1>> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    const process = (msg: PendingMessage<F1>, initial: S) => {
      return T.accessServicesM({ prov: Persistence })(({ prov }) =>
        pipe(
          AS.resolvePath(context.address)["|>"](T.orDie),
          T.map(([sysName, __, ___, actorName]) => `${sysName}(${actorName})`),
          T.chain((actorName) =>
            prov.transaction(actorName)(
              pipe(
                T.do,
                T.bind("s", () =>
                  self.getState(initial, context.actorSystem, actorName)
                ),
                T.bind("events", () => REF.makeRef(Chunk.empty<Ev>())),
                T.bind("state", (_) => REF.makeRef(_.s[0])),
                T.let("fa", () => msg[0]),
                T.let("promise", () => msg[1]),
                T.let("receiver", (_) => {
                  return this.receive(
                    {
                      event: {
                        emit: (ev) => REF.update_(_.events, Chunk.append(ev))
                      },
                      state: { get: REF.get(_.state), set: (s) => REF.set_(_.state, s) }
                    },
                    context
                  )({ _tag: _.fa._tag as any, payload: _.fa as any, handle: identity })
                }),
                T.let(
                  "completer",
                  (_) => (a: AM.ResponseOf<F1>) =>
                    pipe(
                      T.zip_(REF.get(_.events), REF.get(_.state)),
                      T.chain(({ tuple: [evs, s] }) =>
                        T.zip_(
                          self.setState(s, actorName, _.s[1] + evs.length),
                          T.forEach_(
                            Chunk.zipWithIndexOffset_(evs, _.s[1] + 1),
                            ({ tuple: [ev, seq] }) => self.emitEvent(ev, actorName, seq)
                          )
                        )
                      ),
                      T.zipRight(P.succeed_(_.promise, a)),
                      T.as(T.unit)
                    )
                ),
                T.chain((_) =>
                  T.foldM_(
                    _.receiver,
                    (e) =>
                      pipe(
                        supervisor.supervise(_.receiver, e),
                        T.foldM((__) => P.fail_(_.promise, e), _.completer)
                      ),
                    _.completer
                  )
                )
              )["|>"](
                T.tapCause((c) =>
                  T.succeedWith(() => {
                    console.error(pretty(c))
                  })
                )
              )
            )
          )
        )
      )
    }

    return (initial) =>
      pipe(
        T.do,
        T.bind("state", () => REF.makeRef(initial)),
        T.bind("queue", () => Q.makeBounded<PendingMessage<F1>>(mailboxSize)),
        T.bind("ref", () => REF.makeRef(O.emptyOf<S>())),
        T.tap((_) =>
          pipe(
            Q.take(_.queue),
            T.chain((t) => process(t, initial)),
            T.forever,
            T.fork
          )
        ),
        T.map((_) => new Actor(this.messages, _.queue, optOutActorSystem))
      )
  }
}
