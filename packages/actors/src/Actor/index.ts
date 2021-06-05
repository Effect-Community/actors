import { pipe } from "@effect-ts/core"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import type { _R } from "@effect-ts/core/Utils"
import type * as SCH from "@effect-ts/schema"
import type { HasClock } from "@effect-ts/system/Clock"
import { tuple } from "@effect-ts/system/Function"

import type * as AS from "../ActorSystem"
import type { ActorSystemException, Throwable } from "../common"
import type * as EV from "../Envelope"
import * as AM from "../Message"
import type * as SUP from "../Supervisor"

export type PendingMessage<A extends AM.AnyMessage> = readonly [
  A,
  P.Promise<AM.ErrorOf<A> | ActorSystemException, AM.ResponseOf<A>>
]

export class Actor<F1 extends AM.AnyMessage> {
  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly queue: Q.Queue<PendingMessage<F1>>,
    readonly optOutActorSystem: () => T.Effect<unknown, Throwable, void>
  ) {}

  runOp(command: EV.Command) {
    switch (command._tag) {
      case "Ask":
        return T.chain_(AM.decodeCommand(this.messages)(command.msg), ([msg, encode]) =>
          T.map_(this.ask(msg), encode)
        )
      case "Tell":
        return T.chain_(AM.decodeCommand(this.messages)(command.msg), ([msg]) =>
          this.tell(msg)
        )
      case "Stop":
        return this.stop
    }
  }

  ask<A extends F1>(fa: A) {
    return pipe(
      P.make<AM.ErrorOf<A> | ActorSystemException, AM.ResponseOf<A>>(),
      T.tap((promise) => Q.offer_(this.queue, tuple(fa, promise))),
      T.chain(P.await)
    )
  }

  /**
   * This is a fire-and-forget operation, it justs put a message onto the actor queue,
   * so there is no guarantee that it actually gets consumed by the actor.
   */
  tell(fa: F1) {
    return pipe(
      P.make<AM.ErrorOf<F1> | ActorSystemException, any>(),
      T.chain((promise) => Q.offer_(this.queue, tuple(fa, promise))),
      T.zipRight(T.unit)
    )
  }

  readonly stop = pipe(
    Q.takeAll(this.queue),
    T.tap(() => Q.shutdown(this.queue)),
    T.tap(this.optOutActorSystem),
    T.map(CH.map((_) => undefined)) // TODO: wtf? maybe casting to any would be better?
  )
}

export abstract class AbstractStateful<R, S, F1 extends AM.AnyMessage> {
  abstract readonly messages: AM.MessageRegistry<F1>
  abstract makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context<any>,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize?: number
  ): (initial: S) => T.Effect<R & HasClock, Throwable, Actor<F1>>
}

export type StatefulEnvelope<F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: {
    _tag: Tag
    payload: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
  }
}[AM.TagsOf<F1>]

export interface StatefulMatcher<F1 extends AM.AnyMessage> {
  <
    X extends {
      [Tag in AM.TagsOf<F1>]: (
        _: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
      ) => T.Effect<
        any,
        AM.ErrorOf<AM.ExtractTagged<F1, Tag>>,
        AM.ResponseOf<AM.ExtractTagged<F1, Tag>>
      >
    }
  >(
    handlers: X
  ): (
    msg: StatefulEnvelope<F1>
  ) => T.Effect<_R<ReturnType<X[AM.TagsOf<F1>]>>, AM.ErrorOf<F1>, AM.ResponseOf<F1>>
}

export function stateful<S, F1 extends AM.AnyMessage>(
  messages: AM.MessageRegistry<F1>,
  stateSchema: SCH.Standard<S>
) {
  return <R>(
    receive: (
      state: REF.Ref<S>,
      context: AS.Context<F1>,
      matchTag: StatefulMatcher<F1>
    ) => (msg: StatefulEnvelope<F1>) => T.Effect<R, AM.ErrorOf<F1>, AM.ResponseOf<F1>>
  ) => new Stateful<R, S, F1>(messages, stateSchema, receive)
}

export class Stateful<R, S, F1 extends AM.AnyMessage> extends AbstractStateful<
  R,
  S,
  F1
> {
  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly stateSchema: SCH.Standard<S>,
    readonly receive: (
      state: REF.Ref<S>,
      context: AS.Context<F1>,
      matchTag: StatefulMatcher<F1>
    ) => (msg: StatefulEnvelope<F1>) => T.Effect<R, AM.ErrorOf<F1>, AM.ResponseOf<F1>>
  ) {
    super()
  }

  defaultMailboxSize = 10000

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context<F1>,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (initial: S) => T.RIO<R & HasClock, Actor<F1>> {
    const process = (
      msg: PendingMessage<F1>,
      state: REF.Ref<S>
    ): T.RIO<R & HasClock, void> => {
      const receive = this.receive
      return T.gen(function* ($) {
        const s = yield* $(pipe(REF.get(state), T.chain(REF.makeRef)))
        const [fa, promise] = msg
        const receiver = receive(
          s,
          context,
          (rec) => (msg) => rec[msg._tag](msg.payload)
        )({ _tag: fa._tag, payload: fa as any })

        const completer = (a: AM.ResponseOf<F1>) =>
          pipe(
            REF.get(s),
            T.chain((_) => REF.set_(state, _)),
            T.zipRight(P.succeed_(promise, a)),
            T.as(T.unit)
          )

        return yield* $(
          T.foldM_(
            receiver,
            (e) =>
              pipe(
                supervisor.supervise(receiver, e),
                T.foldM(() => P.fail_(promise, e), completer)
              ),
            completer
          )
        )
      })
    }

    return (initial) =>
      pipe(
        T.do,
        T.bind("state", () => REF.makeRef(initial)),
        T.bind("queue", () => Q.makeBounded<PendingMessage<F1>>(mailboxSize)),
        T.tap((_) =>
          pipe(
            Q.take(_.queue),
            T.chain((t) => process(t, _.state)),
            T.forever,
            T.fork
          )
        ),
        T.map((_) => new Actor(this.messages, _.queue, optOutActorSystem))
      )
  }
}

export class ActorProxy<R, S, F1 extends AM.AnyMessage, E> extends AbstractStateful<
  R,
  S,
  F1
> {
  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly process: (
      queue: Q.Queue<PendingMessage<F1>>,
      context: AS.Context<F1>,
      initial: S
    ) => T.Effect<R, E, never>
  ) {
    super()
  }

  defaultMailboxSize = 10_000

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context<F1>,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (initial: S) => T.RIO<R & HasClock, Actor<F1>> {
    return (initial) => {
      return pipe(
        T.do,
        T.bind("queue", () => Q.makeBounded<PendingMessage<F1>>(mailboxSize)),
        T.bind("fiber", (_) => pipe(this.process(_.queue, context, initial), T.fork)),
        T.map((_) => new Actor(this.messages, _.queue, optOutActorSystem))
      )
    }
  }
}
