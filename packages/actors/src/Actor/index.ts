import { pipe } from "@effect-ts/core"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import type * as SCH from "@effect-ts/schema"
import type { HasClock } from "@effect-ts/system/Clock"
import { tuple } from "@effect-ts/system/Function"

import type * as AS from "../ActorSystem"
import type { Throwable } from "../common"
import type * as EV from "../Envelope"
import * as AM from "../Message"
import type * as SUP from "../Supervisor"

export type PendingMessage<A extends AM.AnyMessage> = readonly [
  A,
  P.Promise<Throwable, AM.ResponseOf<A>>
]

export class Actor<F1 extends AM.AnyMessage> {
  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly queue: Q.Queue<PendingMessage<F1>>,
    readonly optOutActorSystem: () => T.Effect<T.DefaultEnv, Throwable, void>
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
      P.make<Throwable, AM.ResponseOf<A>>(),
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
      P.make<Throwable, any>(),
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
    optOutActorSystem: () => T.Effect<T.DefaultEnv, Throwable, void>,
    mailboxSize?: number
  ): (initial: S) => T.Effect<R & T.DefaultEnv, Throwable, Actor<F1>>
}

export type StatefulEnvelope<S, F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: {
    _tag: Tag
    payload: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
    return: (
      s: S,
      r: AM.ResponseOf<AM.ExtractTagged<F1, Tag>>
    ) => T.Effect<unknown, never, StatefulResponse<S, AM.ExtractTagged<F1, Tag>>>
  }
}[AM.TagsOf<F1>]

export type StatefulResponse<S, F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: readonly [S, AM.ResponseOf<AM.ExtractTagged<F1, Tag>>]
}[AM.TagsOf<F1>]

export function stateful<S, F1 extends AM.AnyMessage>(
  messages: AM.MessageRegistry<F1>,
  stateSchema: SCH.Standard<S>
) {
  return <R>(
    receive: (
      state: S,
      context: AS.Context<F1>
    ) => (
      msg: StatefulEnvelope<S, F1>
    ) => T.Effect<R, Throwable, StatefulResponse<S, F1>>
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
      state: S,
      context: AS.Context<F1>
    ) => (
      msg: StatefulEnvelope<S, F1>
    ) => T.Effect<R, Throwable, StatefulResponse<S, F1>>
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
            return: (s: S, r: AM.ResponseOf<F1>) => T.succeed(tuple(s, r))
          } as any)
        ),
        T.let(
          "completer",
          (_) =>
            ([s, a]: readonly [S, AM.ResponseOf<F1>]) =>
              pipe(
                REF.set_(state, s),
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
      )
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
