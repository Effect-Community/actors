import { pipe } from "@effect-ts/core"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import type { HasClock } from "@effect-ts/system/Clock"
import { tuple } from "@effect-ts/system/Function"

import type * as AS from "../ActorSystem"
import type { Throwable } from "../common"
import type * as EV from "../Envelope"
import type * as AM from "../Message"
import type * as SUP from "../Supervisor"

export type PendingMessage<A extends AM.AnyMessage> = readonly [
  A,
  P.Promise<Throwable, AM.ResponseOf<A>>
]

export class Actor<F1 extends AM.AnyMessage> {
  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly queue: Q.Queue<PendingMessage<F1>>,
    readonly optOutActorSystem: () => T.Effect<unknown, Throwable, void>
  ) {}

  unsafeOp(command: EV.Command) {
    switch (command._tag) {
      case "Ask":
        return this.ask(command.msg)
      case "Tell":
        return this.tell(command.msg)
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
  abstract makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context,
    optOutActorSystem: () => T.Effect<unknown, Throwable, void>,
    mailboxSize?: number
  ): (initial: S) => T.Effect<R & HasClock, Throwable, Actor<F1>>
}

type StatefulEnvelope<S, F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: {
    _tag: Tag
    payload: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
    return: (
      s: S,
      r: AM.ResponseOf<AM.ExtractTagged<F1, Tag>>
    ) => T.Effect<unknown, never, StatefulResponse<S, AM.ExtractTagged<F1, Tag>>>
  }
}[AM.TagsOf<F1>]

type StatefulResponse<S, F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: readonly [S, AM.ResponseOf<AM.ExtractTagged<F1, Tag>>]
}[AM.TagsOf<F1>]

export class Stateful<R, S, F1 extends AM.AnyMessage> extends AbstractStateful<
  R,
  S,
  F1
> {
  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly receive: (
      state: S,
      context: AS.Context
    ) => (
      msg: StatefulEnvelope<S, F1>
    ) => T.Effect<R, Throwable, StatefulResponse<S, F1>>
  ) {
    super()
  }

  defaultMailboxSize = 10000

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context,
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