import type * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"

import type * as A from "../Actor"
import type { Throwable } from "../common"
import type * as AM from "../Message"

export interface ActorRef<F1 extends AM.AnyMessage> {
  /**
   * Send a message to an actor as `ask` interaction pattern -
   * caller is blocked until the response is received
   *
   * @param fa message
   * @tparam A return type
   * @return effectful response
   */
  ask<A extends F1>(msg: A): T.IO<Throwable, AM.ResponseOf<A>>

  /**
   * Send message to an actor as `fire-and-forget` -
   * caller is blocked until message is enqueued in stub's mailbox
   *
   * @param fa message
   * @return lifted unit
   */
  tell(msg: F1): T.IO<Throwable, void>

  /**
   * Get referential absolute actor path
   * @return
   */
  readonly path: T.UIO<string>
  /**
   * Stops actor and all its children
   */
  readonly stop: T.IO<Throwable, CH.Chunk<void>>
  /**
   * Contains the Schema for the commands and responses of the ActorRef
   */
  readonly messages: AM.MessageRegistry<F1>
}

export class ActorRefLocal<F1 extends AM.AnyMessage> implements ActorRef<F1> {
  constructor(
    private readonly actorPath: string,
    private readonly actor: A.Actor<F1>
  ) {}

  ask<A extends F1>(msg: A) {
    return this.actor.ask(msg)
  }
  tell(msg: F1) {
    return this.actor.tell(msg)
  }

  readonly stop = this.actor.stop
  readonly path = T.succeed(this.actorPath)
  readonly messages = this.actor.messages
}
