import type * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as S from "@effect-ts/schema"
import * as Th from "@effect-ts/schema/These"
import { pipe } from "@effect-ts/system/Function"

import type * as A from "../Actor"
import { ActorSystem } from "../ActorSystem"
import * as AA from "../Address"
import type { ActorSystemException, Throwable } from "../common"
import * as Envelope from "../Envelope"
import type * as AM from "../Message"

export class ActorRefParserE
  extends S.DefaultLeafE<{
    readonly actual: unknown
    readonly message: string
  }>
  implements S.Actual<unknown>
{
  readonly _tag = "ExtractKey"

  get [S.toTreeSymbol](): S.Tree<string> {
    return S.tree(
      `cannot parse ActorRef from ${JSON.stringify(this.actual)} (${this.message})`
    )
  }
}

let globSys: any = undefined

export function withSystem(s: ActorSystem) {
  return <A>(f: () => A): A => {
    const a = globSys
    globSys = s
    const r = f()
    globSys = a
    return r
  }
}

export function actorRef<F1 extends AM.AnyMessage>(registry: AM.MessageRegistry<F1>) {
  return pipe(
    S.identity(
      (_): _ is ActorRef<F1> =>
        _ instanceof ActorRefLocal || _ instanceof ActorRefRemote
    ),
    S.encoder((s) => s.path),
    S.parser((u): Th.These<S.LeafE<ActorRefParserE>, ActorRef<F1>> => {
      if (globSys instanceof ActorSystem) {
        const s = S.string.Parser(u)
        if (s.effect._tag === "Right") {
          return Th.succeed(
            new ActorRefRemote(AA.address(s.effect.right.get(0), registry), globSys)
          )
        }
        return Th.fail(
          S.leafE(new ActorRefParserE({ actual: u, message: "malformed" }))
        )
      } else {
        return Th.fail(
          S.leafE(new ActorRefParserE({ actual: u, message: "system not available" }))
        )
      }
    })
  )
}

export interface ActorRef<F1 extends AM.AnyMessage> {
  /**
   * Send a message to an actor as `ask` interaction pattern -
   * caller is blocked until the response is received
   *
   * @param fa message
   * @tparam A return type
   * @return effectful response
   */
  ask<A extends F1>(
    msg: A
  ): T.Effect<T.DefaultEnv, AM.ErrorOf<A> | ActorSystemException, AM.ResponseOf<A>>

  /**
   * Send message to an actor as `fire-and-forget` -
   * caller is blocked until message is enqueued in stub's mailbox
   *
   * @param fa message
   * @return lifted unit
   */
  tell(msg: F1): T.IO<ActorSystemException, void>

  /**
   * Get referential absolute actor path
   * @return
   */
  readonly path: T.UIO<AA.Address<F1>>

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
    private readonly address: AA.Address<F1>,
    private readonly actor: A.Actor<F1>
  ) {}

  ask<A extends F1>(msg: A) {
    return this.actor.ask(msg)
  }
  tell(msg: F1) {
    return this.actor.tell(msg)
  }

  readonly stop = this.actor.stop
  readonly path = T.succeed(this.address)
  readonly messages = this.actor.messages
}

export class ActorRefRemote<F1 extends AM.AnyMessage> implements ActorRef<F1> {
  constructor(
    private readonly address: AA.Address<F1>,
    private readonly system: ActorSystem
  ) {}

  ask<A extends F1>(
    msg: A
  ): T.IO<AM.ErrorOf<A> | ActorSystemException, AM.ResponseOf<A>>
  ask<A extends F1>(msg: A) {
    return this.system.runEnvelope({
      command: Envelope.ask(msg),
      recipient: this.address.path
    })
  }

  tell(msg: F1): T.IO<ActorSystemException, void>
  tell(msg: F1) {
    return this.system.runEnvelope({
      command: Envelope.tell(msg),
      recipient: this.address.path
    })
  }

  // @ts-expect-error
  readonly stop: T.IO<Throwable, CH.Chunk<void>> = this.system.runEnvelope({
    command: Envelope.stop(),
    recipient: this.address.path
  })

  readonly path: T.UIO<AA.Address<F1>> = T.succeed(this.address)
  readonly messages = this.address.messages
}
