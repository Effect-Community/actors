import * as T from "@effect-ts/core/Effect"
import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as A from "./Actor"
import {Throwable, _ResponseOf} from "./common"

export interface ActorRef<F1> {
    /**
     * Send a message to an actor as `ask` interaction pattern -
     * caller is blocked until the response is received
     *
     * @param fa message
     * @tparam A return type
     * @return effectful response
     */
    ask<A extends F1>(msg: A): T.IO<Throwable, _ResponseOf<A>>

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
    path: T.UIO<string>
    /**
     * Stops actor and all its children
     */
    stop: T.IO<Throwable, CH.Chunk<void>>
}

export class ActorRefLocal<F1> implements ActorRef<F1> {
    constructor(
        readonly actorPath: string,
        readonly actor: A.Actor<F1>
    ){}

    ask<A extends F1>(msg: A){
        return this.actor.ask(msg)
    }
    tell(msg: F1){
        return this.actor.tell(msg)
    }

    stop = this.actor.stop
    path = T.succeed(this.actorPath)
}