import { pipe } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import { pretty } from "@effect-ts/core/Effect/Cause"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import { flow } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as PG from "@effect-ts/pg"
import type * as SCH from "@effect-ts/schema"
import * as S from "@effect-ts/schema"
import * as Encoder from "@effect-ts/schema/Encoder"
import * as Parser from "@effect-ts/schema/Parser"
import type { HasClock } from "@effect-ts/system/Clock"
import { identity, tuple } from "@effect-ts/system/Function"

import type { PendingMessage, StatefulEnvelope, StatefulResponse } from "../Actor"
import { AbstractStateful, Actor } from "../Actor"
import { withSystem } from "../ActorRef"
import * as AS from "../ActorSystem"
import type { Throwable } from "../common"
import type * as AM from "../Message"
import type * as SUP from "../Supervisor"

export function transactional<S, F1 extends AM.AnyMessage>(
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
  ) => new Transactional<R, S, F1>(messages, stateSchema, receive)
}

export interface StateStorageAdapter {
  readonly transaction: <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R, E, A>

  readonly get: (actorName: string) => T.Effect<unknown, never, O.Option<string>>

  readonly set: (actorName: string, value: string) => T.Effect<unknown, never, void>
}

export const StateStorageAdapter = tag<StateStorageAdapter>()

export const LiveStateStorageAdapter = L.fromManaged(StateStorageAdapter)(
  M.gen(function* (_) {
    const cli = yield* _(PG.PG)

    yield* _(
      cli.query(`
      CREATE TABLE IF NOT EXISTS "state_journal" (
        actor_name  text PRIMARY KEY,
        state       text
      );`)
    )

    const transaction: <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R, E, A> =
      cli.transaction

    const get: (actorName: string) => T.Effect<unknown, never, O.Option<string>> = (
      id
    ) =>
      pipe(
        cli.query(`SELECT * FROM "state_journal" WHERE "actor_name" = '${id}'`),
        T.map((res) => O.fromNullable(res.rows?.[0]?.["state"]))
      )

    const set: (actorName: string, value: string) => T.Effect<unknown, never, void> = (
      id,
      value
    ) =>
      pipe(
        cli.query(
          `INSERT INTO "state_journal" ("actor_name", "state") VALUES('${id}', $1::text) ON CONFLICT ("actor_name") DO UPDATE SET "state" = $1::text`,
          [value]
        ),
        T.asUnit
      )

    return identity<StateStorageAdapter>({
      transaction,
      get,
      set
    })
  })
)

export class Transactional<R, S, F1 extends AM.AnyMessage> extends AbstractStateful<
  R & Has<StateStorageAdapter>,
  S,
  F1
> {
  readonly decodeState = (s: string, system: AS.ActorSystem) =>
    S.condemnDie((u) => withSystem(system)(() => Parser.for(this.stateSchema)(u)))(
      JSON.parse(s)["state"]
    )

  readonly encodeState = flow(Encoder.for(this.stateSchema), (s) =>
    JSON.stringify({ state: s })
  )

  readonly getState = (initial: S, system: AS.ActorSystem, actorName: string) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { get } = yield* _(StateStorageAdapter)

      const state = yield* _(get(actorName))

      if (O.isSome(state)) {
        return yield* _(self.decodeState(state.value, system))
      }
      return initial
    })
  }

  readonly setState = (current: S, actorName: string) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { set } = yield* _(StateStorageAdapter)

      yield* _(set(actorName, self.encodeState(current)))
    })
  }

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
  ): (initial: S) => T.RIO<R & HasClock & Has<StateStorageAdapter>, Actor<F1>> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    const process = (
      msg: PendingMessage<F1>,
      initial: S,
      ref: REF.Ref<O.Option<S>>
    ): T.RIO<R & HasClock & Has<StateStorageAdapter>, void> => {
      return T.accessServicesM({ prov: StateStorageAdapter })(({ prov }) =>
        prov.transaction(
          pipe(
            AS.resolvePath(context.address.path)["|>"](T.orDie),
            T.map(([sysName, __, ___, actorName]) => `${sysName}(${actorName})`),
            T.chain((actorName) =>
              pipe(
                T.do,
                T.bind("s", () =>
                  pipe(
                    REF.get(ref),
                    T.chain((ms) =>
                      O.isSome(ms)
                        ? T.succeed(ms.value)
                        : self.getState(initial, context.actorSystem, actorName)
                    )
                  )
                ),
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
                        self.setState(s, actorName),
                        T.zipRight(REF.set_(ref, O.some(s))),
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
                ),
                T.tapCause((x) => T.succeedWith(() => console.error(pretty(x))))
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
            T.chain((t) => process(t, initial, _.ref)),
            T.forever,
            T.fork
          )
        ),
        T.map((_) => new Actor(this.messages, _.queue, optOutActorSystem))
      )
  }
}
