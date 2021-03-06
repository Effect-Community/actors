import { withSystem } from "@effect-ts/actors/ActorRef"
import {
  ActorSystem,
  ActorSystemTag,
  RemoteConfig
} from "@effect-ts/actors/ActorSystem"
import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as Ex from "@effect-ts/express"
import * as S from "@effect-ts/schema"
import * as Encoder from "@effect-ts/schema/Encoder"
import * as Parser from "@effect-ts/schema/Parser"
import * as exp from "express"

export const CallPayload = S.props({
  _tag: S.prop(S.string),
  op: S.prop(S.literal("Ask", "Tell", "Stop")).opt(),
  path: S.prop(S.string),
  request: S.prop(S.unknown).opt()
})

export const decodePayload = CallPayload.Parser["|>"](S.condemnFail)
export const encodePayload = CallPayload.Encoder

export class ActorError extends Tagged("ActorError")<{
  readonly message: string
}> {}

export const RemotingExpressConfigSym = Symbol()

export interface RemotingExpressConfig {
  [RemotingExpressConfigSym]: typeof RemotingExpressConfigSym
  host: string
  port: number
  bindAddr?: string | undefined
}

export const makeRemotingExpressConfig = (_: {
  host: string
  port: number
  bindAddr?: string
}): RemotingExpressConfig => ({
  [RemotingExpressConfigSym]: RemotingExpressConfigSym,
  host: _.host,
  port: _.port,
  bindAddr: _.bindAddr
})

export const RemotingExpressConfig = tag<RemotingExpressConfig>()

export function StaticRemotingExpressConfig(_: {
  host: string
  port: number
  bindAddr?: string
}) {
  return L.fromEffect(RemotingExpressConfig)(T.succeed(makeRemotingExpressConfig(_)))
}

export const RemotingExpress = L.fresh(
  Ex.LiveExpressApp["<+<"](
    L.fromEffect(Ex.ExpressAppConfig)(
      T.accessService(RemotingExpressConfig)(({ bindAddr, port }) => ({
        _tag: Ex.ExpressAppConfigTag,
        exitHandler: Ex.defaultExitHandler,
        host: bindAddr ?? "0.0.0.0",
        port
      }))
    )
  )
)[">>>"](
  L.fresh(
    L.fromRawManaged(
      M.gen(function* (_) {
        const { host, port } = yield* _(RemotingExpressConfig)
        const system = yield* _(ActorSystemTag)

        yield* _(
          Ex.post("/cmd", Ex.classic(exp.json()), (req, res) =>
            pipe(
              T.gen(function* (_) {
                const payload = yield* _(decodePayload(req.body))
                const actor = yield* _(system.local(payload.path))

                switch (payload.op) {
                  case "Ask": {
                    const msgArgs = yield* _(
                      S.condemnFail((u) =>
                        withSystem(system)(() =>
                          Parser.for(actor.messages[payload._tag].RequestSchema)(u)
                        )
                      )(payload.request)
                    )

                    const msg = new actor.messages[payload._tag](msgArgs)

                    const resp = yield* _(
                      actor.ask(msg)["|>"](
                        T.mapError(
                          (s) =>
                            new ActorError({
                              message: `actor error: ${JSON.stringify(s)}`
                            })
                        )
                      )
                    )

                    res.send(
                      JSON.stringify({
                        response: Encoder.for(
                          actor.messages[payload._tag].ResponseSchema
                        )(resp)
                      })
                    )

                    break
                  }
                  case "Tell": {
                    const msgArgs = yield* _(
                      S.condemnFail((u) =>
                        withSystem(system)(() =>
                          Parser.for(actor.messages[payload._tag].RequestSchema)(u)
                        )
                      )(payload.request)
                    )

                    const msg = new actor.messages[payload._tag](msgArgs)

                    yield* _(
                      actor.tell(msg)["|>"](
                        T.mapError(
                          (s) =>
                            new ActorError({
                              message: `actor error: ${JSON.stringify(s)}`
                            })
                        )
                      )
                    )

                    res.send(JSON.stringify({ tell: true }))

                    break
                  }
                  case "Stop": {
                    const stops = yield* _(
                      actor.stop["|>"](
                        T.mapError(
                          (s) =>
                            new ActorError({
                              message: `actor error: ${JSON.stringify(s)}`
                            })
                        )
                      )
                    )

                    res.send(JSON.stringify({ stops: Chunk.toArray(stops) }))

                    break
                  }
                }
              }),
              T.catchTag("CondemnException", (s) =>
                T.succeedWith(() => res.status(500).send({ message: s.message }))
              ),
              T.catchTag("NoSuchActorException", () =>
                T.succeedWith(() =>
                  res.status(500).send({ message: "actor not found" })
                )
              ),
              T.catchTag("InvalidActorPath", () =>
                T.succeedWith(() =>
                  res.status(500).send({ message: "malformed actor path" })
                )
              ),
              T.catchTag("ActorError", (s) =>
                T.succeedWith(() => res.status(500).send({ message: s.message }))
              )
            )
          )
        )

        return ActorSystemTag.of(
          new ActorSystem(
            system.actorSystemName,
            O.some(new RemoteConfig({ host, port })),
            system.refActorMap,
            system.parentActor
          )
        )
      })
    )
  )
)
