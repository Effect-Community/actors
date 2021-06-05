import { Chunk } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import * as Sh from "@effect-ts/core/Effect/Schedule"
import { pipe } from "@effect-ts/core/Function"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "../src/Actor"
import type { ActorRef } from "../src/ActorRef"
import { actorRef } from "../src/ActorRef"
import { LiveActorSystem } from "../src/ActorSystem"
import * as Cluster from "../src/Cluster"
import * as AM from "../src/Message"
import { RemoteExpress } from "../src/Remote"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">>>"](RemoteExpress("127.0.0.1", 34322)[">+>"](Cluster.LiveCluster))
  ["<+<"](Z.LiveKeeperClient["<<<"](TestKeeperConfig))

const unit = S.unknown["|>"](S.brand<void>())

class SendMessage extends AM.Message(
  "SendMessage",
  S.props({ message: S.prop(S.string) }),
  unit
) {}

class GetMessages extends AM.Message(
  "GetMessages",
  S.props({}),
  S.props({ messages: S.prop(S.chunk(S.string)) })
) {}

const SubscriberMessages = AM.messages(SendMessage, GetMessages)
type SubscriberMessages = AM.TypeOf<typeof SubscriberMessages>

class Subscribe extends AM.Message(
  "Subscribe",
  S.props({ recipient: S.prop(actorRef<SubscriberMessages>()) }),
  unit
) {}

class Publish extends AM.Message(
  "Publish",
  S.props({ message: S.prop(S.string) }),
  unit
) {}

const HubMessages = AM.messages(Subscribe, Publish)

const hub = AC.stateful(
  HubMessages,
  S.props({
    subscribed: S.prop(S.chunk(actorRef<SubscriberMessages>()))
  })
)((s) =>
  matchTag({
    Subscribe: ({ payload: { recipient } }, msg) =>
      T.gen(function* (_) {
        yield* _(recipient.ask(new SendMessage({ message: "it works" })))
        return yield* _(
          msg.return({ subscribed: Chunk.append_(s.subscribed, recipient) })
        )
      }),
    Publish: ({ payload: { message } }, msg) =>
      T.gen(function* (_) {
        yield* _(
          T.forEach_(s.subscribed, (recipient) =>
            recipient.ask(new SendMessage({ message }))
          )
        )
        return yield* _(msg.return(s))
      })
  })
)

const Hub = Cluster.makeSingleton("hub")(
  hub,
  T.succeed({ subscribed: Chunk.empty<ActorRef<SendMessage | GetMessages>>() }),
  (self) =>
    T.gen(function* (_) {
      yield* _(
        T.repeat_(self.ask(new Publish({ message: "tick" })), Sh.windowed(1_000))
      )
      return yield* _(T.never)
    })
)

const sub = AC.stateful(
  SubscriberMessages,
  S.chunk(S.string)
)((s) =>
  matchTag({
    SendMessage: (_) => _.return(Chunk.append_(s, _.payload.message)),
    GetMessages: (_) => _.return(s, { messages: s })
  })
)

const Sub = Cluster.makeSingleton("sub")(sub, T.succeed(Chunk.empty()), () => T.never)

describe("Messaging", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](Hub.Live["+++"](Sub.Live))))
  )

  it("communicate", () =>
    T.gen(function* (_) {
      yield* _(Hub.ask(new Subscribe({ recipient: yield* _(Sub.actor) })))

      expect((yield* _(Sub.ask(new GetMessages()))).messages).equals(
        Chunk.single("it works")
      )

      yield* _(J.adjust(5_000))

      expect((yield* _(Sub.ask(new GetMessages()))).messages).equals(
        Chunk.many("it works", "tick", "tick", "tick", "tick", "tick", "tick")
      )
    }))
})
