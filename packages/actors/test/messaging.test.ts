import { Chunk } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import { pipe } from "@effect-ts/core/Function"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "../src/Actor"
import { actorRef } from "../src/ActorRef"
import * as Cluster from "../src/Cluster"
import * as ClusterConfigSym from "../src/ClusterConfig"
import * as AM from "../src/Message"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = Z.LiveKeeperClient["<<<"](TestKeeperConfig)[">+>"](
  Cluster.LiveCluster["<<<"](
    ClusterConfigSym.StaticClusterConfig({
      sysName: "EffectTsActorsDemo",
      host: "127.0.0.1",
      port: 34322
    })
  )
)

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

class Subscribe extends AM.Message(
  "Subscribe",
  S.props({ recipient: S.prop(actorRef(SubscriberMessages)) }),
  unit
) {}

const HubMessages = AM.messages(Subscribe)

const hub = AC.stateful(
  HubMessages,
  unit
)((s) =>
  matchTag({
    Subscribe: ({ payload: { recipient } }, msg) =>
      T.gen(function* (_) {
        yield* _(recipient.ask(new SendMessage({ message: "it works" })))
        return yield* _(msg.return(s))
      })
  })
)

const Hub = Cluster.makeSingleton("hub")(hub, T.unit, () => T.never)

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
      const a = yield* _(Sub.actor)
      yield* _(Hub.ask(new Subscribe({ recipient: a })))

      expect((yield* _(Sub.ask(new GetMessages()))).messages).equals(
        Chunk.single("it works")
      )
    }))
})
