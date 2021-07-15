import * as AC from "@effect-ts/actors/Actor"
import type { ActorRef } from "@effect-ts/actors/ActorRef"
import { actorRef } from "@effect-ts/actors/ActorRef"
import { ActorSystemTag, LiveActorSystem } from "@effect-ts/actors/ActorSystem"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import { Chunk } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Sh from "@effect-ts/core/Effect/Schedule"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import type { _A } from "@effect-ts/core/Utils"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"

import * as Cluster from "../src/Cluster"
import { RemotingExpress, StaticRemotingExpressConfig } from "../src/Remote"
import * as Singleton from "../src/Singleton"
import { TestKeeperConfig } from "./zookeeper"

const Remoting = RemotingExpress["<<<"](
  StaticRemotingExpressConfig({ host: "127.0.0.1", port: 34322 })
)

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">>>"](Remoting[">+>"](Cluster.LiveCluster))
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

const SubscriberMessages = AM.messages({ SendMessage, GetMessages })
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

const HubMessages = AM.messages({ Subscribe, Publish })

const handlerHub = AC.stateful(
  HubMessages,
  S.props({
    subscribed: S.prop(S.chunk(actorRef<SubscriberMessages>()))
  })
)(({ state }) => ({
  Subscribe: ({ recipient }) =>
    T.gen(function* (_) {
      yield* _(recipient.ask(new SendMessage({ message: "it works" })))
      return yield* _(
        pipe(
          state.get,
          T.chain((s) =>
            state.set({ subscribed: Chunk.append_(s.subscribed, recipient) })
          )
        )
      )
    }),
  Publish: ({ message }) =>
    T.gen(function* (_) {
      const s = yield* _(state.get)
      yield* _(
        T.forEach_(s.subscribed, (recipient) =>
          recipient.ask(new SendMessage({ message }))
        )
      )
      return yield* _(state.set(s))
    })
}))

const handlerSub = AC.stateful(
  SubscriberMessages,
  S.chunk(S.string)
)(({ state }) => ({
  SendMessage: (_) =>
    pipe(
      state.get,
      T.map((s) => Chunk.append_(s, _.message)),
      T.chain(state.set)
    ),
  GetMessages: (_) =>
    pipe(
      state.get,
      T.map((messages) => ({ messages }))
    )
}))

export const makeProcessService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const hub = yield* _(
    system.make(
      "hub",
      SUP.none,
      Singleton.makeSingleton(handlerHub, (self) =>
        T.gen(function* (_) {
          yield* _(
            T.repeat_(self.ask(new Publish({ message: "tick" })), Sh.windowed(1_000))
          )
          return yield* _(T.never)
        })
      ),
      {
        subscribed: Chunk.empty<ActorRef<SendMessage | GetMessages>>()
      }
    )
  )

  const sub = yield* _(
    system.make("sub", SUP.none, Singleton.makeSingleton(handlerSub), Chunk.empty())
  )

  return {
    hub,
    sub
  }
})

export interface ProcessService extends _A<typeof makeProcessService> {}
export const ProcessService = tag<ProcessService>()
export const LiveProcessService = L.fromManaged(ProcessService)(makeProcessService)

describe("Messaging", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](LiveProcessService)))
  )

  it("communicate", () =>
    T.gen(function* (_) {
      const { hub, sub } = yield* _(ProcessService)
      yield* _(hub.ask(new Subscribe({ recipient: sub })))

      expect((yield* _(sub.ask(new GetMessages()))).messages).equals(
        Chunk.single("it works")
      )

      yield* _(J.adjust(5_000))

      expect((yield* _(sub.ask(new GetMessages()))).messages).equals(
        Chunk.many("it works", "tick", "tick", "tick", "tick", "tick")
      )
    }))
})
