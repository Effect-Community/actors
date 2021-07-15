import * as AC from "@effect-ts/actors/Actor"
import { ActorSystemTag, LiveActorSystem } from "@effect-ts/actors/ActorSystem"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import type { _A } from "@effect-ts/core/Utils"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"

import {
  RemotingExpress,
  StaticRemotingExpressConfig
} from "../../actors-node/src/Remote"
import * as Cluster from "../src/Cluster"
import { TestKeeperConfig } from "./zookeeper"

const Remoting = RemotingExpress["<<<"](
  StaticRemotingExpressConfig({ host: "127.0.0.1", port: 34322 })
)

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">>>"](Remoting[">+>"](Cluster.LiveCluster))
  ["<+<"](Z.LiveKeeperClient["<<<"](TestKeeperConfig))

const unit = S.unknown["|>"](S.brand<void>())

class Reset extends AM.Message("Reset", S.props({}), unit) {}
class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}
class GetAndReset extends AM.Message("GetAndReset", S.props({}), S.number) {}

const Message = AM.messages({ Reset, Increase, Get, GetAndReset })
type Message = AM.TypeOf<typeof Message>

const statefulHandler = AC.stateful(
  Message,
  S.number
)(({ state }, ctx) => ({
  Reset: (_) => state.set(0),
  Increase: (_) =>
    pipe(
      state.get,
      T.chain((n) => state.set(n + 1))
    ),
  Get: (_) => state.get,
  GetAndReset: (_) =>
    pipe(
      ctx.self,
      T.chain((self) => self.tell(new Reset())),
      T.zipRight(state.get)
    )
}))

export const makeProcessService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const actor = yield* _(system.make("process-a", SUP.none, statefulHandler, 0))

  return {
    actor
  }
})

export interface ProcessService extends _A<typeof makeProcessService> {}
export const ProcessService = tag<ProcessService>()
export const LiveProcessService = L.fromManaged(ProcessService)(makeProcessService)

describe("Cluster", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](LiveProcessService)))
  )

  it("membership", () =>
    T.gen(function* (_) {
      const cluster = yield* _(Cluster.Cluster)

      expect(yield* _(cluster.members)).equals(
        Chunk.single(new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }))
      )
    }))

  it("singleton", () =>
    T.gen(function* (_) {
      const { actor } = yield* _(ProcessService)
      const system = yield* _(ActorSystemTag)

      const path = actor.address

      expect(path).equals("zio://EffectTsActorsDemo@127.0.0.1:34322/process-a")

      expect(yield* _(actor.ask(new Get()))).equals(0)

      yield* _(actor.ask(new Increase()))

      expect(yield* _(actor.ask(new Get()))).equals(1)

      const selected = yield* _(system.select(path))

      expect(yield* _(selected.ask(new Get()))).equals(1)
    }))
})
