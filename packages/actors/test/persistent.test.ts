import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import type { _A } from "@effect-ts/core/Utils"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import { ActorSystemTag, LiveActorSystem } from "../src/ActorSystem"
import * as Cluster from "../src/Cluster"
import * as AM from "../src/Message"
import { RemotingExpress, StaticRemotingExpressConfig } from "../src/Remote"
import * as Singleton from "../src/Singleton"
import * as SUP from "../src/Supervisor"
import { LiveStateStorageAdapter, transactional } from "../src/Transactional"
import { TestPG } from "./pg"
import { TestKeeperConfig } from "./zookeeper"

const Remoting = RemotingExpress["<<<"](
  StaticRemotingExpressConfig({ host: "127.0.0.1", port: 34322 })
)

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">>>"](Remoting[">+>"](Cluster.LiveCluster))
  ["<+<"](Z.LiveKeeperClient["<<<"](TestKeeperConfig))
  ["<+<"](LiveStateStorageAdapter["<+<"](PG.LivePG["<<<"](TestPG)))

const unit = S.unknown["|>"](S.brand<void>())

class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}

const Message = AM.messages(Get, Increase)
type Message = AM.TypeOf<typeof Message>

const statefulHandler = transactional(
  Message,
  S.number
)((state) =>
  matchTag({
    Increase: (_) => _.return(state + 1),
    Get: (_) => _.return(state, state)
  })
)

export const makeProcessService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const processA = yield* _(
    system.make("process-a", SUP.none, Singleton.makeSingleton(statefulHandler), 0)
  )

  return {
    processA
  }
})

export interface ProcessService extends _A<typeof makeProcessService> {}
export const ProcessService = tag<ProcessService>()
export const LiveProcessService = L.fromManaged(ProcessService)(makeProcessService)

describe("Persistent", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](LiveProcessService)))
  )

  it("persistent", () =>
    T.gen(function* (_) {
      const { processA } = yield* _(ProcessService)

      expect(yield* _(processA.ask(new Get()))).equals(0)

      yield* _(processA.ask(new Increase()))

      expect(yield* _(processA.ask(new Get()))).equals(1)

      expect((yield* _(PG.query("SELECT * FROM state_journal"))).rows).toEqual([
        {
          persistence_id: "EffectTsActorsDemo(/process-a/leader)",
          shard: 3,
          state: { current: 1 }
        }
      ])
    }))
})
