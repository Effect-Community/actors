import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import { pipe } from "@effect-ts/core/Function"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as Cluster from "../src/Cluster"
import * as ClusterConfigSym from "../src/ClusterConfig"
import * as AM from "../src/Message"
import { LiveStateStorageAdapter, transactional } from "../src/Persistent"
import { TestPG } from "./pg"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = L.all(
  Z.LiveKeeperClient["<<<"](TestKeeperConfig),
  TestPG[">>>"](PG.LivePG)[">+>"](LiveStateStorageAdapter)
)[">+>"](
  Cluster.LiveCluster["<<<"](
    ClusterConfigSym.StaticClusterConfig({
      sysName: "EffectTsActorsDemo",
      host: "127.0.0.1",
      port: 34322
    })
  )
)

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

const ProcessA = Cluster.makeSingleton("process-a")(
  statefulHandler,
  T.succeed(0),
  () => T.never
)

describe("Persistent", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](ProcessA.Live)))
  )

  it("persistent", () =>
    T.gen(function* (_) {
      expect(yield* _(ProcessA.ask(new Get()))).equals(0)

      yield* _(ProcessA.ask(new Increase()))

      expect(yield* _(ProcessA.ask(new Get()))).equals(1)

      expect((yield* _(PG.query("SELECT * FROM state_journal"))).rows).toEqual([
        {
          actor_name: "EffectTsActorsDemo(/singleton/leader/process-a)",
          state: '{"state":1}'
        }
      ])
    }))
})
