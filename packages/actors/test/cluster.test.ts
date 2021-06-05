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
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "../src/Actor"
import { ActorSystemTag, LiveActorSystem } from "../src/ActorSystem"
import * as Cluster from "../src/Cluster"
import * as AM from "../src/Message"
import { RemoteExpress } from "../src/Remote"
import * as SUP from "../src/Supervisor"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">>>"](RemoteExpress("127.0.0.1", 34322)[">+>"](Cluster.LiveCluster))
  ["<+<"](Z.LiveKeeperClient["<<<"](TestKeeperConfig))

const unit = S.unknown["|>"](S.brand<void>())

class Reset extends AM.Message("Reset", S.props({}), unit) {}
class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}
class GetAndReset extends AM.Message("GetAndReset", S.props({}), S.number) {}

const Message = AM.messages(Reset, Increase, Get, GetAndReset)
type Message = AM.TypeOf<typeof Message>

const statefulHandler = AC.stateful(
  Message,
  S.number
)((state, ctx) =>
  matchTag({
    Reset: (_) => _.return(0),
    Increase: (_) => _.return(state + 1),
    Get: (_) => _.return(state, state),
    GetAndReset: (_) =>
      pipe(
        ctx.self,
        T.chain((self) => self.tell(new Reset())),
        T.zipRight(_.return(state, state))
      )
  })
)

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

      const path = yield* _(actor.path)

      expect(path).equals("zio://EffectTsActorsDemo@127.0.0.1:34322/process-a")

      expect(yield* _(actor.ask(new Get()))).equals(0)

      yield* _(actor.ask(new Increase()))

      expect(yield* _(actor.ask(new Get()))).equals(1)

      const selected = yield* _(system.select(path))

      expect(yield* _(selected.ask(new Get()))).equals(1)
    }))
})
