import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "../src/Actor"
import { LiveActorSystem } from "../src/ActorSystem"
import * as Cluster from "../src/Cluster"
import * as AM from "../src/Message"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = LiveActorSystem("@effect-ts/actors/cluster/demo")[">+>"](
  Z.LiveKeeperClient["<<<"](TestKeeperConfig)[">+>"](
    Cluster.LiveCluster["<<<"](
      Cluster.StaticClusterConfig({
        host: "127.0.0.1",
        port: 34322
      })
    )
  )
)

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

const ProcessA = Cluster.makeSingleton("process-a")(statefulHandler, T.succeed(0))

describe("Cluster", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](ProcessA.Live)))
  )

  it("membership", () =>
    T.gen(function* (_) {
      const cluster = yield* _(Cluster.Cluster)

      expect(yield* _(cluster.members)).equals(
        Chunk.single(new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }))
      )

      expect(yield* _(cluster.leader)).equals(
        O.some(new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }))
      )
    }))

  it("second", () =>
    T.gen(function* (_) {
      const cluster = yield* _(Cluster.Cluster)

      expect(yield* _(cluster.members)).equals(
        Chunk.from([
          new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }),
          new Cluster.HostPort({ host: "127.0.0.2", port: 34322 })
        ])
      )

      expect(yield* _(cluster.leader)).equals(
        O.some(new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }))
      )
    })["|>"](
      L.fresh(
        Cluster.LiveCluster["<<<"](
          Cluster.StaticClusterConfig({
            host: "127.0.0.2",
            port: 34322
          })
        )
      ).use
    ))

  it("dropped", () =>
    T.gen(function* (_) {
      const cluster = yield* _(Cluster.Cluster)

      expect(yield* _(cluster.members)).equals(
        Chunk.single(new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }))
      )

      expect(yield* _(cluster.leader)).equals(
        O.some(new Cluster.HostPort({ host: "127.0.0.1", port: 34322 }))
      )
    }))

  it("singleton", () =>
    T.gen(function* (_) {
      expect(yield* _(ProcessA.ask(new Get()))).equals(0)

      yield* _(ProcessA.ask(new Increase()))

      expect(yield* _(ProcessA.ask(new Get()))).equals(1)
    }))
})
