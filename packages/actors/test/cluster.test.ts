import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as O from "@effect-ts/core/Option"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import { pipe } from "@effect-ts/system/Function"

import { LiveActorSystem } from "../src/ActorSystem"
import { Cluster, HostPort, LiveCluster, StaticClusterConfig } from "../src/Cluster"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = LiveActorSystem("@effect-ts/actors/cluster/demo")[">+>"](
  Z.LiveKeeperClient["<<<"](TestKeeperConfig)[">+>"](
    LiveCluster["<<<"](
      StaticClusterConfig({
        host: "127.0.0.1",
        port: 34322
      })
    )
  )
)

describe("Cluster", () => {
  const { it } = pipe(J.runtime((TestEnv) => TestEnv[">+>"](AppLayer)))

  it("membership", () =>
    T.gen(function* (_) {
      const cluster = yield* _(Cluster)

      expect(yield* _(cluster.members)).equals(
        Chunk.single(new HostPort({ host: "127.0.0.1", port: 34322 }))
      )

      expect(yield* _(cluster.leader)).equals(
        O.some(new HostPort({ host: "127.0.0.1", port: 34322 }))
      )
    }))

  it("second", () =>
    T.gen(function* (_) {
      const cluster = yield* _(Cluster)

      expect(yield* _(cluster.members)).equals(
        Chunk.from([
          new HostPort({ host: "127.0.0.1", port: 34322 }),
          new HostPort({ host: "127.0.0.2", port: 34322 })
        ])
      )

      expect(yield* _(cluster.leader)).equals(
        O.some(new HostPort({ host: "127.0.0.1", port: 34322 }))
      )
    })["|>"](
      L.fresh(
        LiveCluster["<<<"](
          StaticClusterConfig({
            host: "127.0.0.2",
            port: 34322
          })
        )
      ).use
    ))
})
