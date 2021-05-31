import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as O from "@effect-ts/core/Option"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import { pipe } from "@effect-ts/system/Function"

import { Cluster, DefaultCluster, HostPort } from "../src/Cluster"
import { TestKeeperConfig } from "./zookeeper"

const Keeper = TestKeeperConfig[">>>"](Z.LiveKeeperClient)

const AppLayer = Keeper[">+>"](
  DefaultCluster({
    sysName: "@effect-ts/actors/cluster/demo",
    host: "127.0.0.1",
    port: 34322
  })
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
})
