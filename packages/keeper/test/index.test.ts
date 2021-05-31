import * as T from "@effect-ts/core/Effect"
import * as J from "@effect-ts/jest/Test"
import { pipe } from "@effect-ts/system/Function"

import * as Z from "../src"
import { TestKeeperConfig } from "./zookeeper"

describe("Zookeeper", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](TestKeeperConfig[">>>"](Z.LiveKeeperClient)))
  )

  it("dummy", () =>
    T.gen(function* (_) {
      const client = yield* _(Z.KeeperClient)

      expect(yield* _(client.mkdir("/demo"))).toEqual("/demo")
      expect(yield* _(client.create("/demo/ok"))).toEqual("/demo/ok")

      expect(yield* _(client.mkdirWithData("/demo-2", Buffer.from("yeah")))).toEqual(
        "/demo-2"
      )
      expect(
        yield* _(client.createWithData("/demo-2/ok", Buffer.from("yeah yeah")))
      ).toEqual("/demo-2/ok")
    }))
})
