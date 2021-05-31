import * as T from "@effect-ts/core/Effect"
import * as F from "@effect-ts/core/Effect/Fiber"
import * as O from "@effect-ts/core/Option"
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

      const collectFiber = yield* _(pipe(client.waitDelete("/demo/ok"), T.fork))

      expect(yield* _(client.mkdir("/demo-2", { data: Buffer.from("yeah") }))).toEqual(
        "/demo-2"
      )

      expect(
        yield* _(client.create("/demo-2/ok", { data: Buffer.from("yeah yeah") }))
      ).toEqual("/demo-2/ok")

      expect(
        O.map_(yield* _(client.getData("/demo")), (_) => _.toString("utf8"))
      ).toEqual(O.none)

      expect(
        O.map_(yield* _(client.getData("/demo-2/ok")), (_) => _.toString("utf8"))
      ).toEqual(O.some("yeah yeah"))

      yield* _(client.remove("/demo/ok"))
      yield* _(client.remove("/demo"))

      expect(yield* _(F.join(collectFiber))).toEqual("/demo/ok")
    }))
})
