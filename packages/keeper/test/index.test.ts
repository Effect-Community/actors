import * as T from "@effect-ts/core/Effect"
import * as J from "@effect-ts/jest/Test"
import { pipe } from "@effect-ts/system/Function"

import { TestKeeperConfig } from "./zookeeper"

describe("Zookeeper", () => {
  const { it } = pipe(J.runtime((TestEnv) => TestEnv[">+>"](TestKeeperConfig)))

  it("dummy", () =>
    T.gen(function* (_) {
      yield* _(T.unit)
    }))
})
