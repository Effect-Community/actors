import * as T from "@effect-ts/core/Effect"
import * as J from "@effect-ts/jest/Test"

describe("PG", () => {
  const { it } = J.runtime((TestEnv) => TestEnv)

  it("pass", () =>
    T.gen(function* (_) {
      yield* _(T.unit)
    }))
})
