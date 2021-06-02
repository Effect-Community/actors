import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { makePGConfig, PGConfig } from "@effect-ts/pg"
import { GenericContainer } from "testcontainers"

export const makeTestPG = M.gen(function* (_) {
  const container = yield* _(
    pipe(
      T.promise(() =>
        new GenericContainer("postgres:alpine")
          .withEnv("POSTGRES_USER", "user")
          .withEnv("POSTGRES_PASSWORD", "pass")
          .withEnv("POSTGRES_DB", "db")
          .withExposedPorts(5432)
          .start()
      ),
      M.make((c) => T.promise(() => c.stop()))
    )
  )

  return makePGConfig({
    host: container.getHost(),
    port: container.getMappedPort(5432),
    user: "user",
    password: "pass",
    database: "db"
  })
})

export const TestPG = L.fromManaged(PGConfig)(makeTestPG)
