import * as T from "@effect-ts/core/Effect"
import * as J from "@effect-ts/jest/Test"
import * as PG from "@effect-ts/pg"

import * as PGM from "../src"
import { TestPG } from "./pg"

export const createFilms = PGM.migration(
  PG.query(`
  CREATE TABLE films (
    id SERIAL PRIMARY KEY
  );`),
  PG.query(`
  DROP TABLE films;
  `)
)

export const createUsers = PGM.migration(
  PG.query(`
  CREATE TABLE users (
    id SERIAL PRIMARY KEY
  );`),
  PG.query(`
  DROP TABLE users;
  `)
)

export const db = PGM.migrations(createFilms, createUsers)

export const DbLive = PG.LivePG[">>>"](PGM.migrate(db, "my-db"))
export const DbRoll = PG.LivePG[">>>"](PGM.migrate(db, "my-db", "1"))

describe("PG", () => {
  const DbConfig = J.shared(TestPG)

  const { it } = J.runtime((TestEnv) => TestEnv[">+>"](DbConfig[">+>"](DbLive)))

  it("version 2", () =>
    T.gen(function* (_) {
      const { query } = yield* _(PG.PG)

      expect(yield* _(PGM.currentVersion("my-db"))).toEqual(2)

      const { rows: tables } = yield* _(
        query(
          `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;`
        )
      )

      expect(tables).toEqual([
        { table_name: "films" },
        { table_name: "migrations" },
        { table_name: "users" }
      ])
    }))

  it("version 1", () =>
    T.gen(function* (_) {
      const { query } = yield* _(PG.PG)

      expect(yield* _(PGM.currentVersion("my-db"))).toEqual(1)

      const { rows: tables } = yield* _(
        query(
          `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;`
        )
      )

      expect(tables).toEqual([{ table_name: "films" }, { table_name: "migrations" }])
    })["|>"](DbRoll.use))
})
