import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as J from "@effect-ts/jest/Test"
import * as PG from "@effect-ts/pg"

import * as PGM from "../src"
import { TestPG } from "./pg"

const createUsersTable = PGM.createTable(
  "Users",
  {
    id: PGM.column({ type: PGM.Serial.of() }),
    email: PGM.column({ type: PGM.VarChar.of(32) })
  },
  "users"
)

const createCredentialsTable = PGM.createTable(
  "Credentials",
  {
    id: PGM.column({ type: PGM.Serial.of() }),
    userId: PGM.column({ type: PGM.Integer.of(), alias: "user_id" }),
    password: PGM.column({ type: PGM.VarChar.of(36) })
  },
  "credentials"
)

const createWorkInProgressTable = PGM.createTable(
  "WIP",
  {
    id: PGM.column({ type: PGM.Serial.of() }),
    userId: PGM.column({ type: PGM.Integer.of() }),
    password: PGM.column({ type: PGM.VarChar.of(36) })
  },
  "wip"
)

export const Db = PGM.db("AppDb")
  ["|>"](createUsersTable)
  ["|>"](createCredentialsTable)
  ["|>"](createWorkInProgressTable)
  ["|>"](PGM.useDbVersion("2"))

export const SQL = PGM.sql(Db)

export const DbLive = PG.LivePG[">>>"](PGM.migrationsFor(Db))

describe("MigrationDSL", () => {
  const DbConfig = J.shared(TestPG)

  const { it } = J.runtime((TestEnv) => TestEnv[">+>"](DbConfig[">+>"](DbLive)))

  it("deb", () =>
    T.gen(function* (_) {
      const { query } = yield* _(PG.PG)

      yield* _(query(`INSERT INTO "users" ("email") VALUES('bla')`))
      yield* _(query(`INSERT INTO "users" ("email") VALUES('bla2')`))
      yield* _(
        query(`INSERT INTO "credentials" ("user_id", "password") VALUES(1, 'bla')`)
      )
      yield* _(
        query(`INSERT INTO "credentials" ("user_id", "password") VALUES(2, 'bla')`)
      )

      expect(Chunk.toArray(yield* _(SQL.select("Users").run()))).toEqual([
        { id: 1, email: "bla" },
        { id: 2, email: "bla2" }
      ])

      expect(
        Chunk.toArray(
          yield* _(
            SQL.select("Users")
              .join("Credentials", "c")
              .tablePick("c", "c.password")
              .as("c.password", "password")
              .where((W) => W.eq("id", "c.id"))
              .run()
          )
        )
      ).toEqual([
        {
          id: 1,
          email: "bla",
          password: "bla"
        },
        {
          id: 2,
          email: "bla2",
          password: "bla"
        }
      ])

      expect(
        Chunk.toArray(
          yield* _(
            SQL.select("Users")
              .join("Credentials", "c")
              .tablePick("c", "c.password")
              .as("c.password", "password")
              .param("email", "Users.email")
              .where((W) => W.eq("id", "c.id")["&&"](W.eq("email", { param: "email" })))
              .run({ email: "bla" })
          )
        )
      ).toEqual([
        {
          id: 1,
          email: "bla",
          password: "bla"
        }
      ])
    }))

  it("tables", () =>
    T.gen(function* (_) {
      const { query } = yield* _(PG.PG)

      expect(yield* _(PGM.currentVersion("AppDb"))).toEqual(2)

      const { rows: tables } = yield* _(
        query(
          `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;`
        )
      )

      expect(tables).toEqual([
        { table_name: "credentials" },
        { table_name: "migrations" },
        { table_name: "users" }
      ])
    }))
})
