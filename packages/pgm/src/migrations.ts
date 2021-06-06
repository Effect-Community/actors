import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import type { Has } from "@effect-ts/core/Has"
import type * as U from "@effect-ts/core/Utils"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"

export class Migration<R> extends Tagged("Migration")<{
  readonly up: T.Effect<R & Has<PG.PGClient> & Has<PG.PG>, never, void>
  readonly down: T.Effect<R & Has<PG.PGClient> & Has<PG.PG>, never, void>
}> {
  readonly [T._R]: (_: R) => void
}

export function migration<R0, R1>(
  up: T.Effect<R0 & Has<PG.PGClient> & Has<PG.PG>, never, void>,
  down: T.Effect<R1 & Has<PG.PGClient> & Has<PG.PG>, never, void>
) {
  return new Migration<R0 & R1>({ up, down })
}

export class Migrations<R, V extends string> {
  readonly _N!: () => V
  constructor(readonly migrations: Chunk.Chunk<Migration<R>>) {}
}

export function migrations<Ms extends readonly Migration<any>[]>(
  ...migrations: Ms
): Migrations<
  U._R<Ms[number]>,
  { [k in keyof [...Ms, {}]]: k extends `${number}` ? k : never }[keyof [...Ms, {}]]
> {
  return new Migrations(Chunk.from(migrations))
}

export const createMigrationsRegistry = migration(
  PG.query(`
    CREATE TABLE IF NOT EXISTS migrations (
      id         varchar(32) PRIMARY KEY,
      version    integer
    );
    `),
  PG.query(`
    DROP TABLE migrations;
    `)
)

export class VersionRow extends S.Model<VersionRow>("VersionRow")(
  S.props({
    id: S.prop(S.string),
    version: S.prop(S.number)
  })
) {
  static parse = VersionRow.Parser["|>"](S.condemnDie)
}

export const currentVersion = (id: string) =>
  T.gen(function* (_) {
    const pg = yield* _(PG.PG)

    const { rows } = yield* _(
      pg.query("SELECT * FROM migrations WHERE id = $1::text", [id])
    )

    if (rows.length === 0) {
      yield* _(
        pg.query("INSERT INTO migrations (id, version) VALUES($1::text, $2::integer)", [
          id,
          0
        ])
      )
    }

    const { version } =
      rows.length === 1 ? yield* _(VersionRow.parse(rows[0])) : { version: 0 }

    return version
  })

export function migrate<R, VS extends string, V extends VS>(
  db: Migrations<R, VS>,
  id: string,
  v?: V
): L.Layer<Has<PG.PG> & R, never, Has<PG.PG>> {
  return L.fromEffect(PG.PG)(
    T.gen(function* (_) {
      const pg = yield* _(PG.PG)

      yield* _(pg.transaction(createMigrationsRegistry.up))

      const version = yield* _(currentVersion(id))

      const target = v ? parseInt(v) : db.migrations.length

      if (target > version) {
        const up = Chunk.take(target - version)(Chunk.drop(version)(db.migrations))

        yield* _(
          PG.transaction(
            T.gen(function* (_) {
              yield* _(T.forEachUnit_(up, (_) => _.up))
              yield* _(
                pg.query(
                  "UPDATE migrations SET version = version + $1::integer WHERE id = $2::text",
                  [up.length, id]
                )
              )
            })
          )
        )
      }

      if (target < version) {
        const down = Chunk.take(version - target)(Chunk.drop(target)(db.migrations))

        yield* _(
          PG.transaction(
            T.gen(function* (_) {
              yield* _(T.forEachUnit_(down, (_) => _.down))
              yield* _(
                pg.query(
                  "UPDATE migrations SET version = version - $1::integer WHERE id = $2::text",
                  [down.length, id]
                )
              )
            })
          )
        )
      }

      return pg
    })
  ).setKey(Symbol())
}
