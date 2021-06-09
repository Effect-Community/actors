import { Chunk, pipe } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as PG from "@effect-ts/pg"
import { identity } from "@effect-ts/system/Function"

import { computeShardForId } from "../Shards"

export interface Persistence {
  readonly transaction: (
    persistenceId: string
  ) => <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R, E, A>

  readonly get: (persistenceId: string) => T.Effect<
    unknown,
    never,
    O.Option<{
      persistenceId: string
      shard: number
      state: unknown
      event_sequence: number
    }>
  >

  readonly set: (
    persistenceId: string,
    value: unknown,
    event_sequence: number
  ) => T.Effect<unknown, never, void>

  readonly emit: (
    persistenceId: string,
    value: unknown,
    event_sequence: number
  ) => T.Effect<unknown, never, void>

  readonly events: (
    persistenceId: string,
    shard: number,
    from: number,
    size: number
  ) => T.Effect<
    unknown,
    never,
    Chunk.Chunk<{
      persistenceId: string
      shard: number
      event: unknown
      sequence: number
    }>
  >
}

export const Persistence = tag<Persistence>()

export const LivePersistence = L.fromManaged(Persistence)(
  M.gen(function* (_) {
    const cli = yield* _(PG.PG)

    yield* _(
      cli.query(`
      CREATE TABLE IF NOT EXISTS "state_journal" (
        persistence_id  text PRIMARY KEY,
        shard           integer,
        event_sequence  integer,
        state           jsonb
      );`)
    )

    yield* _(
      cli.query(`
      CREATE TABLE IF NOT EXISTS "event_journal" (
        persistence_id  text,
        shard           integer,
        sequence        integer,
        event           jsonb,
        PRIMARY KEY(persistence_id, sequence)
      );`)
    )

    const transaction: (
      persistenceId: string
    ) => <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R, E, A> = () =>
      cli.transaction

    const get: (persistenceId: string) => T.Effect<
      unknown,
      never,
      O.Option<{
        persistenceId: string
        shard: number
        state: unknown
        event_sequence: number
      }>
    > = (persistenceId) =>
      pipe(
        cli.query(
          `SELECT * FROM "state_journal" WHERE "persistence_id" = '${persistenceId}'`
        ),
        T.map((res) =>
          pipe(
            O.fromNullable(res.rows?.[0]),
            O.map((row) => ({
              persistenceId: row.actor_name,
              shard: row.shard,
              state: row["state"],
              event_sequence: row.event_sequence
            }))
          )
        )
      )

    const events: (
      persistenceId: string,
      from: number,
      size: number
    ) => T.Effect<
      unknown,
      never,
      Chunk.Chunk<{
        persistenceId: string
        shard: number
        event: unknown
        sequence: number
      }>
    > = (persistenceId, from, size) =>
      pipe(
        cli.query(
          `SELECT * FROM "event_journal" WHERE "persistence_id" = '${persistenceId}' AND "sequence" > $1::integer ORDER BY "sequence" ASC LIMIT $2::integer`,
          [from, size]
        ),
        T.map((res) =>
          pipe(
            Chunk.from(res.rows),
            Chunk.map((row) => ({
              persistenceId: row.actor_name,
              shard: row.shard,
              event: row["event"],
              sequence: row.sequence
            }))
          )
        )
      )

    const set: (
      persistenceId: string,
      value: unknown,
      event_sequence: number
    ) => T.Effect<unknown, never, void> = (persistenceId, value, event_sequence) =>
      pipe(
        computeShardForId(persistenceId),
        T.chain((shard) =>
          cli.query(
            `INSERT INTO "state_journal" ("persistence_id", "shard", "state", "event_sequence") VALUES('${persistenceId}', $2::integer, $1::jsonb, $3::integer) ON CONFLICT ("persistence_id") DO UPDATE SET "state" = $1::jsonb, "event_sequence" = $3::integer`,
            [JSON.stringify(value), shard, event_sequence]
          )
        ),
        T.asUnit
      )

    const emit: (
      persistenceId: string,
      value: unknown,
      event_sequence: number
    ) => T.Effect<unknown, never, void> = (persistenceId, value, event_sequence) =>
      pipe(
        computeShardForId(persistenceId),
        T.chain((shard) =>
          cli.query(
            `INSERT INTO "event_journal" ("persistence_id", "shard", "event", "sequence") VALUES('${persistenceId}', $2::integer, $1::jsonb, $3::integer)`,
            [JSON.stringify(value), shard, event_sequence]
          )
        ),
        T.asUnit
      )

    return identity<Persistence>({
      transaction,
      get,
      set,
      emit,
      events
    })
  })
)
