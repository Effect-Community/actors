import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as PG from "@effect-ts/pg"
import { identity } from "@effect-ts/system/Function"

import { computeShardForId, ShardContext } from "../Shards"

export interface Persistence {
  readonly transaction: (
    persistenceId: string
  ) => <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R & Has<ShardContext>, E, A>

  readonly get: (persistenceId: string) => T.Effect<
    Has<ShardContext>,
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
  ) => T.Effect<Has<ShardContext>, never, void>

  readonly emit: (
    persistenceId: string,
    events: Chunk.Chunk<{ value: unknown; eventSequence: number }>
  ) => T.Effect<Has<ShardContext>, never, void>

  readonly events: (
    persistenceId: string,
    from: number,
    size: number
  ) => T.Effect<
    Has<ShardContext>,
    never,
    Chunk.Chunk<{
      persistenceId: string
      shard: number
      shardSequence: number
      event: unknown
      eventSequence: number
    }>
  >

  readonly setup: T.Effect<Has<ShardContext>, never, void>
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
        shard_sequence  integer,
        shard           integer,
        event_sequence  integer,
        event           jsonb,
        PRIMARY KEY(persistence_id, event_sequence)
      );`)
    )

    yield* _(
      cli.query(`
      CREATE TABLE IF NOT EXISTS "shard_journal" (
        domain          text,
        shard           integer,
        sequence        integer,
        PRIMARY KEY(domain, shard)
      );`)
    )

    const setup: T.Effect<Has<ShardContext>, never, void> = pipe(
      T.accessServiceM(ShardContext)((_) =>
        T.forEach_(Chunk.range(1, _.shards), (i) =>
          cli.query(
            `INSERT INTO "shard_journal" ("domain", "shard", "sequence") VALUES($1::text, $2::integer, 0) ON CONFLICT ("domain","shard") DO NOTHING`,
            [_.domain, i]
          )
        )
      ),
      T.asUnit
    )

    const transaction: (
      persistenceId: string
    ) => <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R & Has<ShardContext>, E, A> =
      () => cli.transaction

    const get: (persistenceId: string) => T.Effect<
      Has<ShardContext>,
      never,
      O.Option<{
        persistenceId: string
        shard: number
        state: unknown
        event_sequence: number
      }>
    > = (persistenceId) =>
      pipe(
        computeShardForId(persistenceId),
        T.chain((shard) =>
          cli.query(
            `SELECT * FROM "state_journal" WHERE "persistence_id" = '${persistenceId}' AND "shard" = $1::integer`,
            [shard]
          )
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
      Has<ShardContext>,
      never,
      Chunk.Chunk<{
        persistenceId: string
        shard: number
        shardSequence: number
        event: unknown
        eventSequence: number
      }>
    > = (persistenceId, from, size) =>
      pipe(
        computeShardForId(persistenceId),
        T.chain((shard) =>
          cli.query(
            `SELECT * FROM "event_journal" WHERE "persistence_id" = '${persistenceId}' AND "event_sequence" > $1::integer ANS "shard" = $3::integer ORDER BY "event_sequence" ASC LIMIT $2::integer`,
            [from, size, shard]
          )
        ),
        T.map((res) =>
          pipe(
            Chunk.from(res.rows),
            Chunk.map((row) => ({
              persistenceId: row.actor_name,
              shard: row.shard,
              shardSequence: row.shard_sequence,
              event: row["event"],
              eventSequence: row.event_sequence
            }))
          )
        )
      )

    const set: (
      persistenceId: string,
      value: unknown,
      event_sequence: number
    ) => T.Effect<Has<ShardContext>, never, void> = (
      persistenceId,
      value,
      event_sequence
    ) =>
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

    const sequenceForShard = (persistenceId: string) =>
      pipe(
        computeShardForId(persistenceId),
        T.chain((shard) =>
          T.accessServiceM(ShardContext)((_) =>
            cli.query(
              `SELECT * FROM "shard_journal" WHERE "domain" = $1::text AND "shard" = $2::integer FOR UPDATE`,
              [_.domain, shard]
            )
          )
        ),
        T.map((r) => r.rows[0]["sequence"] as number)
      )

    const emit: (
      persistenceId: string,
      events: Chunk.Chunk<{ value: unknown; eventSequence: number }>
    ) => T.Effect<Has<ShardContext>, never, void> = (persistenceId, events) =>
      events.length > 0
        ? T.accessServiceM(ShardContext)(({ domain }) =>
            pipe(
              T.zip_(computeShardForId(persistenceId), sequenceForShard(persistenceId)),
              T.chain(({ tuple: [shard, shard_sequence] }) =>
                pipe(
                  T.forEach_(
                    Chunk.zipWithIndexOffset_(events, shard_sequence + 1),
                    ({ tuple: [{ eventSequence, value }, shard_sequence_i] }) =>
                      cli.query(
                        `INSERT INTO "event_journal" ("persistence_id", "shard", "event", "event_sequence", "shard_sequence") VALUES('${persistenceId}', $2::integer, $1::jsonb, $3::integer, $4::integer)`,
                        [JSON.stringify(value), shard, eventSequence, shard_sequence_i]
                      )
                  ),
                  T.chain(() =>
                    cli.query(
                      `UPDATE "shard_journal" SET "sequence" = $3::integer WHERE "domain" = $1::text AND "shard" = $2::integer`,
                      [domain, shard, shard_sequence + events.length]
                    )
                  )
                )
              ),
              T.asUnit
            )
          )
        : T.unit

    return identity<Persistence>({
      transaction,
      get,
      set,
      emit,
      events,
      setup
    })
  })
)
