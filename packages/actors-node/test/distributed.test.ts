import { ActorSystemTag, LiveActorSystem } from "@effect-ts/actors/ActorSystem"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as ST from "@effect-ts/core/Effect/Experimental/Stream"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import type { _A } from "@effect-ts/core/Utils"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { tag } from "@effect-ts/system/Has"
import { matchTag_ } from "@effect-ts/system/Utils"

import * as Cluster from "../src/Cluster"
import * as D from "../src/Distributed"
import { Event, LivePersistence, Offset, Persistence } from "../src/Persistence"
import { RemotingExpress, StaticRemotingExpressConfig } from "../src/Remote"
import { transactional } from "../src/Transactional"
import { TestPG } from "./pg"
import { TestKeeperConfig } from "./zookeeper"

const Remoting = RemotingExpress["<<<"](
  StaticRemotingExpressConfig({ host: "127.0.0.1", port: 34322 })
)

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">+>"](Remoting)
  [">+>"](Cluster.LiveCluster)
  ["<+<"](Z.LiveKeeperClient["<<<"](TestKeeperConfig))
  ["<+<"](LivePersistence["<+<"](PG.LivePG["<<<"](TestPG)))

@S.stable
class User extends S.Model<User>()(
  S.props({ _tag: S.prop(S.literal("User")), id: S.prop(S.string) })
) {}

@S.stable
class UserNotFound extends S.Model<UserNotFound>()(
  S.props({ _tag: S.prop(S.literal("UserNotFound")) })
) {}

@S.stable
class UserAlreadyCreated extends S.Model<UserAlreadyCreated>()(
  S.props({ _tag: S.prop(S.literal("UserAlreadyCreated")) })
) {}

class Get extends AM.Message(
  "Get",
  S.props({ id: S.prop(S.string) }),
  S.union({ User, UserNotFound })
) {}

class Create extends AM.Message(
  "Create",
  S.props({ id: S.prop(S.string) }),
  S.union({ User, UserAlreadyCreated })
) {}

const Message = AM.messages(Get, Create)
type Message = AM.TypeOf<typeof Message>

@S.stable
class Initial extends S.Model<Initial>()(
  S.props({ _tag: S.prop(S.literal("Initial")) })
) {}

const UserEvents = S.string

const userHandler = transactional(
  Message,
  S.union({ Initial, User }),
  O.some(UserEvents)
)(({ event, state }) => ({
  Create: ({ id }) =>
    T.gen(function* (_) {
      if ((yield* _(state.get))._tag !== "Initial") {
        return yield* _(T.succeed(new UserAlreadyCreated({})))
      }
      yield* _(event.emit("create-user"))
      yield* _(event.emit("setup-user"))
      const user = new User({ id })
      yield* _(state.set(user))
      return yield* _(T.succeed(user))
    }),
  Get: () =>
    T.gen(function* (_) {
      const maybeUser = matchTag_(yield* _(state.get), {
        Initial: () => new UserNotFound({}),
        User: (_) => _
      })

      return yield* _(T.succeed(maybeUser))
    })
}))

export const makeUsersService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const users = yield* _(
    system.make(
      "users",
      SUP.none,
      D.distributed(userHandler, ({ id }) => id, {
        passivateAfter: 1_000
      }),
      () => new Initial({})
    )
  )

  return {
    users
  }
})

export interface UsersService extends _A<typeof makeUsersService> {}
export const UsersService = tag<UsersService>()
export const LiveUsersService = L.fromManaged(UsersService)(makeUsersService)

describe("Distributed", () => {
  const { it } = J.runtime((Env) => Env[">+>"](AppLayer)[">+>"](LiveUsersService))

  it("distributed", () =>
    T.gen(function* (_) {
      const { users } = yield* _(UsersService)
      expect(yield* _(users.ask(new Get({ id: "mike" })))).equals(new UserNotFound({}))
      expect(yield* _(users.ask(new Create({ id: "mike" })))).equals(
        new User({ id: "mike" })
      )
      expect(yield* _(users.ask(new Get({ id: "mike" })))).equals(
        new User({ id: "mike" })
      )
      expect(yield* _(users.ask(new Get({ id: "mike-2" })))).equals(
        new UserNotFound({})
      )
      expect((yield* _(PG.query("SELECT * FROM state_journal"))).rows).toEqual([
        {
          persistence_id: "EffectTsActorsDemo(/users/mike)",
          state: { current: { _tag: "User", id: "mike" } },
          shard: 3,
          event_sequence: 2
        },
        {
          persistence_id: "EffectTsActorsDemo(/users/mike-2)",
          state: { current: { _tag: "Initial" } },
          shard: 14,
          event_sequence: 0
        }
      ])
      expect((yield* _(PG.query("SELECT * FROM event_journal"))).rows).toEqual([
        {
          persistence_id: "EffectTsActorsDemo(/users/mike)",
          domain: "EffectTsActorsDemo(users)",
          shard: 3,
          shard_sequence: 1,
          event: { event: "create-user" },
          event_sequence: 1
        },
        {
          persistence_id: "EffectTsActorsDemo(/users/mike)",
          domain: "EffectTsActorsDemo(users)",
          shard_sequence: 2,
          shard: 3,
          event: { event: "setup-user" },
          event_sequence: 2
        }
      ])
      expect(
        (yield* _(
          PG.query("SELECT * FROM shard_journal ORDER BY domain ASC, shard ASC")
        )).rows
      ).toEqual([
        { domain: "EffectTsActorsDemo(users)", shard: 1, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 2, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 3, sequence: 2 },
        { domain: "EffectTsActorsDemo(users)", shard: 4, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 5, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 6, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 7, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 8, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 9, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 10, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 11, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 12, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 13, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 14, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 15, sequence: 0 },
        { domain: "EffectTsActorsDemo(users)", shard: 16, sequence: 0 }
      ])
      expect(
        (yield* _(PG.query("SELECT * FROM domain_journal ORDER BY domain ASC"))).rows
      ).toEqual([{ domain: "EffectTsActorsDemo(users)", shards: 16 }])

      const { eventStream } = yield* _(Persistence)

      expect(
        yield* _(
          pipe(
            eventStream(UserEvents)(
              Chunk.many(
                new Offset({
                  domain: "EffectTsActorsDemo(users)",
                  shard: 3,
                  sequence: 0
                })
              )
            ),
            ST.take(2),
            ST.runCollect
          )
        )
      ).equals(
        Chunk.many(
          new Event({
            event: "create-user",
            offset: new Offset({
              domain: "EffectTsActorsDemo(users)",
              shard: 3,
              sequence: 1
            })
          }),
          new Event({
            event: "setup-user",
            offset: new Offset({
              domain: "EffectTsActorsDemo(users)",
              shard: 3,
              sequence: 2
            })
          })
        )
      )
    }))
})
