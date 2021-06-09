import { ActorSystemTag, LiveActorSystem } from "@effect-ts/actors/ActorSystem"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
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
import { LivePersistence } from "../src/Persistence"
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

const userHandler = transactional(
  Message,
  S.union({ Initial, User }),
  O.some(S.string)
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
          shard: 7,
          event_sequence: 2
        },
        {
          persistence_id: "EffectTsActorsDemo(/users/mike-2)",
          state: { current: { _tag: "Initial" } },
          shard: 8,
          event_sequence: 0
        }
      ])
      expect((yield* _(PG.query("SELECT * FROM event_journal"))).rows).toEqual([
        {
          persistence_id: "EffectTsActorsDemo(/users/mike)",
          shard: 7,
          sequence: 1,
          event: { event: "create-user" }
        },
        {
          persistence_id: "EffectTsActorsDemo(/users/mike)",
          shard: 7,
          sequence: 2,
          event: { event: "setup-user" }
        }
      ])
    }))
})
