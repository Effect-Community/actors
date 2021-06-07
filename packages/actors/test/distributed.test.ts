import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import type { _A } from "@effect-ts/core/Utils"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { tag } from "@effect-ts/system/Has"
import { matchTag, matchTag_ } from "@effect-ts/system/Utils"

import { ActorSystemTag, LiveActorSystem } from "../src/ActorSystem"
import * as Cluster from "../src/Cluster"
import * as D from "../src/Distributed"
import * as AM from "../src/Message"
import { RemotingExpress, StaticRemotingExpressConfig } from "../src/Remote"
import * as SUP from "../src/Supervisor"
import { LiveStateStorageAdapter, transactional } from "../src/Transactional"
import { TestPG } from "./pg"
import { TestKeeperConfig } from "./zookeeper"

const Remoting = RemotingExpress["<<<"](
  StaticRemotingExpressConfig({ host: "127.0.0.1", port: 34322 })
)

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">+>"](Remoting)
  [">+>"](Cluster.LiveCluster)
  ["<+<"](Z.LiveKeeperClient["<<<"](TestKeeperConfig))
  ["<+<"](LiveStateStorageAdapter["<+<"](PG.LivePG["<<<"](TestPG)))

class User extends S.Model<User>()(
  S.props({ _tag: S.prop(S.literal("User")), id: S.prop(S.string) })
) {}

class UserNotFound extends S.Model<UserNotFound>()(
  S.props({ _tag: S.prop(S.literal("UserNotFound")) })
) {}

class Get extends AM.Message(
  "Get",
  S.props({ id: S.prop(S.string) }),
  S.union({ User, UserNotFound })
) {}

class Create extends AM.Message(
  "Create",
  S.props({ id: S.prop(S.string) }),
  S.union({ User })
) {}

const Message = AM.messages(Get, Create)
type Message = AM.TypeOf<typeof Message>

class Initial extends S.Model<Initial>()(
  S.props({ _tag: S.prop(S.literal("Initial")) })
) {}

const usersHandler = D.distributed(
  transactional(
    Message,
    S.union({ Initial, User })
  )((state) =>
    matchTag({
      Get: (_) => {
        return _.return(
          state,
          matchTag_(state, { Initial: () => new UserNotFound({}), User: (_) => _ })
        )
      },
      Create: (_) => {
        const user = new User({ id: _.payload.id })
        return _.return(user, user)
      }
    })
  ),
  ({ id }) => id,
  { passivateAfter: 1_000 }
)

export const makeUsersService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const users = yield* _(system.make("users", SUP.none, usersHandler, new Initial({})))

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
          shard: 7
        },
        {
          persistence_id: "EffectTsActorsDemo(/users/mike-2)",
          state: { current: { _tag: "Initial" } },
          shard: 8
        }
      ])
    }))
})
