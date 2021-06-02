import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import { pipe } from "@effect-ts/core/Function"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as Cluster from "../src/Cluster"
import * as ClusterConfigSym from "../src/ClusterConfig"
import * as AM from "../src/Message"
import { LiveStateStorageAdapter, transactional } from "../src/Persistent"
import * as Sharded from "../src/Sharded"
import { TestPG } from "./pg"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = L.all(
  Z.LiveKeeperClient["<<<"](TestKeeperConfig),
  TestPG[">>>"](PG.LivePG)[">+>"](LiveStateStorageAdapter)
)[">+>"](
  Cluster.LiveCluster["<<<"](
    ClusterConfigSym.StaticClusterConfig({
      sysName: "EffectTsActorsDemo",
      host: "127.0.0.1",
      port: 34322
    })
  )
)

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

const userHandler = transactional(
  Message,
  S.union({ Initial, User })
)((state) =>
  matchTag({
    Get: (_) => {
      return _.return(state, state._tag === "Initial" ? new UserNotFound({}) : state)
    },
    Create: (_) => {
      const user = new User({ id: _.payload.id })
      return _.return(user, user)
    }
  })
)

const Users = Sharded.makeSharded(
  "users",
  userHandler,
  new Initial({}),
  (_) => `users-${_.id}`
)

describe("Persistent", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](Users.Live)))
  )

  it("persistent", () =>
    T.gen(function* (_) {
      const users = yield* _(Users.actor)
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
          actor_name: "EffectTsActorsDemo(/sharded/leader/users-mike)",
          state: '{"state":{"_tag":"User","id":"mike"}}'
        },
        {
          actor_name: "EffectTsActorsDemo(/sharded/leader/users-mike-2)",
          state: '{"state":{"_tag":"Initial"}}'
        }
      ])
    }))
})
