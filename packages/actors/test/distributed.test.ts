import * as HM from "@effect-ts/core/Collections/Immutable/HashMap"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as Ref from "@effect-ts/core/Effect/Ref"
import { pipe } from "@effect-ts/core/Function"
import * as J from "@effect-ts/jest/Test"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { matchTag, matchTag_ } from "@effect-ts/system/Utils"

import * as Cluster from "../src/Cluster"
import * as ClusterConfig from "../src/ClusterConfig"
import * as D from "../src/Distributed"
import * as AM from "../src/Message"
import { LiveStateStorageAdapter, transactional } from "../src/Transactional"
import { TestPG as TestPGConfig } from "./pg"
import { TestKeeperConfig } from "./zookeeper"

const AppLayer = Cluster.LiveCluster["<+<"](
  L.all(
    Z.LiveKeeperClient["<<<"](TestKeeperConfig),
    LiveStateStorageAdapter["<+<"](PG.LivePG["<<<"](TestPGConfig)),
    ClusterConfig.StaticClusterConfig({
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

const Users = D.makeDistributed(
  "users",
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
  new Initial({}),
  ({ message: { id }, name }) => `${name}-${id}`
)

describe("Distributed", () => {
  const { it } = pipe(
    J.runtime((TestEnv) => TestEnv[">+>"](AppLayer[">+>"](Users.Live)))
  )

  it("distributed", () =>
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
          actor_name: "EffectTsActorsDemo(/distributed/leader/users-mike)",
          state: '{"state":{"_tag":"User","id":"mike"}}'
        },
        {
          actor_name: "EffectTsActorsDemo(/distributed/leader/users-mike-2)",
          state: '{"state":{"_tag":"Initial"}}'
        }
      ])
      const { runningMapRef } = yield* _(Users.Tag)
      expect(HM.size(yield* _(Ref.get(runningMapRef)))).toEqual(2)
      yield* _(J.adjust(120_000))
      expect(HM.size(yield* _(Ref.get(runningMapRef)))).toEqual(0)
      expect(yield* _(users.ask(new Get({ id: "mike" })))).equals(
        new User({ id: "mike" })
      )
      expect(HM.size(yield* _(Ref.get(runningMapRef)))).toEqual(1)
    }))
})
