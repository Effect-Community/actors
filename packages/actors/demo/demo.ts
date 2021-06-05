import * as T from "@effect-ts/core/Effect"
import { pretty } from "@effect-ts/core/Effect/Cause"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import type { _A } from "@effect-ts/core/Utils"
import * as Z from "@effect-ts/keeper"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { tag } from "@effect-ts/system/Has"
import { matchTag, matchTag_ } from "@effect-ts/system/Utils"

import { ActorSystemTag } from "../src/ActorSystem"
import * as Cluster from "../src/Cluster"
import * as ClusterConfig from "../src/ClusterConfig"
import * as D from "../src/Distributed"
import * as AM from "../src/Message"
import * as SUP from "../src/Supervisor"
import { LiveStateStorageAdapter, transactional } from "../src/Transactional"
import { TestPG as TestPGConfig } from "../test/pg"
import { TestKeeperConfig } from "../test/zookeeper"

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

const program = T.gen(function* (_) {
  const { users } = yield* _(UsersService)
  console.log(yield* _(users.ask(new Get({ id: "mike" }))))
  console.log(yield* _(users.ask(new Create({ id: "mike" }))))
  console.log(yield* _(users.ask(new Get({ id: "mike" }))))
  console.log(yield* _(users.ask(new Get({ id: "mike-2" }))))
  console.log((yield* _(PG.query("SELECT * FROM state_journal"))).rows)

  yield* _(T.sleep(2_000))
})

pipe(
  program,
  T.provideSomeLayer(AppLayer[">+>"](LiveUsersService)),
  T.runPromiseExit
).then((ex) => {
  if (ex._tag === "Failure") {
    console.error(pretty(ex.cause))
  }
})
