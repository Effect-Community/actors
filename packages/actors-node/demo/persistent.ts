/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { ActorSystemTag, LiveActorSystem } from "@effect-ts/actors/ActorSystem"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import type { _A } from "@effect-ts/core/Utils"
import * as Z from "@effect-ts/keeper"
import { KeeperConfig, monitor } from "@effect-ts/keeper"
import * as R from "@effect-ts/node/Runtime"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"

import * as Cluster from "../src/Cluster"
import { LivePersistence } from "../src/Persistence"
import {
  makeRemotingExpressConfig,
  RemotingExpress,
  RemotingExpressConfig
} from "../src/Remote"
import * as Singleton from "../src/Singleton"
import { transactional } from "../src/Transactional"

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">>>"](RemotingExpress[">+>"](Cluster.LiveCluster))
  ["<<<"](
    L.fromEffect(RemotingExpressConfig)(
      T.succeedWith(() =>
        makeRemotingExpressConfig({
          host: "127.0.0.1",
          port: parseInt(process.env["PORT"]!)
        })
      )
    )
  )
  ["<+<"](
    Z.LiveKeeperClient["<<<"](
      L.fromEffect(KeeperConfig)(
        T.succeed(Z.makeKeeperConfig({ connectionString: "127.0.0.1:2181" }))
      )
    )
  )
  ["<+<"](
    LivePersistence["<+<"](
      PG.LivePG["<<<"](
        L.fromEffect(PG.PGConfig)(
          T.succeed(
            PG.makePGConfig({
              host: "127.0.0.1",
              user: "user",
              password: "pass",
              port: 5432,
              database: "db"
            })
          )
        )
      )
    )
  )

const unit = S.unknown["|>"](S.brand<void>())

class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}

const Message = AM.messages(Get, Increase)
type Message = AM.TypeOf<typeof Message>

const statefulHandler = transactional(
  Message,
  S.number,
  O.none
)(({ state }) => ({
  Get: () => state.get,
  Increase: () =>
    pipe(
      state.get,
      T.chain((n) => state.set(n + 1))
    )
}))

export const makeProcessService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const processA = yield* _(
    system.make(
      "process-a",
      SUP.none,
      Singleton.makeSingleton(statefulHandler, (self) =>
        T.forever(
          T.delay(1_000)(
            self.ask(new Increase())["|>"](
              T.tap(() =>
                T.succeedWith(() => {
                  console.log("HERE")
                })
              )
            )
          )
        )
      ),
      0
    )
  )

  return {
    processA
  }
})

export interface ProcessService extends _A<typeof makeProcessService> {}
export const ProcessService = tag<ProcessService>()
export const LiveProcessService = L.fromManaged(ProcessService)(makeProcessService)

pipe(
  T.gen(function* (_) {
    const { processA } = yield* _(ProcessService)
    const { manager } = yield* _(Cluster.Cluster)

    while (1) {
      console.log("n:", yield* _(processA.ask(new Get())))
      console.log(
        "m:",
        Array.from((yield* _(manager.ask(new Cluster.GetMembers()))).members)
      )
      yield* _(T.sleep(2_000))
    }
  })["|>"](T.eventually),
  T.race(monitor),
  T.provideSomeLayer(AppLayer[">+>"](LiveProcessService)),
  R.runMain
)
