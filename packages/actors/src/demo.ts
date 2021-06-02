import "@effect-ts/core/Tracing/Enable"
import "isomorphic-fetch"

import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import { pipe } from "@effect-ts/core/Function"
import * as Z from "@effect-ts/keeper"
import * as R from "@effect-ts/node/Runtime"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "./Actor"
import * as Cluster from "./Cluster"
import * as ClusterConfigSym from "./ClusterConfig"
import * as AM from "./Message"

const AppLayer = Z.LiveKeeperClient["<<<"](
  L.fromEffect(Z.KeeperConfig)(
    T.succeedWith(() => Z.makeKeeperConfig({ connectionString: "127.0.0.1:2181" }))
  )
)[">+>"](
  Cluster.LiveCluster["<<<"](
    ClusterConfigSym.StaticClusterConfig({
      sysName: "EffectTsActorsDemo",
      host: "127.0.0.1",
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      port: parseInt(process.env["CLUSTER_PORT"]!)
    })
  )
)

const unit = S.unknown["|>"](S.brand<void>())

class Reset extends AM.Message("Reset", S.props({}), unit) {}
class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}
class GetAndReset extends AM.Message("GetAndReset", S.props({}), S.number) {}

const Message = AM.messages(Reset, Increase, Get, GetAndReset)
type Message = AM.TypeOf<typeof Message>

const statefulHandler = AC.stateful(
  Message,
  S.number
)((state, ctx) =>
  matchTag({
    Reset: (_) => _.return(0),
    Increase: (_) => _.return(state + 1),
    Get: (_) => {
      return _.return(state, state)
    },
    GetAndReset: (_) =>
      pipe(
        ctx.self,
        T.chain((self) => self.tell(new Reset())),
        T.zipRight(_.return(state, state))
      )
  })
)

const ProcessA = Cluster.makeSingleton("process-a")(statefulHandler, T.succeed(0))

pipe(
  T.gen(function* (_) {
    const actor = yield* _(ProcessA.actor)
    let m = 0

    while (1) {
      yield* _(T.sleep(1_000))
      console.log(yield* _(actor.ask(new Get())), m++)
      yield* _(actor.tell(new Increase()))
    }
  })["|>"](
    T.race(
      T.gen(function* (_) {
        const cli = yield* _(Z.KeeperClient)
        yield* _(cli.monitor)
      })
    )
  ),
  T.provideSomeLayer(AppLayer[">+>"](ProcessA.Live)),
  R.runMain
)
