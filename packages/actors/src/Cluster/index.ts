import { Tagged } from "@effect-ts/core/Case"
import * as A from "@effect-ts/core/Collections/Immutable/Array"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as OT from "@effect-ts/core/OptionT"
import { chainF } from "@effect-ts/core/Prelude"
import type { _A } from "@effect-ts/core/Utils"
import * as K from "@effect-ts/keeper"

import * as AS from "../ActorSystem"

export const ClusterConfigSym = Symbol()

const EO = pipe(OT.monad(T.Monad), (M) => ({
  map: M.map,
  chain: chainF(M)
}))

export interface ConfigInput {
  readonly sysName: string
  readonly host: string
  readonly port: number
}

export interface ClusterConfig extends ConfigInput {
  readonly [ClusterConfigSym]: typeof ClusterConfigSym
  readonly system: AS.ActorSystem
}

export function makeClusterConfig(_: {
  system: AS.ActorSystem
  sysName: string
  host: string
  port: number
}): ClusterConfig {
  return {
    [ClusterConfigSym]: ClusterConfigSym,
    ..._
  }
}

export function LiveClusterConfig(cfg: ConfigInput) {
  return L.fromManaged(ClusterConfig)(
    M.gen(function* (_) {
      const system = yield* _(AS.make(cfg.sysName, O.none))

      return makeClusterConfig({ system, ...cfg })
    })
  )
}

export const ClusterConfig = tag<ClusterConfig>()

export const ClusterSym = Symbol()

export class HostPort extends Tagged("HostPort")<{
  readonly host: string
  readonly port: number
}> {}

export const makeCluster = M.gen(function* (_) {
  const { host, port, system } = yield* _(ClusterConfig)
  const cli = yield* _(K.KeeperClient)

  const membersDir = `/cluster/${system.actorSystemName}/members`

  yield* _(cli.mkdir(membersDir))

  const prefix = `${membersDir}/id_`

  const nodePath = yield* _(
    cli.create(prefix, {
      mode: "EPHEMERAL_SEQUENTIAL",
      data: Buffer.from(JSON.stringify({ host, port }))
    })
  )

  const nodeId = nodePath.substr(prefix.length)

  const members = cli.getChildren(membersDir)["|>"](
    T.chain(
      T.forEach((childPath) =>
        pipe(
          cli.getData(`${membersDir}/${childPath}`),
          T.chain(T.getOrFail),
          T.map((b) => new HostPort(JSON.parse(b.toString("utf8"))))
        )
      )
    )
  )

  const leader = pipe(
    cli.getChildren(membersDir),
    T.map(A.head),
    EO.map((s) => `${membersDir}/${s}`),
    EO.chain(cli.getData),
    EO.map((b) => new HostPort(JSON.parse(b.toString("utf8"))))
  )

  return {
    [ClusterSym]: ClusterSym,
    nodeId,
    members,
    leader
  } as const
})

export interface Cluster extends _A<typeof makeCluster> {}
export const Cluster = tag<Cluster>()
export const LiveCluster = L.fromManaged(Cluster)(makeCluster)

export function DefaultCluster(cfg: ConfigInput) {
  return LiveClusterConfig(cfg)[">+>"](LiveCluster)
}
