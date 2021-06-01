import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
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

import { ActorSystemTag } from "../ActorSystem"

export const ClusterConfigSym = Symbol()

const EO = pipe(OT.monad(T.Monad), (M) => ({
  map: M.map,
  chain: chainF(M),
  chainT:
    <R2, E2, A, B>(f: (a: A) => T.Effect<R2, E2, B>) =>
    <R, E>(fa: T.Effect<R, E, O.Option<A>>): T.Effect<R2 & R, E2 | E, O.Option<B>> =>
      chainF(M)((a: A) => T.map_(f(a), O.some))(fa)
}))

export interface ConfigInput {
  readonly host: string
  readonly port: number
}

export interface ClusterConfig extends ConfigInput {
  readonly [ClusterConfigSym]: typeof ClusterConfigSym
}

export function makeClusterConfig(_: { host: string; port: number }): ClusterConfig {
  return {
    [ClusterConfigSym]: ClusterConfigSym,
    ..._
  }
}

export function LiveClusterConfig(cfg: ConfigInput) {
  return L.fromEffect(ClusterConfig)(T.succeedWith(() => makeClusterConfig(cfg)))
}

export const ClusterConfig = tag<ClusterConfig>()

export const ClusterSym = Symbol()

export class HostPort extends Tagged("HostPort")<{
  readonly host: string
  readonly port: number
}> {}

export class ClusterException extends Tagged("ClusterException")<{
  readonly message: string
}> {}

export const makeCluster = M.gen(function* (_) {
  const { host, port } = yield* _(ClusterConfig)
  const cli = yield* _(K.KeeperClient)
  const system = yield* _(ActorSystemTag)
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

  const getMemberHostPort = yield* _(
    T.memoize(
      (childPath: string): T.Effect<unknown, K.ZooError, HostPort> =>
        pipe(
          cli.getData(`${membersDir}/${childPath}`),
          T.chain(
            O.fold(
              () =>
                T.die(
                  new ClusterException({
                    message: `cannot find metadata on path: ${membersDir}/${childPath}`
                  })
                ),
              (b) => T.succeed(b)
            )
          ),
          T.map((b) => new HostPort(JSON.parse(b.toString("utf8"))))
        )
    )
  )

  const members = pipe(
    cli.getChildren(membersDir),
    T.chain(T.forEach(getMemberHostPort))
  )

  const leader = pipe(
    cli.getChildren(membersDir),
    T.map(Chunk.head),
    EO.chainT(getMemberHostPort)
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
