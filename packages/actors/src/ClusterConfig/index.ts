import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import { tag } from "@effect-ts/core/Has"

export const ClusterConfigSym = Symbol()

export interface ConfigInput {
  readonly sysName: string
  readonly host: string
  readonly port: number
}

export interface ClusterConfig extends ConfigInput {
  readonly [ClusterConfigSym]: typeof ClusterConfigSym
}

export function makeClusterConfig(_: {
  host: string
  port: number
  sysName: string
}): ClusterConfig {
  return {
    [ClusterConfigSym]: ClusterConfigSym,
    ..._
  }
}

export function StaticClusterConfig(cfg: ConfigInput) {
  return L.fromEffect(ClusterConfig)(T.succeedWith(() => makeClusterConfig(cfg)))
}

export const ClusterConfig = tag<ClusterConfig>()
