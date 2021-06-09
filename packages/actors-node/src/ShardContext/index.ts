import * as T from "@effect-ts/core/Effect"
import { tag } from "@effect-ts/core/Has"
import { hash } from "@effect-ts/core/Structural"

export interface ShardContext {
  domain: string
  shards: number
}

export const ShardContext = tag<ShardContext>()

const mod = (m: number) => (x: number) => x < 0 ? (x % m) + m : x % m

export const computeShardForId = (id: string) =>
  T.accessService(ShardContext)(({ domain, shards }) => {
    return mod(shards)(hash(`${domain}-${id}`)) + 1
  })
