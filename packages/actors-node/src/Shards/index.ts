import * as T from "@effect-ts/core/Effect"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import { hash } from "@effect-ts/core/Structural"

export interface Shards {
  shards: number
}

export const Shards = tag<Shards>()
const mod = (m: number) => (x: number) => x < 0 ? (x % m) + m : x % m

export const computeShardForId = (id: string) =>
  T.access((r: unknown) => {
    const maybe = Shards.readOption(r)
    if (O.isSome(maybe)) {
      return mod(maybe.value.shards)(hash(id))
    } else {
      return mod(16)(hash(id))
    }
  })
