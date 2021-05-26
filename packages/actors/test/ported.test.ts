import * as A from "@effect-ts/core/Collections/Immutable/Array"
import * as C from "@effect-ts/core/Collections/Immutable/Chunk"
import * as MAP from "@effect-ts/core/Collections/Immutable/Map"
import * as T from "@effect-ts/core/Effect"
import * as Ex from "@effect-ts/core/Effect/Exit"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as E from "@effect-ts/core/Either"
import { identity, pipe, tuple } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import { NoSuchElementException } from "@effect-ts/system/GlobalExceptions"

import * as AC from "../src/Actor"
import * as AR from "../src/ActorRef"
import * as AS from "../src/ActorSystem"
import { _Response } from "../src/common"

class Reset {
  readonly _tag = "Reset"
}
class Increase {
  readonly _tag = "Increase"
}
class Get {
  readonly _tag = "Get";
  readonly [_Response]: number
}

type Message = Reset | Increase | Get

describe("Actor", () => {
  it("basic actor", async () => {
    const handler = new AC.Stateful<unknown, number, Message>((state, msg, ctx) => {
      switch (msg._tag) {
        case "Reset":
          return T.succeed(tuple(0, undefined))
        case "Increase":
          return T.succeed(tuple(state + 1, undefined))
        case "Get":
          return T.succeed(tuple(state, state))
      }
    })

    //expect(await T.runPromise(f)).toEqual(userIds)
  })
})
