import * as T from "@effect-ts/core/Effect"
import { pipe, tuple } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"

import * as AC from "../src/Actor"
import * as AS from "../src/ActorSystem"
import { _Response } from "../src/common"
import * as SUP from "../src/Supervisor"

class Reset {
  readonly _tag = "Reset"
}
class Increase {
  readonly _tag = "Increase"
}
class Get {
  readonly _tag = "Get";
  readonly [_Response]: () => number
}

type Message = Reset | Increase | Get

const handler = new AC.Stateful<unknown, number, Message>(((
  state: number,
  msg: Message,
  ctx: AS.Context
) => {
  switch (msg._tag) {
    case "Reset":
      return T.succeed(tuple(0, undefined))
    case "Increase":
      return T.succeed(tuple(state + 1, undefined))
    case "Get":
      return T.succeed(tuple(state, state))
  }
}) as any)

describe("Actor", () => {
  it("basic actor", async () => {
    const program = pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, 0, handler)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get())),
      T.tap((_) => _.actor.tell(new Reset())),
      T.bind("c2", (_) => _.actor.ask(new Get()))
    )

    const result = await T.runPromise(program)
    expect(result.c1).toEqual(2)
    expect(result.c2).toEqual(0)
  })

  it("actor selection", async () => {
    const program = pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.tap((_) => _.system.make("actor1", SUP.none, 0, handler)),
      T.bind("actor", (_) => _.system.select("zio://test1@0.0.0.0:0000/actor1")),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get()))
    )

    const result = await T.runPromise(program)
    expect(result.c1).toEqual(1)
  })
})
