import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"

import * as AC from "../src/Actor"
import * as AS from "../src/ActorSystem"
import { _Response } from "../src/common"
import * as ESS from "../src/EventSourcedStateful"
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
class GetAndReset {
  readonly _tag = "GetAndReset";
  readonly [_Response]: () => number
}

type Message = Reset | Increase | Get | GetAndReset

const handler = new AC.Stateful<unknown, number, Message>(
  (state, msg, ctx, replyTo) => {
    switch (msg._tag) {
      case "Reset":
        return T.succeed(replyTo(msg)(0))
      case "Increase":
        return T.succeed(replyTo(msg)(state + 1))
      case "Get":
        return T.succeed(replyTo(msg)(state, state))
      case "GetAndReset":
        return pipe(
          T.do,
          T.bind("self", () => ctx.self),
          T.tap((_) => _.self.tell(new Reset())),
          T.map((_) => replyTo(msg)(state, state))
        )
    }
  }
)

class Increased {
  readonly _tag = "Increased"
}
class Resetted {
  readonly _tag = "Resetted"
}
type Event = Increased | Resetted

const esHandler = new ESS.EventSourcedStateful<unknown, number, Message, Event>(
  (state, msg, ctx, replyTo) => {
    switch (msg._tag) {
      case "Reset":
        return T.succeed(replyTo(msg)(CH.single(new Resetted()), (_) => undefined))
      case "Increase":
        return T.succeed(replyTo(msg)(CH.single(new Increased()), (_) => undefined))
      case "Get":
        return T.succeed(replyTo(msg)(CH.empty(), (state) => state))
      case "GetAndReset":
        return pipe(
          T.do,
          T.bind("self", () => ctx.self),
          T.tap((_) => _.self.tell(new Reset())),
          T.map((_) => replyTo(msg)(CH.empty(), (state) => state))
        )
    }
  },
  (state, event) => {
    switch (event._tag) {
      case "Increased":
        return state + 1
      case "Resetted":
        return 0
    }
  }
)

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

  it("event sourced actor", async () => {
    const program = pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, 0, esHandler)),
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
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, 0, handler)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new GetAndReset())),
      T.bind("c2", (_) => _.actor.ask(new Get()))
    )

    const result = await T.runPromise(program)
    expect(result.c1).toEqual(1)
    expect(result.c2).toEqual(0)
  })

  it("actor can self-message", async () => {
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
