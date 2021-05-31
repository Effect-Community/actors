import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import * as S from "@effect-ts/schema"

import * as AC from "../src/Actor"
import * as AS from "../src/ActorSystem"
import * as ESS from "../src/EventSourcedStateful"
import * as J from "../src/Journal"
import * as AM from "../src/Message"
import * as SUP from "../src/Supervisor"

const unit = S.unknown["|>"](S.brand<void>())

class Reset extends AM.Message("Reset", S.props({}), unit) {}
class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}
class GetAndReset extends AM.Message("GetAndReset", S.props({}), S.number) {}

type Message = Reset | Increase | Get | GetAndReset

const handler = new AC.Stateful<unknown, number, Message>((state, ctx) => (msg) => {
  switch (msg._tag) {
    case "Reset":
      return msg.return(0)
    case "Increase":
      return msg.return(state + 1)
    case "Get":
      return msg.return(state, state)
    case "GetAndReset":
      return pipe(
        T.do,
        T.bind("self", () => ctx.self),
        T.tap((_) => _.self.tell(new Reset())),
        T.chain((_) => msg.return(state, state))
      )
  }
})

class Increased {
  readonly _tag = "Increased"
}
class Resetted {
  readonly _tag = "Resetted"
}
type Event = Increased | Resetted

const esHandler = new ESS.EventSourcedStateful<unknown, number, Message, Event>(
  new J.PersistenceId({ id: "counter" }),
  (state, ctx) => (msg) => {
    switch (msg._tag) {
      case "Reset":
        return msg.return(CH.single(new Resetted()))
      case "Increase":
        return msg.return(CH.single(new Increased()))
      case "Get":
        return msg.return(CH.empty(), (state) => state)
      case "GetAndReset":
        return pipe(
          T.do,
          T.bind("self", () => ctx.self),
          T.tap((_) => _.self.tell(new Reset())),
          T.chain((_) => msg.return(CH.empty(), (state) => state))
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
      T.bind("c2", (_) => _.actor.ask(new Get())),
      T.provideServiceM(J.JournalFactory)(J.makeInMemJournal)
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

  it("event sourced actor can restore its state", async () => {
    const program = pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, 0, esHandler)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get())),
      //T.chain(() => T.do),
      T.bind("system2", () => AS.make("test1", O.none)),
      T.bind("actor2", (_) => _.system2.make("actor1", SUP.none, 0, esHandler)),
      T.bind("c2", (_) => _.actor2.ask(new Get())),
      T.provideServiceM(J.JournalFactory)(J.makeInMemJournal)
    )

    const result = await T.runPromise(program)
    expect(result.c1).toEqual(2)
    expect(result.c2).toEqual(result.c1)
  })
})
