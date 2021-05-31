import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "../src/Actor"
import * as AS from "../src/ActorSystem"
import * as AA from "../src/Address"
import * as ESS from "../src/EventSourcedStateful"
import * as J from "../src/Journal"
import * as AM from "../src/Message"
import * as SUP from "../src/Supervisor"

const unit = S.unknown["|>"](S.brand<void>())

class Reset extends AM.Message("Reset", S.props({}), unit) {}
class Increase extends AM.Message("Increase", S.props({}), unit) {}
class Get extends AM.Message("Get", S.props({}), S.number) {}
class GetAndReset extends AM.Message("GetAndReset", S.props({}), S.number) {}

const Message = AM.messages(Reset, Increase, Get, GetAndReset)
type Message = AM.TypeOf<typeof Message>

const handler = AC.stateful(
  Message,
  S.number
)((state, ctx) =>
  matchTag({
    Reset: (_) => _.return(0),
    Increase: (_) => _.return(state + 1),
    Get: (_) => _.return(state, state),
    GetAndReset: (_) =>
      pipe(
        ctx.self,
        T.chain((self) => self.tell(new Reset())),
        T.zipRight(_.return(state, state))
      )
  })
)

class Increased extends S.Schemed(
  S.props({
    _tag: S.prop(S.literal("Increased"))
  })
) {}

class Resetted extends S.Schemed(
  S.props({
    _tag: S.prop(S.literal("Resetted"))
  })
) {}

const Event = S.union({ Increased: S.schema(Increased), Resetted: S.schema(Resetted) })
type Event = S.ParsedShapeOf<typeof Event>

const esHandler = ESS.eventSourcedStateful(Message, S.number, Event)(
  new J.PersistenceId({ id: "counter" }),
  (state, ctx) =>
    matchTag({
      Reset: (_) => _.return(CH.single(new Resetted({}))),
      Increase: (_) => _.return(CH.single(new Increased({}))),
      Get: (_) => _.return(CH.empty(), (state) => state),
      GetAndReset: (_) =>
        pipe(
          ctx.self,
          T.chain((self) => self.tell(new Reset())),
          T.chain(() => _.return(CH.empty(), (state) => state))
        )
    }),
  (state) =>
    matchTag({
      Increased: (_) => state + 1,
      Resetted: (_) => 0
    })
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
      T.bind("actor", (_) =>
        _.system.select(AA.address("zio://test1@0.0.0.0:0000/actor1", Message))
      ),
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
