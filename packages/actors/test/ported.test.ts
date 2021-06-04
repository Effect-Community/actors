import * as CH from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as SE from "@effect-ts/core/Effect/Schedule"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import * as TE from "@effect-ts/jest/Test"
import * as S from "@effect-ts/schema"
import { matchTag } from "@effect-ts/system/Utils"

import * as AC from "../src/Actor"
import * as AS from "../src/ActorSystem"
import * as AA from "../src/Address"
import * as EN from "../src/Envelope"
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

class Increased extends S.Model<Increased>()(
  S.props({
    _tag: S.prop(S.literal("Increased"))
  })
) {}

class Resetted extends S.Model<Resetted>()(
  S.props({
    _tag: S.prop(S.literal("Resetted"))
  })
) {}

const Event = S.union({ Increased, Resetted })
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
  const { it } = TE.runtime()
  it("basic actor", () =>
    pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, handler, 0)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get())),
      T.tap((_) => _.actor.tell(new Reset())),
      T.bind("c2", (_) => _.actor.ask(new Get())),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(2)
          expect(result.c2).toEqual(0)
        })
      )
    ))

  it("event sourced actor", () =>
    pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, esHandler, 0)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get())),
      T.tap((_) => _.actor.tell(new Reset())),
      T.bind("c2", (_) => _.actor.ask(new Get())),
      T.provideServiceM(J.JournalFactory)(J.makeInMemJournal),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(2)
          expect(result.c2).toEqual(0)
        })
      )
    ))

  it("actor selection", () =>
    pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, handler, 0)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new GetAndReset())),
      T.bind("c2", (_) => _.actor.ask(new Get())),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(1)
          expect(result.c2).toEqual(0)
        })
      )
    ))

  it("actor can self-message", () =>
    pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.tap((_) => _.system.make("actor1", SUP.none, handler, 0)),
      T.bind("actor", (_) =>
        _.system.select(AA.address("zio://test1@0.0.0.0:0000/actor1", Message))
      ),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get())),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(1)
        })
      )
    ))

  it("event sourced actor can restore its state", () =>
    pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, esHandler, 0)),
      T.tap((_) => _.actor.tell(new Increase())),
      T.tap((_) => _.actor.tell(new Increase())),
      T.bind("c1", (_) => _.actor.ask(new Get())),
      T.bind("system2", () => AS.make("test1", O.none)),
      T.bind("actor2", (_) => _.system2.make("actor1", SUP.none, esHandler, 0)),
      T.bind("c2", (_) => _.actor2.ask(new Get())),
      T.provideServiceM(J.JournalFactory)(J.makeInMemJournal),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(2)
          expect(result.c2).toEqual(result.c1)
        })
      )
    ))

  it("error recovery by retrying", () => {
    class Tick extends AM.Message("Tick", S.props({}), unit) {}
    const TickMessage = AM.messages(Tick)

    const tickHandler = (ref: REF.Ref<number>) =>
      AC.stateful(
        TickMessage,
        S.number
      )((_, ctx) =>
        matchTag({
          Tick: (_) =>
            pipe(
              REF.updateAndGet_(ref, (s) => s + 1),
              T.chain((s) => (s < 10 ? T.fail("fail") : _.return(0)))
            )
        })
      )

    const program = pipe(
      T.do,
      T.bind("ref", (_) => REF.makeRef(0)),
      T.let("handler", (_) => tickHandler(_.ref)),
      T.let("schedule", (_) => SE.recurs(10)),
      T.let("policy", (_) => SUP.retry(_.schedule)),
      T.bind("system", () => AS.make("test2", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", _.policy, tickHandler(_.ref), 0)),
      T.tap((_) => _.actor.ask(new Tick())),
      T.bind("c1", (_) => REF.get(_.ref)),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(10)
        })
      )
    )

    return program
  })

  it("should handle envelope", () =>
    pipe(
      T.do,
      T.bind("system", () => AS.make("test1", O.none)),
      T.bind("actor", (_) => _.system.make("actor1", SUP.none, handler, 0)),
      T.bind("address", (_) => _.actor.path),
      T.tap((_) =>
        _.system.runEnvelope(EN.envelope(EN.tell({ _tag: "Increase" }), _.address.path))
      ),
      T.tap((_) =>
        _.system.runEnvelope(EN.envelope(EN.tell({ _tag: "Increase" }), _.address.path))
      ),
      T.bind("c1", (_) =>
        _.system.runEnvelope(EN.envelope(EN.ask({ _tag: "Get" }), _.address.path))
      ),
      T.tap((_) =>
        _.system.runEnvelope(EN.envelope(EN.tell({ _tag: "Reset" }), _.address.path))
      ),
      T.bind("c2", (_) =>
        _.system.runEnvelope(EN.envelope(EN.ask({ _tag: "Get" }), _.address.path))
      ),
      T.tap((result) =>
        T.succeedWith(() => {
          expect(result.c1).toEqual(2)
          expect(result.c2).toEqual(0)
        })
      )
    ))
})
