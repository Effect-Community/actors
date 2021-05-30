import * as T from "@effect-ts/core/Effect"
import { hole, pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import type { IsEqualTo } from "@effect-ts/core/Utils"
import { matchTag } from "@effect-ts/core/Utils"
import * as S from "@effect-ts/schema"

import type { AbstractStateful } from "./Actor"
import * as AS from "./ActorSystem"
import type { _Response, _ResponseOf, Throwable } from "./common"
import * as SUP from "./Supervisor"

export const unit = S.unknown["|>"](S.brand<void>())

const ResponseSchemaSymbol = Symbol()
const RequestSchemaSymbol = Symbol()

type TaggedType<Req extends S.SchemaUPI, Res extends S.SchemaUPI, Tag extends string> =
  S.ParsedShapeOf<Req> & {
    readonly _tag: Tag
    readonly [_Response]: () => S.ParsedShapeOf<Res>

    readonly [ResponseSchemaSymbol]: Res
    readonly [RequestSchemaSymbol]: Req
  }

interface MessageFactory<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
> {
  readonly Tag: Tag
  readonly RequestSchema: Req
  readonly ResponseSchema: Res

  new (
    _: IsEqualTo<S.ParsedShapeOf<Req>, {}> extends true ? void : S.ParsedShapeOf<Req>
  ): TaggedType<Req, Res, Tag>
}

export function Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
>(Tag: Tag, Req: Req, Res: Res): MessageFactory<Tag, Req, Res> {
  // @ts-expect-error
  return class {
    static RequestSchema = Req
    static ResponseSchema = Res
    static Tag = Tag

    readonly _tag = Tag;

    readonly [ResponseSchemaSymbol] = Res;
    readonly [RequestSchemaSymbol] = Req

    constructor(ps?: any) {
      if (ps) {
        for (const k of Object.keys(ps)) {
          Object.defineProperty(this, k, { value: ps[k] })
        }
      }
    }
  }
}

type AnyMessageFactory = MessageFactory<any, S.SchemaAny, S.SchemaAny>
type AnyMessageFactoryRecord = Record<string, AnyMessageFactory>

export declare function messages<Messages extends readonly AnyMessageFactory[]>(
  ...messages: Messages
): {
  [K in Messages[number]["Tag"]]: Extract<Messages[number], { Tag: K }>
}

type InstanceOf<R extends AnyMessageFactory> = R extends {
  new (...args: any[]): infer A
}
  ? A
  : never

export function statefulHandler<
  StateSchema extends S.SchemaUPI,
  Messages extends AnyMessageFactoryRecord
>(messages: Messages, state: StateSchema) {
  return <R>(
    handler: (
      state: S.ParsedShapeOf<StateSchema>,
      context: AS.Context
    ) => (
      msg: {
        [k in keyof Messages]: {
          _tag: InstanceOf<Messages[k]>["_tag"]
          payload: InstanceOf<Messages[k]>
          return: IsEqualTo<_ResponseOf<InstanceOf<Messages[k]>>, void> extends true
            ? (
                state: S.ParsedShapeOf<StateSchema>
              ) => T.UIO<
                readonly [
                  S.ParsedShapeOf<StateSchema>,
                  _ResponseOf<InstanceOf<Messages[k]>>
                ]
              >
            : (
                state: S.ParsedShapeOf<StateSchema>,
                response: _ResponseOf<InstanceOf<Messages[k]>>
              ) => T.UIO<
                readonly [
                  S.ParsedShapeOf<StateSchema>,
                  _ResponseOf<InstanceOf<Messages[k]>>
                ]
              >
        }
      }[keyof Messages]
    ) => T.Effect<
      R,
      Throwable,
      readonly [
        S.ParsedShapeOf<StateSchema>,
        {
          [k in keyof Messages]: _ResponseOf<InstanceOf<Messages[k]>>
        }[keyof Messages]
      ]
    >
  ): AbstractStateful<
    R,
    S.ParsedShapeOf<StateSchema>,
    InstanceOf<Messages[keyof Messages]>
  > => hole()
}

export class GetCount extends Message("GetCount", S.props({}), S.number) {}

export class IncCount extends Message("IncCount", S.props({}), unit) {}

const CounterMessage = messages(GetCount, IncCount)

class CounterState extends S.Model<CounterState>()(
  S.props({ count: S.prop(S.number) })
) {}

export const counter = statefulHandler(
  CounterMessage,
  CounterState
)((state) =>
  matchTag({
    GetCount: (_) => _.return(state, state.count),
    IncCount: (_) => _.return(state.copy({ count: state.count + 1 }))
  })
)

export const program = pipe(
  T.do,
  T.bind("system", () => AS.make("test1", O.none)),
  T.bind("actor", (_) =>
    _.system.make("actor1", SUP.none, new CounterState({ count: 0 }), counter)
  ),
  T.bind("c1", (_) => _.actor.ask(new GetCount())),
  T.tap((_) => _.actor.tell(new IncCount())),
  T.bind("c2", (_) => _.actor.ask(new GetCount()))
)
