import type * as T from "@effect-ts/core/Effect"
import type { IsEqualTo } from "@effect-ts/core/Utils"
import * as S from "@effect-ts/schema"

const ResponseSchemaSymbol = Symbol()
const RequestSchemaSymbol = Symbol()

type TaggedType<
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI,
  Tag extends string
> = S.ParsedShapeOf<Req> & {
  readonly _tag: Tag

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

type Opaque<A extends AnyMessageFactoryRecord> = A

interface Actor<F1 extends AnyMessageFactoryRecord> {
  ask<A extends InstanceOf<F1[keyof F1]>>(
    msg: A
  ): T.IO<unknown, S.ParsedShapeOf<F1[A["_tag"]]["ResponseSchema"]>>
}

export class GetCount extends Message("GetCount", S.props({}), S.number) {}

export class IncCount extends Message("IncCount", S.props({}), S.unknown) {}

const CounterMessage = messages(GetCount, IncCount)

// WORKS
declare const actor1: Actor<typeof CounterMessage>
// ERROR

interface CounterMessage extends Opaque<typeof CounterMessage> {}
declare const actor2: Actor<CounterMessage>

// ...
const shouldBeUnknown = actor1.ask(new IncCount())
const shouldBeNumber = actor2.ask(new GetCount())
