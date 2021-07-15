import * as CS from "@effect-ts/core/Case"
import type { IsEqualTo } from "@effect-ts/core/Utils"
import type * as S from "@effect-ts/schema"
import { _A, _E } from "@effect-ts/system/Effect"

import type { Throwable } from "../Common"

export const RequestSchemaSymbol = Symbol.for("@effect-ts/actors/RequestSchema")
export const ResponseSchemaSymbol = Symbol.for("@effect-ts/actors/ResponseSchema")
export const _Response = Symbol.for("@effect-ts/actors/PhantomResponse")

export interface Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
> {
  readonly _tag: Tag
  readonly [_Response]: () => S.ParsedShapeOf<Res>

  readonly [ResponseSchemaSymbol]: Res
  readonly [RequestSchemaSymbol]: Req
}

export type AnyMessage = Message<any, S.SchemaAny, S.SchemaAny>

export type ResponseOf<A extends AnyMessage> = [A] extends [Message<any, any, infer B>]
  ? S.ParsedShapeOf<B>
  : void

export type RequestOf<A extends AnyMessage> = [A] extends [Message<any, infer B, any>]
  ? S.ParsedShapeOf<B>
  : void

export type TagsOf<A extends AnyMessage> = A["_tag"]
export type ExtractTagged<A extends AnyMessage, Tag extends string> = Extract<
  A,
  Message<Tag, any, any>
>

type MessageFactoryOf<A extends AnyMessage> = [A] extends [
  Message<infer Tag, infer Req, infer Res>
]
  ? MessageFactory<Tag, Req, Res>
  : never

export interface MessageFactory<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
> {
  readonly Tag: Tag
  readonly RequestSchema: Req
  readonly ResponseSchema: Res

  new (
    _: IsEqualTo<S.ParsedShapeOf<Req>, {}> extends true ? void : S.ParsedShapeOf<Req>
  ): S.ParsedShapeOf<Req> & Message<Tag, Req, Res>
}

export type AnyMessageFactory = MessageFactory<any, S.SchemaAny, S.SchemaAny>

export type TypeOf<A extends MessageRegistry<any>> = [A] extends [
  MessageRegistry<infer B>
]
  ? B
  : never

export function Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
>(Tag: Tag, Req: Req, Res: Res): MessageFactory<Tag, Req, Res> {
  // @ts-expect-error
  return class extends CS.Tagged(Tag)<S.ParsedShapeOf<Req>> {
    static RequestSchema = Req
    static ResponseSchema = Res
    static Tag = Tag;

    readonly [ResponseSchemaSymbol] = Res;
    readonly [RequestSchemaSymbol] = Req;

    // NOTE(mattiamanzati): This phantom fields allow to use a message as if it were a @effect-ts/query Request<E, A>
    readonly [_E]: () => Throwable;
    readonly [_A]: () => S.ParsedShapeOf<Res>
  }
}

export class MessageRegistry<F1 extends AnyMessage> {
  constructor(
    readonly index: { [Tag in TagsOf<F1>]: MessageFactoryOf<ExtractTagged<F1, Tag>> },
    readonly reverse: Record<string, TagsOf<F1>>
  ) {}
}

export function messages<Messages extends AnyMessageFactory>(
  messages: Record<string, Messages>
): MessageRegistry<InstanceType<Messages>> {
  const index = Object.values(messages).reduce(
    (obj, entry) => ({ ...obj, [entry.Tag]: entry }),
    {} as Record<string, AnyMessageFactory>
  )

  const reverse = Object.keys(messages).reduce(
    (obj, key) => ({ ...obj, [key]: messages[key].Tag }),
    {} as Record<string, string>
  )

  return new MessageRegistry(index as any, reverse)
}
