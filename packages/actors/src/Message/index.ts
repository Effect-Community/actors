import * as D from "@effect-ts/core/Collections/Immutable/Dictionary"
import * as T from "@effect-ts/core/Effect"
import * as O from "@effect-ts/core/Option"
import type { IsEqualTo } from "@effect-ts/core/Utils"
import * as S from "@effect-ts/schema"
import * as ENC from "@effect-ts/schema/Encoder"
import * as PARS from "@effect-ts/schema/Parser"
import * as TH from "@effect-ts/schema/These"
import { pipe, tuple } from "@effect-ts/system/Function"

import { ActorSystemException, CommandParserException } from "../common"

export const RequestSchemaSymbol = Symbol("@effect-ts/actors/RequestSchema")
export const ResponseSchemaSymbol = Symbol("@effect-ts/actors/RequestSchema")
export const ErrorSchemaSymbol = Symbol("@effect-ts/actors/ErrorSchema")
export const _Response = Symbol("@effect-ts/actors/PhantomResponse")
export const _Error = Symbol("@effect-ts/actors/PhantomError")

export interface Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Err extends O.Option<S.SchemaUPI>,
  Res extends S.SchemaUPI
> {
  readonly _tag: Tag
  readonly [_Response]: () => S.ParsedShapeOf<Res>
  readonly [_Error]: () => [Err] extends [O.Some<any>]
    ? S.ParsedShapeOf<Err["value"]>
    : never

  readonly [RequestSchemaSymbol]: Req
  readonly [ErrorSchemaSymbol]: Res
  readonly [ResponseSchemaSymbol]: Res
}

export type TypedMessage<
  Tag extends string,
  Req extends S.SchemaUPI,
  Err extends O.Option<S.SchemaUPI>,
  Res extends S.SchemaUPI
> = S.ParsedShapeOf<Req> & Message<Tag, Req, Err, Res>

export type AnyMessage = Message<any, S.SchemaAny, any, S.SchemaAny>

export interface MessageFactory<
  Tag extends string,
  Req extends S.SchemaUPI,
  Err extends O.Option<S.SchemaUPI>,
  Res extends S.SchemaUPI
> {
  readonly Tag: Tag
  readonly RequestSchema: Req
  readonly ErrorSchema: Err
  readonly ResponseSchema: Res

  new (
    _: IsEqualTo<S.ParsedShapeOf<Req>, {}> extends true ? void : S.ParsedShapeOf<Req>
  ): TypedMessage<Tag, Req, Err, Res>
}

export type AnyMessageFactory = MessageFactory<
  any,
  S.SchemaAny,
  O.Option<S.SchemaAny>,
  S.SchemaAny
>

export type RequestOf<A extends AnyMessage> = [A] extends [
  Message<any, infer B, any, any>
]
  ? S.ParsedShapeOf<B>
  : void

export type ErrorOf<A extends AnyMessage> = [A] extends [Message<any, any, any, any>]
  ? ReturnType<A[typeof _Error]>
  : never

export type ResponseOf<A extends AnyMessage> = [A] extends [
  Message<any, any, any, infer B>
]
  ? S.ParsedShapeOf<B>
  : void

export type TagsOf<A extends AnyMessage> = A["_tag"]
export type ExtractTagged<A extends AnyMessage, Tag extends string> = Extract<
  A,
  TypedMessage<Tag, any, any, any>
>

type MessageFactoryOf<A extends AnyMessage> = [A] extends [
  Message<infer Tag, infer Req, infer Err, infer Res>
]
  ? MessageFactory<Tag, Req, Err, Res>
  : never

export interface MessageRegistry<F1>
  extends Record<string, [F1] extends [AnyMessage] ? MessageFactoryOf<F1> : never> {}

export type InstanceOf<A> = [A] extends [{ new (...any: any[]): infer B }] ? B : never

export type TypeOf<A extends MessageRegistry<any>> = [A] extends [
  MessageRegistry<infer B>
]
  ? B
  : never

export function Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Err extends S.SchemaUPI,
  Res extends S.SchemaUPI
>(Tag: Tag, Req: Req, Err: Err, Res: Res): MessageFactory<Tag, Req, O.Some<Err>, Res> {
  // @ts-expect-error
  return class {
    static Tag = Tag
    static RequestSchema = Req
    static ErrorSchema = Err
    static ResponseSchema = Res

    readonly _tag = Tag;

    readonly [RequestSchemaSymbol] = Req;
    readonly [ErrorSchemaSymbol] = Err;
    readonly [ResponseSchemaSymbol] = Res

    constructor(ps?: any) {
      if (ps) {
        for (const k of Object.keys(ps)) {
          Object.defineProperty(this, k, { value: ps[k] })
        }
      }
    }
  }
}

export function messages<Messages extends AnyMessageFactory[]>(
  ...messages: Messages
): MessageRegistry<InstanceOf<Messages[number]>> {
  return messages.reduce(
    (obj, entry) => ({ ...obj, [entry.Tag]: entry }),
    {} as MessageRegistry<any>
  )
}

export function decodeCommand<F1 extends AnyMessage>(
  registry: MessageRegistry<F1>
): (
  msg: unknown
) => T.Effect<
  unknown,
  ActorSystemException,
  readonly [
    F1,
    (response: ResponseOf<F1>) => unknown,
    (error: ErrorOf<F1> | ActorSystemException) => unknown
  ]
> {
  const parser = pipe(
    registry,
    D.map((entry) =>
      S.intersect_(entry.RequestSchema, S.props({ _tag: S.prop(S.literal(entry.Tag)) }))
    ),
    S.union,
    PARS.for
  )

  return (msg) =>
    pipe(
      parser(msg),
      TH.result,
      (e) => T.fromEither(() => e),
      T.map((t) =>
        tuple(
          new registry[t.get(0)._tag](t.get(0)),
          ENC.for(registry[t.get(0)._tag].ResponseSchema),
          ENC.for(
            S.union({
              Error: O.fold_(
                registry[t.get(0)._tag].ErrorSchema,
                () => S.unknown,
                (_) => _
              ),
              ActorSystemException
            })
          )
        )
      ),
      T.mapError((e) => CommandParserException(e))
    )
}
