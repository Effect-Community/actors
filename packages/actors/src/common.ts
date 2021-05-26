export type Throwable = unknown

export const _Response = Symbol("@effect-ts/actors/Response")
export type _ResponseOf<A> = [A] extends [{ [_Response]: infer B }] ? B : unknown
