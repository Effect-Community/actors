export type Throwable = unknown

export const _Response = Symbol("@effect-ts/actors/Response")
export type _ResponseOf<A> = A extends { [_Response]: () => infer B } ? B : void

export class ActorAlreadyExistsException {
  readonly _tag = "ActorAlreadyExistsException"
  constructor(readonly actorName: string) {}
}

export class NoSuchActorException {
  readonly _tag = "NoSuchActorException"
  constructor(readonly path: string) {}
}

export class NoRemoteSupportException {
  readonly _tag = "NoRemoteSupportException"
}

export class InvalidActorName {
  readonly _tag = "InvalidActorName"
  constructor(readonly actorName: string) {}
}

export class InvalidActorPath {
  readonly _tag = "InvalidActorPath"
  constructor(readonly path: string) {}
}

export class ErrorMakingActorException {
  readonly _tag = "ErrorMakingActorException"
  constructor(readonly error: unknown) {}
}
