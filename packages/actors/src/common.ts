export type Throwable = unknown

export const _Response = Symbol("@effect-ts/actors/Response")
export type _ResponseOf<A> = A extends { [_Response]: () => infer B } ? B : void

export class ActorAlreadyExistsException {
  constructor(readonly actorName: string) {}
}

export class NoSuchActorException {
  constructor(readonly path: string) {}
}

export class NoRemoteSupportException {}

export class InvalidActorName {
  constructor(readonly actorName: string) {}
}

export class InvalidActorPath {
  constructor(readonly path: string) {}
}
