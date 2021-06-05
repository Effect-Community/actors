import * as S from "@effect-ts/schema"

export const ActorAlreadyExistsException = (actorName: string) =>
  new ActorSystemException({
    message: "ActorAlreadyExistsException",
    meta: actorName
  })

export const NoSuchActorException = (path: string) =>
  new ActorSystemException({
    message: "NoSuchActorException",
    meta: path
  })

export const NoRemoteSupportException = () =>
  new ActorSystemException({
    message: "NoRemoteSupportException",
    meta: null
  })

export const InvalidActorName = (name: string) =>
  new ActorSystemException({
    message: "InvalidActorName",
    meta: name
  })

export const InvalidActorPath = (path: string) =>
  new ActorSystemException({
    message: "InvalidActorPath",
    meta: path
  })

export const ErrorMakingActorException = (exception: unknown) =>
  new ActorSystemException({
    message: "ErrorMakingActorException",
    meta: exception
  })

export const CommandParserException = (exception: unknown) =>
  new ActorSystemException({
    message: "CommandParserException",
    meta: exception
  })

export class ActorSystemException extends S.Model<ActorSystemException>()(
  S.props({
    _tag: S.prop(S.literal("ActorSystemException")),
    message: S.prop(S.string),
    meta: S.prop(S.unknown)
  })
) {}
