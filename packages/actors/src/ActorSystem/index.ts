import { Tagged } from "@effect-ts/core/Case"
import * as MAP from "@effect-ts/core/Collections/Immutable/HashMap"
import * as HS from "@effect-ts/core/Collections/Immutable/HashSet"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as REF from "@effect-ts/core/Effect/Ref"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as St from "@effect-ts/core/Structural"
import { pipe } from "@effect-ts/system/Function"

import type * as A from "../Actor"
import * as AR from "../ActorRef"
import {
  ActorAlreadyExistsException,
  ErrorMakingActorException,
  InvalidActorName,
  InvalidActorPath,
  NoSuchActorException
} from "../Common"
import type * as AM from "../Message"
import type * as SUP from "../Supervisor"

/**
 * Context for actor used inside Stateful which provides self actor reference and actor creation/selection API
 */
export class Context<FC extends AM.AnyMessage> {
  constructor(
    readonly address: string,
    readonly actorSystem: ActorSystem,
    readonly childrenRef: REF.Ref<HS.HashSet<AR.ActorRef<any>>>
  ) {}

  /**
   * Accessor for self actor reference
   *
   * @return actor reference in a task
   */
  self = this.actorSystem.select<FC>(this.address)

  /**
   * Creates actor and registers it to dependent actor system
   *
   * @param actorName name of the actor
   * @param sup       - supervision strategy
   * @param init      - initial state
   * @param stateful  - actor's behavior description
   * @tparam S  - state type
   * @tparam F1 - DSL type
   * @return reference to the created actor in effect that can't fail
   */
  make<R, S, F1 extends AM.AnyMessage>(
    actorName: string,
    sup: SUP.Supervisor<R>,
    stateful: A.AbstractStateful<R, S, F1>,
    init: S
  ): T.Effect<
    R & T.DefaultEnv,
    ActorAlreadyExistsException | InvalidActorName | ErrorMakingActorException,
    AR.ActorRef<F1>
  > {
    return pipe(
      T.do,
      T.bind("actorRef", () => this.actorSystem.make(actorName, sup, stateful, init)),
      T.bind("children", () => REF.get(this.childrenRef)),
      T.tap((_) => REF.set_(this.childrenRef, HS.add_(_.children, _.actorRef))),
      T.map((_) => _.actorRef)
    )
  }

  /**
   * Looks up for actor on local actor system, and in case of its absence - delegates it to remote internal module.
   * If remote configuration was not provided for ActorSystem (so the remoting is disabled) the search will
   * fail with ActorNotFoundException.
   * Otherwise it will always create remote actor stub internally and return ActorRef as if it was found.   *
   *
   * @param address - absolute path to the actor
   * @tparam F1 - actor's DSL type
   * @return task if actor reference. Selection process might fail with "Actor not found error"
   */
  select<F1 extends AM.AnyMessage>(address: string) {
    return this.actorSystem.select<F1>(address)
  }
}

export class RemoteConfig extends Tagged("RemoteConfig")<{
  host: string
  port: number
}> {}

export class ActorSystem {
  constructor(
    readonly actorSystemName: string,
    readonly remoteConfig: O.Option<RemoteConfig>,
    readonly refActorMap: REF.Ref<MAP.HashMap<string, A.Actor<any>>>,
    readonly parentActor: O.Option<string>
  ) {}

  /**
   * Creates actor and registers it to dependent actor system
   *
   * @param actorName name of the actor
   * @param sup       - supervision strategy
   * @param init      - initial state
   * @param stateful  - actor's behavior description
   * @tparam S - state type
   * @tparam F - DSL type
   * @return reference to the created actor in effect that can't fail
   */
  make<R, S, F1 extends AM.AnyMessage>(
    actorName: string,
    sup: SUP.Supervisor<R>,
    stateful: A.AbstractStateful<R, S, F1>,
    init: S
  ): T.Effect<
    R & T.DefaultEnv,
    ActorAlreadyExistsException | InvalidActorName | ErrorMakingActorException,
    AR.ActorRef<F1>
  > {
    return pipe(
      T.do,
      T.bind("map", () => REF.get(this.refActorMap)),
      T.bind("finalName", (_) =>
        buildFinalName(
          O.getOrElse_(this.parentActor, () => ""),
          actorName
        )
      ),
      T.tap((_) =>
        O.fold_(
          MAP.get_(_.map, _.finalName),
          () => T.unit,
          () => T.fail(new ActorAlreadyExistsException(_.finalName))
        )
      ),
      T.let("path", (_) =>
        buildPath(this.actorSystemName, _.finalName, this.remoteConfig)
      ),
      T.let("address", (_) => _.path),
      T.let(
        "derivedSystem",
        (_) =>
          new ActorSystem(
            this.actorSystemName,
            this.remoteConfig,
            this.refActorMap,
            O.some(_.finalName)
          )
      ),
      T.bind("childrenSet", (_) => REF.makeRef(HS.make<AR.ActorRef<any>>())),
      T.bind("actor", (_) =>
        pipe(
          stateful.makeActor(
            sup,
            new Context(_.address, _.derivedSystem, _.childrenSet),
            () => this.dropFromActorMap(_.path, _.childrenSet)
          )(init),
          T.catchAll((e) => T.fail(new ErrorMakingActorException(e)))
        )
      ),
      T.tap((_) => REF.set_(this.refActorMap, MAP.set_(_.map, _.finalName, _.actor))),
      T.map((_) => new AR.ActorRefLocal(_.address, _.actor))
    )
  }

  dropFromActorMap(path: string, childrenRef: REF.Ref<HS.HashSet<AR.ActorRef<any>>>) {
    return pipe(
      T.do,
      T.bind("solvedPath", () => resolvePath(path)),
      T.let("actorName", (_) => _.solvedPath[3]),
      T.tap((_) => REF.update_(this.refActorMap, (m) => MAP.remove_(m, _.actorName))),
      T.bind("children", (_) => REF.get(childrenRef)),
      T.tap((_) => T.forEach_(_.children, (a) => a.stop)),
      T.tap((_) => REF.set_(childrenRef, HS.make())),
      T.zipRight(T.unit)
    )
  }

  /**
   * Looks up for actor on local actor system, and in case of its absence - delegates it to remote internal module.
   * If remote configuration was not provided for ActorSystem (so the remoting is disabled) the search will
   * fail with ActorNotFoundException.
   * Otherwise it will always create remote actor stub internally and return ActorRef as if it was found.   *
   *
   * @param path - absolute path to the actor
   * @tparam F - actor's DSL type
   * @return task if actor reference. Selection process might fail with "Actor not found error"
   */
  select<F1 extends AM.AnyMessage>(
    address: string
  ): T.Effect<unknown, NoSuchActorException | InvalidActorPath, AR.ActorRef<F1>> {
    return pipe(
      T.do,
      T.bind("solvedPath", (_) => resolvePath(address)),
      T.let("pathActSysName", (_) => _.solvedPath[0]),
      T.let("addr", (_) => _.solvedPath[1]),
      T.let("port", (_) => _.solvedPath[2]),
      T.let("rc", ({ addr, port }) =>
        `${addr}:${port}` === "0.0.0.0:0000"
          ? O.none
          : O.some(new RemoteConfig({ host: addr, port }))
      ),
      T.let("actorName", (_) => _.solvedPath[3]),
      T.bind("actorMap", (_) => REF.get(this.refActorMap)),
      T.chain((_) =>
        pipe(
          T.succeed(_),
          T.let("actorRef", (_) => MAP.get_(_.actorMap, _.actorName)),
          T.chain((_) =>
            O.fold_(
              _.actorRef,
              () =>
                St.equals(_.rc, this.remoteConfig)
                  ? T.fail(new NoSuchActorException(address))
                  : T.succeed(new AR.ActorRefRemote(address, this) as AR.ActorRef<F1>),
              (actor: A.Actor<F1>) =>
                T.succeed(new AR.ActorRefLocal(address, actor) as AR.ActorRef<F1>)
            )
          )
        )
      )
    )
  }

  local<F1 extends AM.AnyMessage>(
    address: string
  ): T.Effect<unknown, NoSuchActorException | InvalidActorPath, A.Actor<F1>> {
    return pipe(
      T.do,
      T.bind("solvedPath", (_) => resolvePath(address)),
      T.let("pathActSysName", (_) => _.solvedPath[0]),
      T.let("addr", (_) => _.solvedPath[1]),
      T.let("port", (_) => _.solvedPath[2]),
      T.let("rc", ({ addr, port }) =>
        `${addr}:${port}` === "0.0.0.0:0000"
          ? O.none
          : O.some(new RemoteConfig({ host: addr, port }))
      ),
      T.let("actorName", (_) => _.solvedPath[3]),
      T.bind("actorMap", (_) => REF.get(this.refActorMap)),
      T.chain((_) =>
        pipe(
          T.succeed(_),
          T.let("actorRef", (_) => MAP.get_(_.actorMap, _.actorName)),
          T.chain((_) =>
            O.fold_(
              _.actorRef,
              () => T.fail(new NoSuchActorException(address)),
              (actor: A.Actor<F1>) => T.succeed(actor)
            )
          )
        )
      )
    )
  }
}

function buildFinalName(parentActorName: string, actorName: string) {
  return actorName.length === 0
    ? T.fail(new InvalidActorName(actorName))
    : T.succeed(parentActorName + "/" + actorName)
}

function buildPath(
  actorSystemName: string,
  actorPath: string,
  remoteConfig: O.Option<RemoteConfig>
): string {
  return `zio://${actorSystemName}@${pipe(
    remoteConfig,
    O.map(({ host, port }) => `${host}:${port}`),
    O.getOrElse(() => "0.0.0.0:0000")
  )}${actorPath}`
}

const regexFullPath =
  /^(?:zio:\/\/)(\w+)[@](\d+\.\d+\.\d+\.\d+)[:](\d+)[/]([\w+|\d+|\-_.*$+:@&=,!~';.|/]+)$/i

export function resolvePath(
  path: string
): T.Effect<
  unknown,
  InvalidActorPath,
  readonly [sysName: string, host: string, port: number, actor: string]
> {
  const match = path.match(regexFullPath)
  if (match) {
    return T.succeed([match[1], match[2], parseInt(match[3], 10), "/" + match[4]])
  }
  return T.fail(new InvalidActorPath(path))
}

export function make(sysName: string, remoteConfig: O.Option<RemoteConfig> = O.none) {
  return pipe(
    T.do,
    T.bind("initActorRefMap", (_) =>
      REF.makeRef<MAP.HashMap<string, A.Actor<any>>>(MAP.make())
    ),
    T.let(
      "actorSystem",
      (_) => new ActorSystem(sysName, remoteConfig, _.initActorRefMap, O.none)
    ),
    T.map((_) => _.actorSystem)
  )
}

export const ActorSystemTag = tag<ActorSystem>()

export const LiveActorSystem = (sysName: string) =>
  L.fromEffect(ActorSystemTag)(make(sysName, O.none))
