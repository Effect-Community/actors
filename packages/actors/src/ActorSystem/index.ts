import * as HS from "@effect-ts/core/Collections/Immutable/HashSet"
import * as MAP from "@effect-ts/core/Collections/Immutable/Map"
import * as T from "@effect-ts/core/Effect"
import * as REF from "@effect-ts/core/Effect/Ref"
import * as O from "@effect-ts/core/Option"
import type { HasClock } from "@effect-ts/system/Clock"
import { pipe } from "@effect-ts/system/Function"

import type * as A from "../Actor"
import * as AR from "../ActorRef"
import * as AA from "../Address"
import {
  ActorAlreadyExistsException,
  ErrorMakingActorException,
  InvalidActorName,
  InvalidActorPath,
  NoRemoteSupportException,
  NoSuchActorException
} from "../common"
import type * as AM from "../Message"
import type * as SUP from "../Supervisor"

/**
 * Context for actor used inside Stateful which provides self actor reference and actor creation/selection API
 */
export class Context<FC extends AM.AnyMessage> {
  constructor(
    readonly address: AA.Address<FC>,
    readonly actorSystem: ActorSystem,
    readonly childrenRef: REF.Ref<HS.HashSet<AR.ActorRef<any>>>
  ) {}
  /**
   * Accessor for self actor reference
   *
   * @return actor reference in a task
   */
  self = this.actorSystem.select(this.address)
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
    init: S,
    stateful: A.Stateful<R, S, F1>
  ): T.Effect<
    R & HasClock,
    ActorAlreadyExistsException | InvalidActorName | ErrorMakingActorException,
    AR.ActorRef<F1>
  > {
    return pipe(
      T.do,
      T.bind("actorRef", () => this.actorSystem.make(actorName, sup, init, stateful)),
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
  select<F1 extends AM.AnyMessage>(address: AA.Address<F1>) {
    return this.actorSystem.select<F1>(address)
  }

  lookup<F1 extends AM.AnyMessage>(address: AA.Address<F1>) {
    return this.actorSystem.lookup<F1>(address)
  }
}

type RemoteConfig = {}

export class ActorSystem {
  constructor(
    readonly actorSystemName: string,
    readonly config: O.Option<string>,
    readonly remoteConfig: O.Option<RemoteConfig>,
    readonly refActorMap: REF.Ref<MAP.Map<string, A.Actor<any>>>,
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
    init: S,
    stateful: A.AbstractStateful<R, S, F1>
  ): T.Effect<
    R & HasClock,
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
          MAP.lookup_(_.map, _.finalName),
          () => T.unit,
          () => T.fail(new ActorAlreadyExistsException(_.finalName))
        )
      ),
      T.let("path", (_) =>
        buildPath(this.actorSystemName, _.finalName, this.remoteConfig)
      ),
      T.let("address", (_) => AA.address(_.path, stateful.messages)),
      T.let(
        "derivedSystem",
        (_) =>
          new ActorSystem(
            this.actorSystemName,
            this.config,
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
      T.tap((_) =>
        REF.set_(this.refActorMap, MAP.insert_(_.map, _.finalName, _.actor))
      ),
      T.map((_) => new AR.ActorRefLocal(_.address, _.actor))
    )
  }

  selectOrMake<R, S, F1 extends AM.AnyMessage>(
    actorName: string,
    sup: SUP.Supervisor<R>,
    init: S,
    stateful: A.AbstractStateful<R, S, F1>
  ): T.Effect<
    R & HasClock,
    | InvalidActorPath
    | InvalidActorName
    | ActorAlreadyExistsException
    | ErrorMakingActorException,
    AR.ActorRef<F1>
  > {
    return pipe(
      T.do,
      T.bind("finalName", (_) =>
        buildFinalName(
          O.getOrElse_(this.parentActor, () => ""),
          actorName
        )
      ),
      T.let("path", (_) =>
        buildPath(this.actorSystemName, _.finalName, this.remoteConfig)
      ),
      T.chain((_) => this.lookup(AA.address(_.path, stateful.messages))),
      T.chain(O.fold(() => this.make(actorName, sup, init, stateful), T.succeed))
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
    address: AA.Address<F1>
  ): T.Effect<unknown, NoSuchActorException | InvalidActorPath, AR.ActorRef<F1>> {
    return pipe(
      this.lookup(address),
      T.chain(O.fold(() => T.fail(new NoSuchActorException(address.path)), T.succeed))
    )
  }

  lookup<F1 extends AM.AnyMessage>(
    address: AA.Address<F1>
  ): T.Effect<unknown, InvalidActorPath, O.Option<AR.ActorRef<F1>>> {
    return pipe(
      T.do,
      T.bind("solvedPath", (_) => resolvePath(address.path)),
      T.let("pathActSysName", (_) => _.solvedPath[0]),
      T.let("addr", (_) => _.solvedPath[1]),
      T.let("port", (_) => _.solvedPath[2]),
      T.let("actorName", (_) => _.solvedPath[3]),
      T.bind("actorMap", (_) => REF.get(this.refActorMap)),
      T.chain((_) => {
        if (_.pathActSysName === this.actorSystemName) {
          return pipe(
            T.succeed(_),
            T.let("actorRef", (_) => MAP.lookup_(_.actorMap, _.actorName)),
            T.chain((_) =>
              O.fold_(
                _.actorRef,
                () => T.succeed(O.none),
                (actor: A.Actor<F1>) =>
                  T.succeed(O.some(new AR.ActorRefLocal(address, actor)))
              )
            )
          )
        } else {
          return T.die(new NoRemoteSupportException())
        }
      })
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
  const host = pipe(
    remoteConfig,
    //O.map(c => c.addr.value + ":" + c.port.value),
    O.map((_) => "0.0.0.0:0000"),
    O.getOrElse(() => "0.0.0.0:0000")
  )
  return `zio://${actorSystemName}@${host}${actorPath}`
}

const regexFullPath =
  /^(?:zio:\/\/)(\w+)[@](\d+\.\d+\.\d+\.\d+)[:](\d+)[/]([\w+|\d+|\-_.*$+:@&=,!~';.|/]+)$/i
function resolvePath(
  path: string
): T.Effect<unknown, InvalidActorPath, readonly [string, number, number, string]> {
  const match = path.match(regexFullPath)
  if (match) {
    return T.succeed([
      match[1],
      parseInt(match[2], 10),
      parseInt(match[3], 10),
      "/" + match[4]
    ])
  }
  return T.fail(new InvalidActorPath(path))
}

export function make(sysName: string, configFile: O.Option<string>) {
  return pipe(
    T.do,
    T.bind("initActorRefMap", (_) =>
      REF.makeRef<MAP.Map<string, A.Actor<any>>>(MAP.empty)
    ),
    T.let(
      "actorSystem",
      (_) => new ActorSystem(sysName, O.none, O.none, _.initActorRefMap, O.none)
    ),
    T.map((_) => _.actorSystem)
  )
}
