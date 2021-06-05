import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as OT from "@effect-ts/core/OptionT"
import { chainF } from "@effect-ts/core/Prelude"
import type { _A } from "@effect-ts/core/Utils"
import * as K from "@effect-ts/keeper"

import * as AS from "../ActorSystem"

export const EO = pipe(OT.monad(T.Monad), (M) => ({
  map: M.map,
  chain: chainF(M),
  chainT:
    <R2, E2, A, B>(f: (a: A) => T.Effect<R2, E2, B>) =>
    <R, E>(fa: T.Effect<R, E, O.Option<A>>): T.Effect<R2 & R, E2 | E, O.Option<B>> =>
      chainF(M)((a: A) => T.map_(f(a), O.some))(fa)
}))

export const ClusterSym = Symbol()

export class HostPort extends Tagged("HostPort")<{
  readonly host: string
  readonly port: number
}> {}

export class ClusterException extends Tagged("ClusterException")<{
  readonly message: string
}> {}

export class ActorError extends Tagged("ActorError")<{
  readonly message: string
}> {}

export interface MemberIdBrand {
  readonly MemberIdBrand: unique symbol
}

export type MemberId = string & MemberIdBrand

export const makeCluster = M.gen(function* (_) {
  const cli = yield* _(K.KeeperClient)
  const system = yield* _(AS.ActorSystemTag)
  const clusterDir = `/cluster/${system.actorSystemName}`
  const membersDir = `${clusterDir}/members`

  if (O.isNone(system.remoteConfig)) {
    return yield* _(
      T.die(`actor system ${system.actorSystemName} doesn't support remoting`)
    )
  }

  yield* _(cli.mkdir(membersDir))

  const prefix = `${membersDir}/member_`

  const nodePath = yield* _(
    cli
      .create(prefix, {
        mode: "EPHEMERAL_SEQUENTIAL",
        data: Buffer.from(
          JSON.stringify({
            host: system.remoteConfig.value.host,
            port: system.remoteConfig.value.port
          })
        )
      })
      ["|>"](M.make((p) => cli.remove(p)["|>"](T.orDie)))
  )

  const nodeId = `member_${nodePath.substr(prefix.length)}` as MemberId

  const memberHostPort = yield* _(
    T.memoize(
      (member: MemberId): T.Effect<unknown, K.ZooError, HostPort> =>
        pipe(
          cli.getData(`${membersDir}/${member}`),
          T.chain(
            O.fold(
              () =>
                T.die(
                  new ClusterException({
                    message: `cannot find metadata on path: ${membersDir}/${member}`
                  })
                ),
              (b) => T.succeed(b)
            )
          ),
          T.map((b) => new HostPort(JSON.parse(b.toString("utf8"))))
        )
    )
  )

  const members = pipe(
    cli.getChildren(membersDir),
    T.chain(T.forEach((x) => memberHostPort(x as MemberId)))
  )

  const init = (scope: string) => cli.mkdir(`${clusterDir}/elections/${scope}`)

  const join = (scope: string) =>
    cli.create(`${clusterDir}/elections/${scope}/w_`, {
      mode: "EPHEMERAL_SEQUENTIAL",
      data: Buffer.from(nodeId)
    })

  const leave = (nodePath: string) => cli.remove(nodePath)

  const leaderPath = (scope: string) =>
    pipe(cli.getChildren(`${clusterDir}/elections/${scope}`), T.map(Chunk.head))

  const leaderId = (scope: string) =>
    pipe(
      leaderPath(scope),
      EO.chain((s) => cli.getData(`${clusterDir}/elections/${scope}/${s}`)),
      EO.map((b) => b.toString("utf-8") as MemberId)
    )

  const runOnLeader =
    (scope: string) =>
    <R, E, R2, E2>(
      onLeader: T.Effect<R, E, never>,
      whileFollower: (leader: MemberId) => T.Effect<R2, E2, never>
    ): T.Effect<R & R2, K.ZooError | E | E2, never> => {
      return T.gen(function* (_) {
        while (1) {
          const leader = yield* _(leaderId(scope))
          if (O.isNone(leader)) {
            return yield* _(T.die("cannot find a leader"))
          }
          if (leader.value === nodeId) {
            yield* _(onLeader)
          } else {
            yield* _(T.race_(watchLeader(scope), whileFollower(leader.value)))
          }
        }
        return yield* _(T.never)
      })
    }

  const watchMember = (member: MemberId) => cli.waitDelete(`${membersDir}/${member}`)

  const watchLeader = (scope: string) =>
    pipe(
      leaderPath(scope),
      T.chain((o) =>
        O.isNone(o)
          ? T.die("cannot find a leader")
          : cli.waitDelete(`${clusterDir}/elections/${scope}/${o.value}`)
      )
    )

  return {
    [ClusterSym]: ClusterSym,
    members,
    init,
    join,
    leave,
    nodeId,
    memberHostPort,
    leaderId,
    runOnLeader,
    watchLeader,
    watchMember
  } as const
})

export interface Cluster extends _A<typeof makeCluster> {}
export const Cluster = tag<Cluster>()
export const LiveCluster = L.fromManaged(Cluster)(makeCluster)
