import { Tagged } from "@effect-ts/core/Case"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { tag } from "@effect-ts/core/Has"
import type { _A } from "@effect-ts/system/Utils"
import * as Z from "node-zookeeper-client"

import { KeeperConfig } from "./config"

export class ZooError extends Tagged("ZooError")<{
  readonly op: string
  readonly message: string
}> {}

export const KeeperClientSym = Symbol()

export const makeKeeperClient = M.gen(function* (_) {
  const { connectionString, options } = yield* _(KeeperConfig)

  const client = yield* _(
    T.effectAsync<unknown, ZooError, Z.Client>((cb) => {
      const cli = Z.createClient(connectionString, options)

      cli.once("state", (state) => {
        if (state.code === Z.State.SYNC_CONNECTED.code) {
          cb(T.succeed(cli))
        } else {
          cb(T.fail(new ZooError({ op: "CONNECT", message: JSON.stringify(state) })))
        }
      })

      cli.connect()
    })["|>"](M.make((cli) => T.succeedWith(() => cli.close())))
  )

  function create(path: string) {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      client.create(path, (e, p) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "CREATE", message: JSON.stringify(e) })))
        } else {
          cb(T.succeed(p))
        }
      })
    })
  }

  function createWithData(path: string, data: Buffer) {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      client.create(path, data, (e, p) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "CREATE", message: JSON.stringify(e) })))
        } else {
          cb(T.succeed(p))
        }
      })
    })
  }

  function mkdir(path: string) {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      client.mkdirp(path, (e, p) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "CREATE", message: JSON.stringify(e) })))
        } else {
          cb(T.succeed(p))
        }
      })
    })
  }

  function mkdirWithData(path: string, data: Buffer) {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      client.mkdirp(path, data, (e, p) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "CREATE", message: JSON.stringify(e) })))
        } else {
          cb(T.succeed(p))
        }
      })
    })
  }

  return {
    [KeeperClientSym]: KeeperClientSym,
    client,
    create,
    createWithData,
    mkdir,
    mkdirWithData
  } as const
})

export interface KeeperClient extends _A<typeof makeKeeperClient> {}
export const KeeperClient = tag<KeeperClient>()
export const LiveKeeperClient = L.fromManaged(KeeperClient)(makeKeeperClient)
