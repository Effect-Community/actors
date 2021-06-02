import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as P from "@effect-ts/core/Effect/Promise"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as Ord from "@effect-ts/core/Ord"
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

  const monitor = yield* _(P.make<ZooError, never>())

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
      cli.on("state", (s) => {
        if (s.code === Z.State.DISCONNECTED.code) {
          T.run(
            P.fail_(
              monitor,
              new ZooError({ op: "DISCONNECTED", message: JSON.stringify(s) })
            )
          )
        }
      })
    })["|>"](M.make((cli) => T.succeedWith(() => cli.close())))
  )

  function create(
    path: string,
    opt: { data?: Buffer; mode?: keyof typeof Z.CreateMode },
    __trace?: string
  ): T.Effect<unknown, ZooError, string>
  function create(path: string, __trace?: string): T.Effect<unknown, ZooError, string>
  function create(
    path: string,
    opt?: { data?: Buffer; mode?: keyof typeof Z.CreateMode } | string,
    __trace?: string
  ): T.Effect<unknown, ZooError, string> {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      const handler = (e: Error | Z.Exception, p: string): void => {
        if (e) {
          cb(
            T.fail(new ZooError({ op: "CREATE", message: JSON.stringify(e) }), __trace)
          )
        } else {
          cb(T.succeed(p, __trace))
        }
      }
      if (typeof opt === "object") {
        if (opt && typeof opt.data !== "undefined" && typeof opt.mode !== "undefined") {
          client.create(path, opt.data, Z.CreateMode[opt.mode], handler)
        } else if (opt && typeof opt.data !== "undefined") {
          client.create(path, opt.data, handler)
        } else if (opt && typeof opt.mode !== "undefined") {
          client.create(path, Z.CreateMode[opt.mode], handler)
        } else {
          client.create(path, handler)
        }
      } else {
        client.create(path, handler)
      }
    })
  }

  function mkdir(
    path: string,
    opt: { data?: Buffer; mode?: keyof typeof Z.CreateMode },
    __trace?: string
  ): T.Effect<unknown, ZooError, string>
  function mkdir(path: string, __trace?: string): T.Effect<unknown, ZooError, string>
  function mkdir(
    path: string,
    opt?: { data?: Buffer; mode?: keyof typeof Z.CreateMode } | string,
    __trace?: string
  ): T.Effect<unknown, ZooError, string> {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      const handler = (e: Error | Z.Exception, p: string): void => {
        if (e) {
          cb(T.fail(new ZooError({ op: "MKDIR", message: JSON.stringify(e) }), __trace))
        } else {
          cb(T.succeed(p, __trace))
        }
      }
      if (typeof opt === "object") {
        if (opt && typeof opt.data !== "undefined" && typeof opt.mode !== "undefined") {
          client.mkdirp(path, opt.data, Z.CreateMode[opt.mode], handler)
        } else if (opt && typeof opt.data !== "undefined") {
          client.mkdirp(path, opt.data, handler)
        } else if (opt && typeof opt.mode !== "undefined") {
          client.mkdirp(path, Z.CreateMode[opt.mode], handler)
        } else {
          client.mkdirp(path, handler)
        }
      } else {
        client.mkdirp(path, handler)
      }
    })
  }

  function waitDelete(path: string) {
    return T.effectAsync<unknown, ZooError, string>((cb) => {
      client.exists(
        path,
        (ev) => {
          if (ev.name === "NODE_DELETED") {
            cb(T.succeed(ev.path))
          }
        },
        (e, p) => {
          if (e) {
            cb(T.fail(new ZooError({ op: "CREATE", message: JSON.stringify(e) })))
          } else {
            if (p == null) {
              cb(
                T.fail(
                  new ZooError({ op: "WAIT_DELETE", message: "path does not exist" })
                )
              )
            }
          }
        }
      )
    })
  }

  function remove(path: string) {
    return T.effectAsync<unknown, ZooError, void>((cb) => {
      client.remove(path, (e) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "REMOVE", message: JSON.stringify(e) })))
        } else {
          cb(T.unit)
        }
      })
    })
  }

  function getData(path: string) {
    return T.effectAsync<unknown, ZooError, O.Option<Buffer>>((cb) => {
      client.getData(path, (e, b) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "GET_DATA", message: JSON.stringify(e) })))
        } else {
          cb(T.succeed(O.fromNullable(b)))
        }
      })
    })
  }

  function getChildren(path: string) {
    return T.effectAsync<unknown, ZooError, Chunk.Chunk<string>>((cb) => {
      client.getChildren(path, (e, b) => {
        if (e) {
          cb(T.fail(new ZooError({ op: "GET_DATA", message: JSON.stringify(e) })))
        } else {
          cb(T.succeed(Chunk.from(b)["|>"](Chunk.sort(Ord.string))))
        }
      })
    })
  }

  return {
    [KeeperClientSym]: KeeperClientSym,
    client,
    create,
    mkdir,
    monitor: P.await(monitor),
    waitDelete,
    remove,
    getData,
    getChildren
  } as const
})

export interface KeeperClient extends _A<typeof makeKeeperClient> {}
export const KeeperClient = tag<KeeperClient>()
export const LiveKeeperClient = L.fromManaged(KeeperClient)(makeKeeperClient)
