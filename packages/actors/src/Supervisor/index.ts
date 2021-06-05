import { _R } from "@effect-ts/core/Effect"
import * as T from "@effect-ts/core/Effect"
import type { HasClock } from "@effect-ts/core/Effect/Clock"
import * as SCH from "@effect-ts/core/Effect/Schedule"

import type * as C from "../common"

export class Supervisor<R, E> {
  readonly [_R]: (r: R) => void

  constructor(
    readonly supervise: <R0, A>(
      zio: T.Effect<R0, E, A>,
      error: E
    ) => T.Effect<R & R0 & HasClock, void, A>
  ) {}
}

export const none: Supervisor<unknown, any> = retry(SCH.once)

export function retry<R, E, A>(policy: SCH.Schedule<R, E, A>): Supervisor<R, E> {
  return retryOrElse(policy, (_, __) => T.unit)
}

export function retryOrElse<R, E, A>(
  policy: SCH.Schedule<R, E, A>,
  orElse: (e: E, a: A) => T.Effect<R, C.Throwable, void>
): Supervisor<R, E> {
  return new Supervisor<R, E>((zio, error) =>
    T.mapError_(
      T.retryOrElse_(zio, policy, (e, a) => T.zipRight_(orElse(e, a), T.fail(error))),
      () => undefined
    )
  )
}
