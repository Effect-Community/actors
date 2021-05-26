import { _R } from "@effect-ts/core/Effect";
import * as T from "@effect-ts/core/Effect";
import * as SCH from "@effect-ts/core/Effect/Schedule";
import { HasClock } from "@effect-ts/core/Effect/Clock";
import * as C from "./common"

export class Supervisor<R> {
  readonly [_R]: (r: R) => void;

  constructor(
    readonly supervise: <R0, A>(
      zio: T.RIO<R0, A>,
      error: C.Throwable
    ) => T.Effect<R & R0 & HasClock, void, A>
  ) {}
}

export const none: Supervisor<unknown> = retry(SCH.once);

export function retry<R, A>(
  policy: SCH.Schedule<R, C.Throwable, A>
): Supervisor<R> {
  return retryOrElse(policy, (_, __) => T.unit);
}

export function retryOrElse<R, A>(
  policy: SCH.Schedule<R, C.Throwable, A>,
  orElse: (e: C.Throwable, a: A) => T.RIO<R, void>
): Supervisor<R> {
  return new Supervisor<R>((zio, error) =>
    T.mapError_(
      T.retryOrElse_(zio, policy, (e, a) =>
        T.zipRight_(orElse(e, a), T.fail(error))
      ),
      () => {}
    )
  );
}
