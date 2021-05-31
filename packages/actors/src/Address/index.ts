import * as C from "@effect-ts/core/Case"

import type * as AM from "../Message"

export class Address<F1 extends AM.AnyMessage> extends C.Case<{
  path: string
  messages: AM.MessageRegistry<F1>
}> {}

export function address<F1 extends AM.AnyMessage>(
  path: string,
  messages: AM.MessageRegistry<F1>
) {
  return new Address({ path, messages })
}
