import * as S from "@effect-ts/schema"

class ABC extends S.Model<ABC>()(S.props({ a: S.prop(S.string) })) {}
