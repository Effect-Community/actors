
class Ask<A> {
    public readonly _tag = "Ask"
    constructor(
        readonly msg: A
    ){}
}
class Tell<A> {
    public readonly _tag = "Tell"
    constructor(
        readonly msg: A
    ){}
}
class Stop {
    public readonly _tag = "Stop"
}

export type Command = Ask<any> | Tell<any> | Stop

export class Envelope {
    constructor(
        readonly command: Command,
        readonly recipient: string
    ){}
}