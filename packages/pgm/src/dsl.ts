/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* eslint-disable no-constant-condition */
/* eslint-disable no-unused-labels */
import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import type { Dictionary } from "@effect-ts/core/Collections/Immutable/Dictionary"
import * as T from "@effect-ts/core/Effect"
import type { Layer } from "@effect-ts/core/Effect/Layer"
import { absurd } from "@effect-ts/core/Function"
import type { Has } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import type { IsEqualTo, UnionToIntersection } from "@effect-ts/core/Utils"
import type { PG } from "@effect-ts/pg"
import { query } from "@effect-ts/pg"

import type { Migration } from "./migrations"
import { migrate, migration, Migrations } from "./migrations"

export type Db<X> = Named<X> | Table<X> | AddColumns<X> | FixVersion<X>

export class FixVersion<X> extends Tagged("FixVersion")<{
  readonly parent: Db<any>
  readonly version: string
}> {
  readonly _X!: () => X
}

export type MigrationsFrom<X> = X extends { Migrations: infer V }
  ? V extends any[]
    ? V
    : []
  : []

export type MergeColumns<X> = {
  [k in keyof X]: X[k]
} extends infer A
  ? A
  : never

export type MergeTables<X> = {
  [k in keyof X]: {
    [h in keyof X[k]]: h extends "Columns" ? MergeColumns<X[k][h]> : X[k][h]
  }
} extends infer A
  ? A
  : never

export type MergeHistory<X> = {
  [k in keyof X]: {
    [h in keyof X[k]]: X[k][h]
  }
} extends infer A
  ? A
  : never

export type Merge<X, Y> = {
  [k in keyof (X & Y)]: k extends "Tables"
    ? MergeTables<(X & Y)[k]>
    : k extends "History"
    ? MergeTables<(X & Y)[k]>
    : k extends "Migrations"
    ? [...MigrationsFrom<X>, ...MigrationsFrom<Y>]
    : (X & Y)[k]
} extends infer X
  ? X
  : never

export class Named<X> extends Tagged("Named")<{
  readonly name: string
}> {
  readonly _X!: () => X
}

export function db<K extends string>(name: K): Db<{ Db: K; Migrations: [] }> {
  return new Named({ name })
}

export class Table<X> extends Tagged("Table")<{
  readonly name: string
  readonly parent: Db<any>
  readonly columns: Dictionary<Column<any, any, any>>
  readonly alias?: string | undefined
}> {
  readonly _X!: () => X
}

export function createTable<
  K extends string,
  Columns extends { readonly [k in keyof Columns]: Column<any, any, any> },
  A extends string
>(
  name: K,
  columns: Columns,
  alias: A
): <X>(self: Db<X>) => Db<
  Merge<
    X,
    {
      Tables: { [N in K]: { Name: A; Columns: Columns } }
      Migrations: [`CreateTable(${K}, ${A})`]
      History: {
        [k in MigrationsFrom<X>["length"] as `v${k}`]: {
          Migrations: X extends { Migrations: infer A } ? A : never
          Tables: X extends { Tables: infer A } ? A : {}
        }
      }
    }
  >
>
export function createTable<
  K extends string,
  Columns extends { readonly [k in keyof Columns]: Column<any, any, any> }
>(
  name: K,
  columns: Columns
): <X>(self: Db<X>) => Db<
  Merge<
    X,
    {
      Migrations: [`CreateTable(${K})`]
      Tables: { [N in K]: { Name: K; Columns: Columns } }
      History: {
        [k in MigrationsFrom<X>["length"] as `v${k}`]: {
          Migrations: X extends { Migrations: infer A } ? A : never
          Tables: X extends { Tables: infer A } ? A : {}
        }
      }
    }
  >
>
export function createTable<
  K extends string,
  Columns extends { readonly [k in keyof Columns]: Column<any, any, any> },
  A extends string
>(name: K, columns: Columns, alias?: A): <X>(self: Db<X>) => Db<any> {
  return (parent) => new Table({ name, parent, columns, alias })
}

export class AddColumns<X> extends Tagged("AddColumns")<{
  readonly name: string
  readonly parent: Db<any>
  readonly columns: Dictionary<Column<any, any, any>>
  readonly defaults: Dictionary<any>
}> {
  readonly _X!: () => X
}

export function columnsToAdd<
  Columns extends { readonly [k in keyof Columns]: Column<any, any, any> }
>(_: {
  columns: Columns
  defaults: UnionToIntersection<
    {
      [k in keyof Columns]: Columns[k]["nullable"] extends true
        ? never
        : { [h in k]: TypeFromColumnType<Columns[k]["type"]> }
    }[keyof Columns]
  >
}): {
  columns: Columns
  defaults: UnionToIntersection<
    {
      [k in keyof Columns]: Columns[k]["nullable"] extends true
        ? never
        : { [h in k]: TypeFromColumnType<Columns[k]["type"]> }
    }[keyof Columns]
  >
} {
  return _
}

type TablesFrom<X> = [X] extends [
  {
    Tables: infer V
  }
]
  ? V
  : never

export function alterTableAddColumns<
  K extends keyof TablesFrom<X> & string,
  X,
  Columns extends { readonly [k in keyof Columns]: Column<any, any, any> }
>(
  name: K,
  _: {
    columns: Columns
    defaults: UnionToIntersection<
      {
        [k in keyof Columns]: Columns[k]["nullable"] extends true
          ? never
          : { [h in k]: TypeFromColumnType<Columns[k]["type"]> }
      }[keyof Columns]
    >
  }
): (self: Db<X>) => Db<
  Merge<
    X,
    {
      Migrations: [`AlterTable(${K})`]
      Tables: { [N in K]: { Columns: Columns } }
      History: {
        [k in MigrationsFrom<X>["length"] as `v${k}`]: {
          Migrations: X extends { Migrations: infer A } ? A : never
          Tables: X extends { Tables: infer A } ? A : {}
        }
      }
    }
  >
> {
  return (parent) =>
    // @ts-expect-error
    new AddColumns({ name, parent, columns: _.columns, defaults: _.defaults })
}

export interface ColumnType {
  readonly TsType: unknown
  readonly DbType: string
}

export type TypeFromColumnType<X> = X extends { TsType: infer A } ? A : never

export interface Column<
  X extends ColumnType,
  K extends O.Option<string>,
  Nullable extends boolean
> {
  type: X
  alias: K
  nullable: Nullable
}

export function column<
  X extends ColumnType,
  K extends string | undefined,
  Nullable extends boolean
>(_: {
  readonly type: X
  readonly alias?: K
  readonly nullable?: Nullable
}): Column<
  X,
  [K] extends [string] ? O.Some<K> : O.None,
  [Nullable] extends [true] ? true : false
> {
  return {
    type: _.type,
    // @ts-expect-error
    alias: O.fromNullable(_.alias),
    // @ts-expect-error
    nullable: _.nullable ?? false
  }
}

export class Serial implements ColumnType {
  readonly _tag = "Serial"

  readonly TsType!: number
  readonly DbType = "serial"

  static of = () => new Serial()
}

export class Integer implements ColumnType {
  readonly _tag = "Integer"

  readonly TsType!: number
  readonly DbType = "integer"

  static of = () => new Integer()
}

export class VarChar<N extends number> implements ColumnType {
  readonly _tag = "VarChar"

  readonly TsType!: string
  readonly DbType = `varchar(${this.length})`

  static of = <N extends number>(length: N) => new VarChar<N>(length)

  readonly _N!: () => N

  constructor(readonly length: number) {}
}

export class Literal<K extends string> implements ColumnType {
  readonly _tag = "Literal"

  readonly TsType!: K
  readonly DbType = `varchar(${this.values
    .map((_) => _.length)
    .reduce((x, y) => (x > y ? x : y))})`

  static of = <K extends readonly string[]>(...values: K) =>
    new Literal<K[number]>(values)

  readonly _K!: () => K

  constructor(readonly values: readonly string[]) {}
}

export type VersionsFromDb<X> = {
  [k in keyof [{}, ...MigrationsFrom<X>]]: k extends `${number}` ? k : never
}[keyof [{}, ...MigrationsFrom<X>]] extends infer A
  ? A extends string
    ? A
    : never
  : never

export function useDbVersion<X, V extends VersionsFromDb<X> & string>(
  version: V
): (parent: Db<X>) => Db<Merge<{ UseVersion: `v${V}` }, Omit<X, "UseVersion">>> {
  return (parent) => new FixVersion({ parent, version })
}

function migrationCreateTable<X>(createTableOp: Table<X>) {
  const columns = Object.keys(createTableOp.columns)
  const tableName = createTableOp.alias ?? createTableOp.name
  const pgColumns: string[] = []

  for (const c of columns) {
    const columnName = O.getOrElse_(createTableOp.columns[c].alias, () => c) as string
    const columnType = <ColumnType>createTableOp.columns[c].type

    const columnTypePg = columnType["DbType"]

    pgColumns.push(
      `${columnName} ${columnTypePg}` +
        (createTableOp.columns[c].nullable ? `` : ` NOT NULL`)
    )
  }

  const up = `CREATE TABLE "${tableName}" (
    ${pgColumns.join(",\n    ")}
  );`

  const down = `DROP TABLE "${tableName}";`

  return migration(query(up), query(down))
}

function migrationAddColumns<X>(
  addColumnsOp: AddColumns<X>,
  tableNameMap: Record<string, string>
) {
  const columns = Object.keys(addColumnsOp.columns)
  const tableName = tableNameMap[addColumnsOp.name]
  const upQueries: [string, any[]][] = []
  const downQueries: string[] = []

  for (const c of columns) {
    const columnName = O.getOrElse_(addColumnsOp.columns[c].alias, () => c) as string
    const columnTypePg = (addColumnsOp.columns[c].type as ColumnType)["DbType"]

    upQueries.push([
      `ALTER TABLE "${tableName}" ADD COLUMN "${columnName}" ${columnTypePg};`,
      []
    ])

    if (!addColumnsOp.columns[c].nullable) {
      upQueries.push([
        `UPDATE "${tableName}" SET "${columnName}" = $1::${columnTypePg};`,
        [addColumnsOp.defaults[c]]
      ])
      upQueries.push([
        `ALTER TABLE "${tableName}" ALTER COLUMN "${columnName}" SET NOT NULL;`,
        []
      ])
    }

    downQueries.push(
      `ALTER TABLE "${tableName}" DROP COLUMN "${columnName}" ${columnTypePg};`
    )
  }

  return migration(
    T.forEachUnit_(upQueries, ([q, a]) => query(q, a)),
    T.forEachUnit_(downQueries, query)
  )
}

export function migrationsFor<X>(db: Db<X>): Layer<Has<PG>, never, Has<PG>> {
  const frames: Db<X>[] = []
  const migrationsBuilder = Chunk.builder<Migration<unknown>>()
  const tableNameMap = {} as Record<string, string>

  pushing: while (db) {
    switch (db._tag) {
      case "Named": {
        frames.push(db)
        break pushing
      }
      case "AddColumns": {
        frames.push(db)
        db = db.parent
        continue pushing
      }
      case "Table": {
        frames.push(db)
        db = db.parent
        continue pushing
      }
      case "FixVersion": {
        frames.push(db)
        db = db.parent
        continue pushing
      }
    }
  }

  let id: string
  let cv = 0
  let v: string | undefined

  popping: while (frames.length > 0) {
    const frame = frames.pop()!
    switch (frame._tag) {
      case "Named": {
        id = frame.name
        continue popping
      }
      case "AddColumns": {
        migrationsBuilder.append(migrationAddColumns(frame, tableNameMap))
        cv++
        continue popping
      }
      case "Table": {
        tableNameMap[frame.name] = frame.alias ?? frame.name
        migrationsBuilder.append(migrationCreateTable(frame))
        cv++
        continue popping
      }
      case "FixVersion": {
        v = frame.version
        continue popping
      }
    }
  }

  if (typeof v === "undefined") {
    v = String(cv)
  }

  return migrate(new Migrations(migrationsBuilder.build()), id!, v)
}

interface AnyTable {
  readonly name: string
  readonly alias: string
  readonly columns: Record<
    string,
    {
      readonly type: ColumnType
      readonly alias: O.Option<string>
      readonly nullable: boolean
    }
  >
}

export function tablesFor<X>(db: Db<X>): Record<string, AnyTable> {
  const frames: Db<X>[] = []
  const tables = {} as Record<string, AnyTable>
  let version: string | undefined

  pushing: while (db) {
    switch (db._tag) {
      case "Named": {
        frames.push(db)
        break pushing
      }
      case "AddColumns": {
        frames.push(db)
        db = db.parent
        continue pushing
      }
      case "Table": {
        frames.push(db)
        db = db.parent
        continue pushing
      }
      case "FixVersion": {
        frames.push(db)
        if (!version) {
          version = db.version
        }
        db = db.parent
        continue pushing
      }
    }
  }

  let v = 0

  popping: while (frames.length > 0) {
    if (`${v}` === version) {
      return tables
    }
    const frame = frames.pop()!
    switch (frame._tag) {
      case "Named": {
        continue popping
      }
      case "AddColumns": {
        tables[frame.name] = {
          ...tables[frame.name],
          columns: { ...tables[frame.name].columns, ...frame.columns }
        }
        v++
        continue popping
      }
      case "Table": {
        tables[frame.name] = {
          name: frame.name,
          alias: frame.alias ?? frame.name,
          columns: frame.columns
        }
        v++
        continue popping
      }
      case "FixVersion": {
        continue popping
      }
    }
  }

  return tables
}

export type SelectConfig<X, K extends keyof X> = {
  columns?: readonly (keyof SQLColumns<X, K>)[]
}

export type SelectedColumns<X, K extends keyof X, C> = [C] extends [
  { columns: readonly (keyof SQLColumns<X, K>)[] }
]
  ? C["columns"][number]
  : keyof SQLColumns<X, K>

const TableSym = Symbol()
const ResponseSym = Symbol()
const ParSym = Symbol()
const QueryTablesSym = Symbol()

export interface Select<Tables, QueryTables, Params, RowType> {
  readonly [TableSym]: Tables
  readonly [ResponseSym]: RowType
  readonly [ParSym]: Params
  readonly [QueryTablesSym]: QueryTables

  readonly pick: {
    <Ks extends readonly (keyof RowType)[]>(...ks: Ks): Select<
      Tables,
      QueryTables,
      Params,
      Flat<Pick<RowType, Ks[number]>>
    >
  }

  readonly tablePick: {
    <
      Scope extends keyof RowType & `${string}.${string}` extends `${infer S}.${string}`
        ? S
        : never,
      Ks extends readonly (keyof RowType & `${Scope}.${string}`)[]
    >(
      scope: Scope,
      ...ks: Ks
    ): Select<
      Tables,
      QueryTables,
      Params,
      Flat<Omit<RowType, `${Scope}.${string}`> & Pick<RowType, Ks[number]>>
    >
  }

  readonly omit: <Ks extends readonly (keyof RowType)[]>(
    ...ks: Ks
  ) => Select<Tables, QueryTables, Params, Flat<Omit<RowType, Ks[number]>>>

  readonly as: <From extends keyof RowType, To extends string>(
    f: From,
    t: To
  ) => Select<
    Tables,
    QueryTables,
    Params,
    {
      readonly [k in keyof RowType & string as k extends From ? To : k]: RowType[k]
    }
  >

  readonly join: <Table extends keyof Tables, As extends string>(
    t: Table,
    as: As
  ) => Select<
    Tables,
    Flat<QueryTables & { [k in Table as As]: Tables[k] }>,
    Params,
    Flat<
      RowType &
        {
          readonly [k in keyof TypeFromColumns<SQLColumns<Tables, Table>> &
            string as `${As}.${k}`]: TypeFromColumns<SQLColumns<Tables, Table>>[k]
        }
    >
  >

  readonly param: {
    // custom
    <P extends string, V extends ColumnType>(p: P, t: V): Select<
      Tables,
      QueryTables,
      Flat<Params & { readonly [p in P]: V }>,
      RowType
    >
    // custom optional
    <P extends string, V extends ColumnType>(p: P, t: V, o: "optional"): Select<
      Tables,
      QueryTables,
      Flat<Params & { readonly [p in P]?: V }>,
      RowType
    >
    // from table column
    <
      P extends string,
      K extends {
        [k in keyof QueryTables]: {
          [h in keyof SQLColumns<QueryTables, k>]: h extends string
            ? k extends string
              ? `${k}.${h}`
              : never
            : never
        }[keyof SQLColumns<QueryTables, k>]
      }[keyof QueryTables]
    >(
      p: P,
      t: K
    ): Select<
      Tables,
      QueryTables,
      Flat<
        Params &
          {
            [k in keyof QueryTables]: {
              [h in keyof SQLColumns<QueryTables, k>]: h extends string
                ? k extends string
                  ? `${k}.${h}` extends K
                    ? [SQLColumns<QueryTables, k>[h]] extends [
                        Column<infer A, any, infer N>
                      ]
                      ? [N] extends [true]
                        ? {
                            readonly [p in P]?: A
                          }
                        : {
                            readonly [p in P]: A
                          }
                      : never
                    : never
                  : never
                : never
            }[keyof SQLColumns<QueryTables, k>]
          }[keyof QueryTables]
      >,
      RowType
    >
  }

  readonly prepare: () => IsEqualTo<Params, {}> extends true
    ? () => T.UIO<Chunk.Chunk<RowType>>
    : (
        _: { readonly [p in keyof Params]: TypeFromColumnType<Params[p]> }
      ) => T.UIO<Chunk.Chunk<RowType>>

  readonly run: IsEqualTo<Params, {}> extends true
    ? () => T.UIO<Chunk.Chunk<RowType>>
    : (
        _: { readonly [p in keyof Params]: TypeFromColumnType<Params[p]> }
      ) => T.UIO<Chunk.Chunk<RowType>>

  readonly where: (
    f: (_: Where<this>) => Where<this>
  ) => Select<Tables, QueryTables, Params, RowType>
}

type Comp<X> = X extends infer Y ? Y : never

type Targets<X extends Select<any, any, any, any>, F = unknown> = Comp<
  | {
      [k in keyof X[typeof QueryTablesSym]]: {
        [h in keyof X[typeof QueryTablesSym][k]["Columns"]]: k extends string
          ? h extends string
            ? TypeFromColumnType<
                X[typeof QueryTablesSym][k]["Columns"][h]["type"]
              > extends F
              ? `${k}.${h}`
              : never
            : never
          : never
      }[keyof X[typeof QueryTablesSym][k]["Columns"]]
    }[keyof X[typeof QueryTablesSym]]
  | {
      [k in keyof X[typeof ResponseSym]]: X[typeof ResponseSym][k] extends F ? k : never
    }[keyof X[typeof ResponseSym]]
>

type TypeOfTarget<X extends Select<any, any, any, any>, F extends Targets<X>> = Comp<
  | {
      [k in keyof X[typeof QueryTablesSym]]: {
        [h in keyof X[typeof QueryTablesSym][k]["Columns"]]: k extends string
          ? h extends string
            ? `${k}.${h}` extends F
              ? TypeFromColumnType<X[typeof QueryTablesSym][k]["Columns"][h]["type"]>
              : never
            : never
          : never
      }[keyof X[typeof QueryTablesSym][k]["Columns"]]
    }[keyof X[typeof QueryTablesSym]]
  | {
      [k in keyof X[typeof ResponseSym]]: k extends F ? X[typeof ResponseSym][k] : never
    }[keyof X[typeof ResponseSym]]
>

export interface Where<X extends Select<any, any, any, any>> {
  readonly ["&&"]: (_: Where<X>) => Where<X>
  readonly ["||"]: (_: Where<X>) => Where<X>

  readonly eq: <
    K extends Targets<X>,
    H extends
      | Targets<X, TypeOfTarget<X, K>>
      | {
          param: {
            [p in keyof X[typeof ParSym]]: TypeFromColumnType<
              X[typeof ParSym][p]
            > extends TypeOfTarget<X, K>
              ? p
              : never
          }[keyof X[typeof ParSym]]
        }
  >(
    k: K,
    h: H
  ) => Where<X>
}

export interface SQLFor<X> {
  readonly select: {
    <K extends keyof X>(table: K): Select<
      X,
      { [k in K]: X[k] },
      {},
      TypeFromColumns<SQLColumns<X, K>>
    >
    <K extends keyof X, As extends string>(table: K, as: As): Select<
      X,
      { [k in K as As]: X[k] },
      {},
      {
        readonly [k in keyof TypeFromColumns<SQLColumns<X, K>> &
          string as `${As}.${k}`]: TypeFromColumns<SQLColumns<X, K>>[k]
      }
    >
  }
}

export type TypeFromColumns<A> = Flat<
  UnionToIntersection<
    {
      [k in keyof A]: [A[k]] extends [Column<any, any, any>]
        ? A[k]["nullable"] extends true
          ? {
              [h in k]?: TypeFromColumnType<A[k]["type"]>
            }
          : {
              [h in k]: TypeFromColumnType<A[k]["type"]>
            }
        : never
    }[keyof A]
  >
>

export type Flat<X> = { readonly [k in keyof X]: X[k] } extends infer XX ? XX : never

export type SQLColumns<X, K extends keyof X> = [X] extends [
  { [k in K]: { Columns: infer A } }
]
  ? A
  : never

export type SelectedDb<X> = [X] extends [{ UseVersion: `v${infer V}` }]
  ? [X] extends [{ History: { [k in `v${V}`]: infer A } }]
    ? TablesFrom<A>
    : TablesFrom<X>
  : TablesFrom<X>

export function sql<X>(db: Db<X>): SQLFor<SelectedDb<X>> {
  const tables = tablesFor(db)
  return {
    // @ts-expect-error
    select: (table: any, alias: any) =>
      new SelectImpl(tables, Chunk.single(new SelectTable({ table, alias })))
  }
}

class SelectTable extends Tagged("SelectTable")<{
  readonly table: string
  readonly alias?: string
}> {}

class SelectJoin extends Tagged("SelectJoin")<{
  readonly table: string
  readonly alias: string
}> {}

class SelectAs extends Tagged("SelectAs")<{
  readonly from: string
  readonly to: string
}> {}

class SelectPick extends Tagged("SelectPick")<{
  readonly keys: readonly string[]
}> {}

class SelectOmit extends Tagged("SelectOmit")<{
  readonly keys: readonly string[]
}> {}

class SelectTablePick extends Tagged("SelectTablePick")<{
  readonly table: string
  readonly keys: readonly string[]
}> {}

class SelectCustomParam extends Tagged("SelectCustomParam")<{
  readonly param: string
  readonly value: ColumnType
  readonly optional: boolean
}> {}

class SelectColumnParam extends Tagged("SelectColumnParam")<{
  readonly param: string
  readonly key: string
}> {}

class SelectWhere extends Tagged("SelectWhere")<{
  readonly ops: WhereOps
}> {}

export type SelectOps =
  | SelectTable
  | SelectAs
  | SelectJoin
  | SelectPick
  | SelectOmit
  | SelectTablePick
  | SelectCustomParam
  | SelectColumnParam
  | SelectWhere

export class SelectImpl {
  constructor(
    readonly tables: Record<string, AnyTable>,
    readonly ops: Chunk.Chunk<SelectOps>
  ) {}

  readonly pick = (...keys: string[]) =>
    new SelectImpl(this.tables, Chunk.append_(this.ops, new SelectPick({ keys })))

  readonly omit = (...keys: string[]) =>
    new SelectImpl(this.tables, Chunk.append_(this.ops, new SelectOmit({ keys })))

  readonly tablePick = (table: string, ...keys: string[]) =>
    new SelectImpl(
      this.tables,
      Chunk.append_(this.ops, new SelectTablePick({ table, keys }))
    )

  readonly as = (from: string, to: string) =>
    new SelectImpl(this.tables, Chunk.append_(this.ops, new SelectAs({ from, to })))

  readonly join = (table: string, alias: string) =>
    new SelectImpl(
      this.tables,
      Chunk.append_(this.ops, new SelectJoin({ table, alias }))
    )

  readonly param = (p: any, v: any, o: any) =>
    typeof v === "string"
      ? new SelectImpl(
          this.tables,
          Chunk.append_(this.ops, new SelectColumnParam({ param: p, key: v }))
        )
      : new SelectImpl(
          this.tables,
          Chunk.append_(
            this.ops,
            new SelectCustomParam({ param: p, optional: o === "optional", value: v })
          )
        )

  remapField(
    target: string,
    nameMap: Record<string, { table: string; column: string }>,
    tableMap: Record<
      string,
      Record<
        string,
        {
          readonly type: ColumnType
          readonly alias: O.Option<string>
          readonly nullable: boolean
        }
      >
    >,
    defTab: Record<
      string,
      {
        readonly type: ColumnType
        readonly alias: O.Option<string>
        readonly nullable: boolean
      }
    >
  ) {
    let targ = target
    if (nameMap[target]) {
      targ = `"${nameMap[target].table}"."${nameMap[target].column}"`
    } else if (target.includes(".")) {
      const [tab, field] = target.split(".")
      targ = `"${this.tables[tab] ? this.tables[tab].alias : tab}"."${O.getOrElse_(
        tableMap[tab][field].alias,
        () => field
      )}"`
    } else if (defTab[target]) {
      targ = `"${O.getOrElse_(defTab[target].alias, () => target)}"`
    }
    return targ
  }

  readonly run = (i?: any) => this.prepare()(i)

  readonly prepare = () => {
    const tables: string[] = []
    const tableMap: Record<
      string,
      Record<
        string,
        {
          readonly type: ColumnType
          readonly alias: O.Option<string>
          readonly nullable: boolean
        }
      >
    > = {}
    let nameMap: Record<string, { table: string; column: string }> = {}
    let paramN = 0
    const params: Record<string, { n: number }> = {}
    const conds: string[] = []
    let defTab: Record<
      string,
      {
        readonly type: ColumnType
        readonly alias: O.Option<string>
        readonly nullable: boolean
      }
    >

    const buildWhere = (op: WhereOps): string => {
      switch (op._tag) {
        case "WhereInit": {
          throw new Error("buggy buggy")
        }
        case "WhereAnd": {
          return `(${buildWhere(op.left)}) AND (${buildWhere(op.right)})`
        }
        case "WhereOr": {
          return `(${buildWhere(op.left)}) AND (${buildWhere(op.right)})`
        }
        case "WhereEq": {
          return `${this.remapField(op.target, nameMap, tableMap, defTab)} = ${
            op.field._tag === "Field"
              ? this.remapField(op.field.field, nameMap, tableMap, defTab)
              : `$${params[op.field.param].n}`
          }`
        }
      }
    }

    for (const op of this.ops) {
      switch (op._tag) {
        case "SelectTable": {
          tables.push(
            `"${this.tables[op.table].alias}"${op.alias ? ` AS ${op.alias}` : ""}`
          )
          const columns = this.tables[op.table].columns
          for (const col of Object.keys(columns)) {
            nameMap[op.alias ? `${op.alias}.${col}` : `${col}`] = {
              table: op.alias ?? this.tables[op.table].alias,
              column: O.getOrElse_(this.tables[op.table].columns[col].alias, () => col)
            }
          }
          tableMap[op.alias ?? op.table] = columns
          defTab = columns
          break
        }
        case "SelectJoin": {
          tables.push(`"${this.tables[op.table].alias}" AS "${op.alias}"`)
          const columns = this.tables[op.table].columns
          for (const col of Object.keys(columns)) {
            nameMap[`${op.alias}.${col}`] = {
              table: op.alias ?? this.tables[op.table].alias,
              column: O.getOrElse_(this.tables[op.table].columns[col].alias, () => col)
            }
          }
          tableMap[op.alias ?? op.table] = columns
          break
        }
        case "SelectAs": {
          nameMap[op.to] = nameMap[op.from]
          delete nameMap[op.from]
          break
        }
        case "SelectPick": {
          const newMap = {}
          for (const k of op.keys) {
            newMap[k] = nameMap[k]
          }
          nameMap = newMap
          break
        }
        case "SelectOmit": {
          for (const k of op.keys) {
            delete nameMap[k]
          }
          break
        }
        case "SelectTablePick": {
          const newMap = {}
          for (const k of Object.keys(nameMap)) {
            if (!k.startsWith(`${op.table}.`)) {
              newMap[k] = nameMap[k]
            }
          }
          for (const k of op.keys) {
            newMap[k] = nameMap[k]
          }
          nameMap = newMap
          break
        }
        case "SelectColumnParam": {
          paramN++
          params[op.param] = {
            n: paramN
          }
          break
        }
        case "SelectCustomParam": {
          paramN++
          params[op.param] = {
            n: paramN
          }
          break
        }
        case "SelectWhere": {
          conds.push(buildWhere(op.ops))
          break
        }
        default: {
          absurd(op)
        }
      }
    }

    const views = []

    for (const n of Object.keys(nameMap)) {
      views.push(`"${nameMap[n].table}"."${nameMap[n].column}" AS "${n}"`)
    }

    const queryStr =
      `SELECT ${views.join(", ")} FROM ${tables.join(", ")}` +
      (conds.length > 0 ? ` WHERE ${conds.join(" AND ")}` : "")

    const paramsK = Object.keys(params)

    if (paramsK.length === 0) {
      return () => T.map_(query(queryStr), (q) => Chunk.from(q.rows))
    }

    const paramsToArr = (inp: any) => {
      const out = []
      for (const k of paramsK) {
        out[params[k].n - 1] = inp[k]
      }
      return out
    }

    return (inp: any) =>
      T.map_(query(queryStr, paramsToArr(inp)), (q) => Chunk.from(q.rows))
  }

  readonly where = (f: (_: WhereImpl) => WhereImpl) => {
    const ops = f(new WhereImpl(new WhereInit())).ops
    return ops._tag === "WhereInit"
      ? this
      : new SelectImpl(this.tables, Chunk.append_(this.ops, new SelectWhere({ ops })))
  }
}

type FieldOrParam = Field | Param

class Field extends Tagged("Field")<{
  readonly field: string
}> {}

class Param extends Tagged("Param")<{
  readonly param: string
}> {}

class WhereEq extends Tagged("WhereEq")<{
  readonly target: string
  readonly field: FieldOrParam
}> {}

class WhereAnd extends Tagged("WhereAnd")<{
  readonly left: WhereOps
  readonly right: WhereOps
}> {}

class WhereOr extends Tagged("WhereOr")<{
  readonly left: WhereOps
  readonly right: WhereOps
}> {}

class WhereInit extends Tagged("WhereInit")<{}> {}

type WhereOps = WhereEq | WhereAnd | WhereOr | WhereInit

class WhereImpl {
  constructor(readonly ops: WhereOps) {}

  readonly eq = (target: string, value: any) =>
    new WhereImpl(
      typeof value === "string"
        ? new WhereEq({ target, field: new Field({ field: value }) })
        : new WhereEq({ target, field: new Param({ param: value.param }) })
    );

  readonly ["&&"] = (that: WhereImpl) =>
    this.ops._tag === "WhereInit"
      ? that
      : that.ops._tag === "WhereInit"
      ? this
      : new WhereImpl(new WhereAnd({ left: this.ops, right: that.ops }));

  readonly ["||"] = (that: WhereImpl) =>
    this.ops._tag === "WhereInit"
      ? that
      : that.ops._tag === "WhereInit"
      ? this
      : new WhereImpl(new WhereOr({ left: this.ops, right: that.ops }))
}
