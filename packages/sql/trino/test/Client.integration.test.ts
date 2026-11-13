import { TrinoClient, TrinoTypes } from "@effect/sql-trino"
import { assert, it } from "@effect/vitest"
import { BigDecimal, DateTime, Duration, Effect, Fiber, Schedule, Stream } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as Reactivity from "effect/reactivity/Reactivity"
import { SqlClient, SqlError } from "effect/sql"
import { layerClientIn, layerTimeout, RequestLog, TrinoContainer, valueModes } from "./utils.ts"

// A marker comment makes a statement findable in system.runtime.queries.
const marked = (label: string) => `/* ${label}-${crypto.randomUUID()} */`

const queryState = Effect.fnUntraced(function*(marker: string) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql<{ state: string; error_code: string | null }>`
    SELECT state, error_code FROM system.runtime.queries
    WHERE query LIKE ${`%${marker}%`} AND query NOT LIKE '%system.runtime.queries%'`
})

const RUNNING_STATES: Record<string, true> = {
  QUEUED: true,
  WAITING_FOR_RESOURCES: true,
  DISPATCHING: true,
  PLANNING: true,
  STARTING: true,
  RUNNING: true,
  FINISHING: true
}

// The cancel is asynchronous on the coordinator; poll until the query left the running states.
const settledState = (marker: string) =>
  queryState(marker).pipe(
    Effect.flatMap((rows) =>
      rows.some((row) => RUNNING_STATES[row.state]) ? Effect.fail("still running" as const) : Effect.succeed(rows)
    ),
    Effect.retry({ schedule: Schedule.spaced("200 millis"), times: 100 })
  )

// 600 billion rows: the scan runs for hours, so a cancelled query can only have
// ended because of the cancel.
const UNBOUNDED_SCAN = "tpch.sf100000.lineitem"

// A JVM coordinator shared by concurrent tests; the default 5 s is too tight on a shared runner.
const timeout = 60_000

// Every value assertion holds in both modes: the pg-style default and `TrinoTypes.makeEffectRegistry()`.
for (const mode of valueModes) {
  it.layer(layerClientIn(mode), { timeout: layerTimeout, excludeTestServices: true })(
    `TrinoClient values (${mode})`,
    (it) => {
      it.effect("decodes every scalar and nested type", () =>
        Effect.gen(function*() {
          const sql = yield* TrinoClient.TrinoClient
          const [row] = yield* sql<Record<string, unknown>>`
          SELECT
            BIGINT '9007199254740993' AS big,
            DECIMAL '12345678901234567890.0123456789' AS dec,
            nan() AS nan,
            infinity() AS inf,
            REAL '1.5' AS r,
            true AS bool,
            X'00ff' AS bin,
            DATE '2026-11-11' AS d,
            TIMESTAMP '2026-11-11 10:00:00.123' AS ts3,
            TIMESTAMP '2026-11-11 10:00:00.123456' AS ts6,
            TIMESTAMP '2026-11-11 10:00:00.123 Europe/Dublin' AS tstz,
            TIME '10:00:00.123456' AS t,
            INTERVAL '3' MONTH AS iym,
            INTERVAL '1 02:03:04.005' DAY TO SECOND AS ids,
            ARRAY[1, 2] AS arr,
            MAP(ARRAY['a'], ARRAY[BIGINT '1']) AS m,
            CAST(ROW(1, 'x') AS ROW(id integer, name varchar)) AS rw,
            JSON '{"a":1}' AS j,
            UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS u,
            CAST(NULL AS bigint) AS nothing`

          assert.strictEqual(row.big, 9007199254740993n)
          assert.isTrue(Number.isNaN(row.nan))
          assert.strictEqual(row.inf, Infinity)
          assert.strictEqual(row.r, 1.5)
          assert.strictEqual(row.bool, true)
          assert.deepStrictEqual(row.bin, new Uint8Array([0, 255]))
          assert.deepStrictEqual(row.arr, [1, 2])
          assert.deepStrictEqual(row.m, new Map([["a", 1n]]))
          assert.deepStrictEqual(row.rw, { id: 1, name: "x" })
          assert.strictEqual(row.j, "{\"a\":1}")
          assert.strictEqual(row.u, "12151fd2-7586-11e9-8f9e-2a86e4085a59")
          assert.isNull(row.nothing)

          if (mode === "default") {
            assert.strictEqual(row.dec, "12345678901234567890.0123456789")
            assert.strictEqual(row.d, "2026-11-11")
            assert.deepStrictEqual(row.ts3, new Date(Date.UTC(2026, 10, 11, 10, 0, 0, 123)))
            // truncated to milliseconds
            assert.deepStrictEqual(row.ts6, new Date(Date.UTC(2026, 10, 11, 10, 0, 0, 123)))
            // the instant; the zone is dropped
            assert.deepStrictEqual(row.tstz, new Date(Date.UTC(2026, 10, 11, 10, 0, 0, 123)))
            assert.strictEqual(row.t, "10:00:00.123456")
            assert.strictEqual(row.iym, "0-3")
            assert.match(row.ids as string, /^1 02:03:04\.005\d*$/)
            return
          }
          assert.isTrue(BigDecimal.equals(
            row.dec as BigDecimal.BigDecimal,
            BigDecimal.fromStringUnsafe("12345678901234567890.0123456789")
          ))
          assert.strictEqual(DateTime.toEpochMillis(row.d as DateTime.Utc), Date.UTC(2026, 10, 11))
          assert.isTrue(DateTime.isUtc(row.ts3 as DateTime.DateTime))
          assert.strictEqual(DateTime.toEpochMillis(row.ts3 as DateTime.Utc), Date.UTC(2026, 10, 11, 10, 0, 0, 123))
          const ts6 = row.ts6 as TrinoTypes.PreciseTimestamp
          assert.instanceOf(ts6, TrinoTypes.PreciseTimestamp)
          assert.strictEqual(DateTime.toEpochMillis(ts6.dateTime), Date.UTC(2026, 10, 11, 10, 0, 0, 123))
          assert.strictEqual(ts6.picosOfMilli, 456_000_000)
          const tstz = row.tstz as DateTime.Zoned
          assert.isTrue(DateTime.isZoned(tstz))
          assert.strictEqual(DateTime.zoneToString(tstz.zone), "Europe/Dublin")
          assert.strictEqual(DateTime.toEpochMillis(tstz), Date.UTC(2026, 10, 11, 10, 0, 0, 123))
          assert.deepStrictEqual(
            row.t,
            new TrinoTypes.Time({ picosOfDay: 36_000n * 10n ** 12n + 123_456n * 10n ** 6n, offsetMinutes: undefined })
          )
          assert.deepStrictEqual(row.iym, new TrinoTypes.IntervalYearMonth({ months: 3 }))
          assert.strictEqual(Duration.toMillis(row.ids as Duration.Duration), 93_784_005)
        }), timeout)

      it.effect("advertises PARAMETRIC_DATETIME, so column types keep their precision", () =>
        Effect.gen(function*() {
          const sql = yield* TrinoClient.TrinoClient
          const raw = (yield* sql`SELECT TIMESTAMP '2026-11-11 10:00:00.123456789' AS ts`.raw) as TrinoClient.RawResult
          assert.strictEqual(raw.columns[0].type, "timestamp(9)")
          const ts = raw.rows[0][0]
          if (mode === "default") {
            assert.deepStrictEqual(ts, new Date(Date.UTC(2026, 10, 11, 10, 0, 0, 123)))
          } else {
            assert.strictEqual((ts as TrinoTypes.PreciseTimestamp).picosOfMilli, 456_789_000)
          }
        }), timeout)

      it.effect(
        "round-trips parameters through EXECUTE IMMEDIATE without losing precision",
        () =>
          Effect.gen(function*() {
            const sql = yield* TrinoClient.TrinoClient
            const instant = new Date(Date.UTC(2026, 10, 11, 10, 0, 0, 7))
            const [row] = yield* sql<Record<string, unknown>>`
          SELECT
            ${9007199254740993n} AS big,
            ${"O'Brien'; DROP TABLE t; --"} AS s,
            ${BigDecimal.fromStringUnsafe("1.25")} AS dec,
            ${new Uint8Array([1, 2])} AS bin,
            ${null} AS nothing,
            ${true} AS bool,
            ${1.5} AS dbl,
            ${instant} AS instant,
            ${sql.param("decimal(10,2)", "1.50")} AS typed`

            assert.strictEqual(row.big, 9007199254740993n)
            assert.strictEqual(row.s, "O'Brien'; DROP TABLE t; --")
            assert.deepStrictEqual(row.bin, new Uint8Array([1, 2]))
            assert.isNull(row.nothing)
            assert.strictEqual(row.bool, true)
            assert.strictEqual(row.dbl, 1.5)
            if (mode === "default") {
              assert.strictEqual(row.dec, "1.25")
              assert.deepStrictEqual(row.instant, instant)
              assert.strictEqual(row.typed, "1.50")
              return
            }
            assert.isTrue(BigDecimal.equals(row.dec as BigDecimal.BigDecimal, BigDecimal.fromStringUnsafe("1.25")))
            assert.strictEqual(DateTime.toEpochMillis(row.instant as DateTime.DateTime), instant.getTime())
            assert.isTrue(BigDecimal.equals(row.typed as BigDecimal.BigDecimal, BigDecimal.fromStringUnsafe("1.50")))
          }),
        timeout
      )
    }
  )
}

// Real time throughout: the client's retry schedules and the polling below must not run on a TestClock.
it.layer(TrinoContainer.layerClient, {
  timeout: layerTimeout,
  excludeTestServices: true
})(
  "TrinoClient",
  (it) => {
    it.effect("writes with the insert helper and reads back values and value rows", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const table = `memory.default.people_${crypto.randomUUID().replaceAll("-", "")}`
        yield* sql`CREATE TABLE ${sql(table)} (id bigint, name varchar)`
        yield* sql`INSERT INTO ${sql(table)} ${sql.insert([{ id: 1n, name: "a" }, { id: 2n, name: "b" }])}`
        const rows = yield* sql<{ id: bigint; name: string }>`SELECT id, name FROM ${sql(table)} ORDER BY id`
        assert.deepStrictEqual(rows, [{ id: 1n, name: "a" }, { id: 2n, name: "b" }])
        const values = yield* sql`SELECT id, name FROM ${sql(table)} ORDER BY id`.values
        assert.deepStrictEqual(values, [[1n, "a"], [2n, "b"]])
        const ids = yield* sql<{ id: bigint }>`SELECT id FROM ${sql(table)} WHERE id IN ${sql.in([2n])}`
        assert.deepStrictEqual(ids, [{ id: 2n }])
      }), timeout)

    it.effect("streams rows across pages", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const count = yield* sql`SELECT orderkey FROM tpch.tiny.orders`.stream.pipe(Stream.runCount)
        assert.strictEqual(count, 15_000)
      }), timeout)

    it.effect("ending a stream early cancels the query on the coordinator", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const log = yield* RequestLog
        const marker = marked("stream-take")
        const taken = yield* sql.unsafe(`${marker} SELECT * FROM tpch.sf1.lineitem`).stream.pipe(
          Stream.take(5),
          Stream.runCollect
        )
        assert.strictEqual(taken.length, 5)
        assert.isTrue(log.some((entry) => entry.startsWith("DELETE ")))
        const [state] = yield* settledState(marker)
        assert.strictEqual(state.state, "FAILED")
        assert.strictEqual(state.error_code, "USER_CANCELED")
      }), timeout)

    it.effect("interrupting a running statement cancels the query on the coordinator", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const marker = marked("interrupt")
        const fiber = yield* Effect.forkChild(sql.unsafe(`${marker} SELECT count(*) FROM ${UNBOUNDED_SCAN}`))
        yield* queryState(marker).pipe(
          Effect.filterOrFail((rows) => rows.length > 0 && rows[0].state === "RUNNING"),
          Effect.retry({ schedule: Schedule.spaced("100 millis"), times: 300 })
        )
        yield* Fiber.interrupt(fiber)
        const [state] = yield* settledState(marker)
        assert.strictEqual(state.state, "FAILED")
        assert.strictEqual(state.error_code, "USER_CANCELED")
      }), timeout)

    it.effect("interrupting before the first response leaves no running query behind", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const marker = marked("early-interrupt")
        const fiber = yield* Effect.forkChild(sql.unsafe(`${marker} SELECT count(*) FROM ${UNBOUNDED_SCAN}`))
        yield* Fiber.interrupt(fiber)
        const states = yield* settledState(marker)
        // Either the POST never reached the coordinator, or the query it created was cancelled.
        assert.isTrue(states.every((row) => row.state === "FAILED" && row.error_code === "USER_CANCELED"))
      }), timeout)

    it.effect(
      "a reserved connection keeps USE and SET SESSION without touching the shared session",
      () =>
        Effect.gen(function*() {
          const sql = yield* TrinoClient.TrinoClient
          const conn = yield* sql.reserve
          yield* conn.executeUnprepared("USE tpch.tiny", [], undefined)
          yield* conn.executeUnprepared("SET SESSION query_max_run_time = '1h'", [], undefined)
          const [nations] = yield* conn.execute("SELECT count(*) AS n FROM nation", [], undefined)
          assert.strictEqual(nations.n, 25n)
          const settings = yield* conn.execute("SHOW SESSION LIKE 'query_max_run_time'", [], undefined)
          assert.strictEqual(settings[0].Value, "1h")
          const shared = yield* sql.session
          assert.isUndefined(shared.catalog)
          assert.isUndefined(shared.properties.get("query_max_run_time"))
        }),
      timeout
    )

    it.effect("USE on the shared session applies to later statements of the client", () =>
      Effect.gen(function*() {
        const container = yield* TrinoContainer
        const sql = yield* TrinoClient.make({
          url: `http://${container.getHost()}:${container.getMappedPort(8080)}`,
          user: "test"
        }).pipe(Effect.provide([FetchHttpClient.layer, Reactivity.layer]))
        yield* sql`USE tpch.tiny`
        const [row] = yield* sql<{ n: bigint }>`SELECT count(*) AS n FROM region`
        assert.strictEqual(row.n, 5n)
        const session = yield* sql.session
        assert.strictEqual(session.catalog, "tpch")
        assert.strictEqual(session.schema, "tiny")
      }), timeout)

    it.effect("runs statements of withTransaction in one Trino transaction", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const inside = yield* sql<{ n: bigint }>`
          SELECT count(*) AS n FROM system.runtime.transactions WHERE NOT auto_commit_context`.pipe(
          sql.withTransaction
        )
        assert.isTrue(inside[0].n >= 1n)
      }), timeout)

    it.effect("passes BeginTransaction through as the START TRANSACTION statement", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const rows = yield* sql<{ read_only: boolean }>`
          SELECT read_only FROM system.runtime.transactions WHERE NOT auto_commit_context`.pipe(
          sql.withTransaction,
          Effect.provideService(SqlClient.BeginTransaction, "START TRANSACTION READ ONLY")
        )
        assert.isTrue(rows.some((row) => row.read_only))
      }), timeout)

    it.effect("a failure inside withTransaction rolls back and the client keeps working", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const table = `memory.default.tx_${crypto.randomUUID().replaceAll("-", "")}`
        yield* sql`CREATE TABLE ${sql(table)} (id bigint)`
        // The memory connector refuses writes inside explicit transactions.
        const error = yield* sql`INSERT INTO ${sql(table)} VALUES 1`.pipe(sql.withTransaction, Effect.flip)
        assert.instanceOf(error, SqlError.SqlError)
        const [row] = yield* sql<{ n: bigint }>`SELECT count(*) AS n FROM ${sql(table)}`
        assert.strictEqual(row.n, 0n)
      }), timeout)

    it.effect("nested withTransaction fails: Trino has no savepoints", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const error = yield* sql`SELECT 1`.pipe(sql.withTransaction, sql.withTransaction, Effect.flip)
        assert.instanceOf(error, SqlError.SqlError)
        assert.strictEqual(error.reason._tag, "UnknownError")
        assert.strictEqual(error.reason.operation, "savepoint")
      }), timeout)

    it.effect("classifies server errors into SqlError reasons", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const syntax = yield* sql.unsafe("SELEC 1").pipe(Effect.flip)
        assert.strictEqual(syntax.reason._tag, "SqlSyntaxError")
        const missing = yield* sql.unsafe("SELECT * FROM tpch.tiny.no_such_table").pipe(Effect.flip)
        assert.strictEqual(missing.reason._tag, "SqlSyntaxError")
      }), timeout)

    it.effect("enforces a session query_max_run_time as a statement timeout", () =>
      Effect.gen(function*() {
        const sql = yield* TrinoClient.TrinoClient
        const conn = yield* sql.reserve
        yield* conn.executeUnprepared("SET SESSION query_max_run_time = '1s'", [], undefined)
        const error = yield* conn.executeUnprepared("SELECT count(*) FROM tpch.sf100.lineitem", [], undefined).pipe(
          Effect.flip
        )
        assert.strictEqual(error.reason._tag, "StatementTimeoutError")
      }), timeout)
  }
)
