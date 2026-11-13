import { assert, it } from "@effect/vitest"
import { Effect, Layer, Schema, Stream } from "effect"
import * as HttpClient from "effect/http/HttpClient"
import type * as HttpClientRequest from "effect/http/HttpClientRequest"
import {
  clientOn,
  type Exchange,
  type Fault,
  faults,
  faultyClient,
  isStatementRequest,
  retryAfterSeconds,
  settledQueries
} from "./protocolHarness.ts"
import { Spooling } from "./spooling-utils.ts"
import { queriesFrom, TrinoContainer } from "./utils.ts"

// Several pages on the direct protocol, so faults land on the POST, queued GETs and executing GETs.
const READ_QUERY =
  "SELECT orderkey, linenumber, extendedprice, shipdate, comment FROM tpch.tiny.lineitem ORDER BY orderkey, linenumber"

const timeout = 120_000

const FaultSchedule = Schema.Array(Schema.Literals(faults)).check(Schema.isMaxLength(8))

/**
 * Applies `schedule` in order to the requests `applies` selects; every other
 * request goes through untouched. At most two 429s per run keep the run inside
 * its time budget (each one costs a Retry-After of one second).
 */
const scheduled = (
  schedule: ReadonlyArray<Fault>,
  applies: (request: HttpClientRequest.HttpClientRequest) => boolean
) => {
  let next = 0
  let rateLimited = 0
  return (request: HttpClientRequest.HttpClientRequest): Fault => {
    if (!applies(request) || next >= schedule.length) return "none"
    const fault = schedule[next++]
    if (fault !== "429") return fault
    return ++rateLimited <= 2 ? fault : "none"
  }
}

/** Every 429 was followed by a retry of the same request no sooner than its Retry-After. */
const assertRetryAfterHonoured = (exchanges: ReadonlyArray<Exchange>) => {
  exchanges.forEach((exchange, index) => {
    if (exchange.fault !== "429") return
    const retry = exchanges.slice(index + 1).find((later) =>
      later.method === exchange.method && later.url === exchange.url
    )
    assert.isDefined(retry, `no retry after the 429 on ${exchange.method} ${exchange.url}`)
    if (retry === undefined) return
    // Timer granularity allows a few milliseconds of slack.
    assert.isAtLeast(retry.at - exchange.at, retryAfterSeconds * 1000 - 50)
  })
}

it.layer(TrinoContainer.layerClient.pipe(Layer.provideMerge(TrinoContainer.layer)), {
  timeout: "5 minutes",
  excludeTestServices: true
})("TrinoClient under injected HTTP faults", (it) => {
  it.effect.prop(
    "a read returns exactly the fault-free result and runs once",
    { schedule: FaultSchedule },
    Effect.fnUntraced(function*({ schedule }) {
      const container = yield* TrinoContainer
      const real = yield* HttpClient.HttpClient
      const clean = yield* clientOn(container, real, { encodings: [] })
      const expected = yield* clean.sql.unsafe(READ_QUERY)

      const { exchanges, http } = faultyClient(real, scheduled(schedule, isStatementRequest))
      const { source, sql } = yield* clientOn(container, http, { encodings: [] })
      const rows = yield* sql.unsafe(READ_QUERY)

      assert.strictEqual(rows.length, expected.length)
      assert.deepStrictEqual(rows, expected)
      // A lost or refused POST is re-sent; the coordinator dispatches a statement only on the first GET of
      // its queued nextUri, so exactly one query may ever have run. A client that re-POSTs after a failed
      // nextUri GET would show two here.
      const queries = yield* settledQueries(source)
      assert.strictEqual(queries.length, 1, JSON.stringify(queries))
      assert.strictEqual(queries[0].state, "FINISHED")
      assertRetryAfterHonoured(exchanges)
    }, Effect.scoped),
    { timeout, arbitrary: { runs: 12 } }
  )

  it.effect(
    "a POST answered 503 after the coordinator accepted it is re-sent and runs once",
    () =>
      Effect.gen(function*() {
        const container = yield* TrinoContainer
        const real = yield* HttpClient.HttpClient
        const { exchanges, http } = faultyClient(
          real,
          scheduled(["503-after", "reset-after"], (request) => request.method === "POST")
        )
        const { source, sql } = yield* clientOn(container, http, { encodings: [] })
        const [row] = yield* sql.unsafe<{ n: bigint }>("SELECT count(*) AS n FROM tpch.tiny.nation")
        assert.strictEqual(row.n, 25n)
        assert.strictEqual(
          exchanges.filter((exchange) => exchange.method === "POST").length,
          3,
          JSON.stringify(yield* queriesFrom(source))
        )
        const queries = yield* settledQueries(source)
        assert.strictEqual(queries.length, 1, JSON.stringify(queries))
      }).pipe(Effect.scoped),
    timeout
  )

  it.effect.prop(
    "a write under faults is applied exactly once",
    {
      values: Schema.Array(Schema.Int.check(Schema.isBetween({ minimum: -1000, maximum: 1000 }))).check(
        Schema.isMinLength(1),
        Schema.isMaxLength(20)
      ),
      schedule: FaultSchedule
    },
    Effect.fnUntraced(function*({ schedule, values }) {
      const container = yield* TrinoContainer
      const real = yield* HttpClient.HttpClient
      const table = `memory.default.faults_${crypto.randomUUID().replaceAll("-", "_")}`
      const clean = yield* clientOn(container, real)
      yield* clean.sql.unsafe(`CREATE TABLE ${table} (v integer)`)
      yield* Effect.gen(function*() {
        const { exchanges, http } = faultyClient(real, scheduled(schedule, isStatementRequest))
        const { source, sql } = yield* clientOn(container, http)
        yield* sql.unsafe(`INSERT INTO ${table} VALUES ${values.map(() => "(?)").join(", ")}`, values)
        const rows = yield* clean.sql.unsafe<{ v: number }>(`SELECT v FROM ${table} ORDER BY v`)
        assert.deepStrictEqual(rows.map((row) => row.v), [...values].sort((a, b) => a - b))
        const queries = yield* settledQueries(source)
        assert.strictEqual(queries.length, 1, JSON.stringify(queries))
        assertRetryAfterHonoured(exchanges)
      }).pipe(Effect.ensuring(Effect.ignore(clean.sql.unsafe(`DROP TABLE IF EXISTS ${table}`))))
    }, Effect.scoped),
    { timeout, arbitrary: { runs: 8 } }
  )

  it.effect.prop(
    "a cancel DELETE under faults still cancels the query",
    {
      schedule: Schema.Array(Schema.Literals(["502", "503", "504", "reset-before", "429"] as const)).check(
        Schema.isMinLength(1),
        Schema.isMaxLength(3)
      )
    },
    Effect.fnUntraced(function*({ schedule }) {
      const container = yield* TrinoContainer
      const { exchanges, http } = faultyClient(
        yield* HttpClient.HttpClient,
        scheduled(schedule, (request) => request.method === "DELETE")
      )
      const { source, sql } = yield* clientOn(container, http, { cancelTimeout: "10 seconds" })
      yield* sql.unsafe("SELECT * FROM tpch.sf1.lineitem").stream.pipe(Stream.take(1), Stream.runDrain)
      const deletes = exchanges.filter((exchange) => exchange.method === "DELETE")
      assert.strictEqual(deletes.at(-1)?.status, 204)
      const queries = yield* settledQueries(source)
      assert.strictEqual(queries.length, 1, JSON.stringify(queries))
      assert.strictEqual(queries[0].state, "FAILED")
      assert.strictEqual(queries[0].error_code, "USER_CANCELED")
    }, Effect.scoped),
    { timeout, arbitrary: { runs: 5 } }
  )
})

// Large enough to exceed the coordinator's inlining limits, so the result arrives in spooled segments.
const SPOOLED_QUERY = "SELECT * FROM tpch.tiny.lineitem ORDER BY orderkey, linenumber"

const isSegmentRequest = (request: HttpClientRequest.HttpClientRequest) => request.url.includes("/v1/spooled/")

// coordinator_proxy keeps segment downloads and acks on the coordinator, so the injected faults reach them.
it.layer(Spooling.layerClientWith().pipe(Layer.provideMerge(Spooling.layer("coordinator_proxy"))), {
  timeout: "5 minutes",
  excludeTestServices: true
})("TrinoClient spooled segments under injected HTTP faults", (it) => {
  it.effect.prop(
    "segment downloads and acks under faults return the direct-protocol result",
    { schedule: FaultSchedule },
    Effect.fnUntraced(function*({ schedule }) {
      const container = (yield* Spooling).trino
      const real = yield* HttpClient.HttpClient
      const direct = yield* clientOn(container, real, { encodings: [] })
      const expected = yield* direct.sql.unsafe(SPOOLED_QUERY)

      const { exchanges, http } = faultyClient(real, scheduled(schedule, isSegmentRequest))
      const { sql } = yield* clientOn(container, http)
      const rows = yield* sql.unsafe(SPOOLED_QUERY)
      assert.strictEqual(rows.length, expected.length)
      assert.deepStrictEqual(rows, expected)

      const downloads = new Set(
        exchanges.filter((exchange) => exchange.url.includes("/v1/spooled/download/")).map((exchange) =>
          exchange.url.slice(exchange.url.lastIndexOf("/") + 1)
        )
      )
      assert.isAbove(downloads.size, 0, "the query was not spooled")
      for (const segment of downloads) {
        assert.isTrue(
          exchanges.some((exchange) => exchange.url.includes(`/v1/spooled/ack/${segment}`)),
          `segment ${segment} was never acknowledged`
        )
      }
      // Acks are best effort (the coordinator falls back to TTL cleanup); downloads must honour Retry-After.
      assertRetryAfterHonoured(exchanges.filter((exchange) => !exchange.url.includes("/v1/spooled/ack/")))
    }, Effect.scoped),
    { timeout, arbitrary: { runs: 6 } }
  )
})
