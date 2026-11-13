import { assert, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Layer, Schedule, Schema, Stream } from "effect"
import * as HttpClient from "effect/http/HttpClient"
import * as HttpClientResponse from "effect/http/HttpClientResponse"
import { SqlClient } from "effect/sql"
import { clientOn, type Exchange, faultyClient, isExecuting, settledQueries } from "./protocolHarness.ts"
import { layerTimeout, runs, TrinoContainer } from "./utils.ts"

// Large enough that the coordinator is still producing pages when the client stops.
const LONG_QUERY = "SELECT * FROM tpch.sf1.lineitem"

const timeout = 120_000

const deletes = (exchanges: ReadonlyArray<Exchange>) => exchanges.filter((exchange) => exchange.method === "DELETE")

const assertCancelled = (rows: ReadonlyArray<{ readonly state: string; readonly error_code: string | null }>) => {
  assert.strictEqual(rows.length, 1, JSON.stringify(rows))
  assert.strictEqual(rows[0].state, "FAILED")
  assert.strictEqual(rows[0].error_code, "USER_CANCELED")
}

// Real time throughout: the client's schedules and the polling of system.runtime.queries must not run on a TestClock.
it.layer(TrinoContainer.layerClient.pipe(Layer.provideMerge(TrinoContainer.layer)), {
  timeout: layerTimeout,
  excludeTestServices: true
})("TrinoClient cancellation", (it) => {
  it.effect.prop(
    "ending a stream after k rows cancels the query",
    { rows: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50_000 })) },
    Effect.fnUntraced(function*({ rows }) {
      const container = yield* TrinoContainer
      const { exchanges, http } = faultyClient(yield* HttpClient.HttpClient)
      const { source, sql } = yield* clientOn(container, http)
      const taken = yield* sql.unsafe(LONG_QUERY).stream.pipe(Stream.take(rows), Stream.runCount)
      assert.strictEqual(taken, rows)
      assert.isAtLeast(deletes(exchanges).length, 1)
      assertCancelled(yield* settledQueries(source))
    }, Effect.scoped),
    { timeout, arbitrary: { runs: runs(5) } }
  )

  it.effect.prop(
    "interrupting the consumer after k rows cancels the query",
    { rows: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50_000 })) },
    Effect.fnUntraced(function*({ rows }) {
      const container = yield* TrinoContainer
      const { exchanges, http } = faultyClient(yield* HttpClient.HttpClient)
      const { source, sql } = yield* clientOn(container, http)
      const reached = yield* Deferred.make<void>()
      let seen = 0
      const fiber = yield* sql.unsafe(LONG_QUERY).stream.pipe(
        Stream.tap(() => ++seen === rows ? Deferred.succeed(reached, undefined) : Effect.void),
        Stream.runDrain,
        Effect.forkChild
      )
      yield* Deferred.await(reached)
      yield* Fiber.interrupt(fiber)
      assert.isAtLeast(deletes(exchanges).length, 1)
      assertCancelled(yield* settledQueries(source))
    }, Effect.scoped),
    { timeout, arbitrary: { runs: runs(5) } }
  )

  it.effect("interrupting while QUEUED cancels the dispatched query", () =>
    Effect.gen(function*() {
      const container = yield* TrinoContainer
      // The first queued GET dispatches the query on the coordinator; its answer never arrives.
      const { exchanges, http } = faultyClient(
        yield* HttpClient.HttpClient,
        (request, seen) =>
          request.method === "GET" && request.url.includes("/v1/statement/queued/") &&
            !seen.some((exchange) => exchange.url.includes("/v1/statement/queued/"))
            ? "hold-after"
            : "none"
      )
      const { source, sql } = yield* clientOn(container, http)
      const fiber = yield* sql.unsafe(LONG_QUERY).stream.pipe(Stream.runDrain, Effect.forkChild)
      // Wait until the held queued GET reached the coordinator, then interrupt.
      yield* Effect.suspend(() =>
        exchanges.some((exchange) => exchange.url.includes("/v1/statement/queued/"))
          ? Effect.void
          : Effect.fail("queued GET not sent yet")
      ).pipe(Effect.retry({ schedule: Schedule.spaced("50 millis").pipe(Schedule.upTo({ duration: "30 seconds" })) }))
      // The coordinator answers a queued GET within 1 s; give it time to dispatch the query.
      yield* Effect.sleep("2 seconds")
      yield* Fiber.interrupt(fiber)
      const deleted = deletes(exchanges)
      assert.strictEqual(deleted.length, 1)
      assert.include(deleted[0].url, "/v1/statement/queued/")
      assert.strictEqual(deleted[0].status, 204)
      assertCancelled(yield* settledQueries(source))
    }).pipe(Effect.scoped), timeout)

  it.effect(
    "interrupting during the POST cancels the statement once its nextUri is known",
    () =>
      Effect.gen(function*() {
        const container = yield* TrinoContainer
        const postSent = yield* Deferred.make<void>()
        const real = yield* HttpClient.HttpClient
        // The POST reaches the coordinator; its answer is held until the client was interrupted.
        const release = yield* Deferred.make<void>()
        const { exchanges, http } = faultyClient(
          HttpClient.make((request) =>
            request.method === "POST"
              ? real.execute(request).pipe(
                Effect.flatMap((response) =>
                  Effect.map(response.text, (body) =>
                    HttpClientResponse.fromWeb(
                      request,
                      new Response(body, { status: response.status, headers: response.headers })
                    ))
                ),
                Effect.tap(() => Deferred.succeed(postSent, undefined)),
                Effect.tap(() => Deferred.await(release))
              )
              : real.execute(request)
          )
        )
        const { source, sql } = yield* clientOn(container, http)
        const fiber = yield* sql.unsafe(LONG_QUERY).stream.pipe(Stream.runDrain, Effect.forkChild)
        yield* Deferred.await(postSent)
        const interrupted = yield* Effect.forkChild(Fiber.interrupt(fiber))
        yield* Effect.sleep("200 millis")
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(interrupted)

        // The cancel waited for the POST's answer and DELETEd its queued nextUri, which the coordinator accepted.
        const deleted = deletes(exchanges)
        assert.strictEqual(deleted.length, 1)
        assert.include(deleted[0].url, "/v1/statement/queued/")
        assert.strictEqual(deleted[0].status, 204)
        // The coordinator dispatches a statement on the first GET of its queued nextUri, which never happened:
        // the query never reaches the dispatcher, so system.runtime.queries cannot show it, cancelled or not.
        // The DELETE above is the observable effect; Protocol.test.ts covers the same path with a stub.
        // Only statement GETs count; the client's connect check (`GET /v1/info`) goes through the same client.
        assert.isFalse(
          exchanges.some((exchange) => exchange.method === "GET" && exchange.url.includes("/v1/statement/"))
        )
        assert.deepStrictEqual(yield* settledQueries(source), [])
      }).pipe(Effect.scoped),
    timeout
  )

  it.effect("a decode failure mid-stream cancels the query", () =>
    Effect.gen(function*() {
      const container = yield* TrinoContainer
      const real = yield* HttpClient.HttpClient
      let corrupted = false
      // The first page carrying rows gets an extra, non-bigint value in its first row.
      const corrupt = (body: string) => {
        const index = body.indexOf("\"data\":[[")
        if (index === -1) return body
        corrupted = true
        return `${body.slice(0, index)}"data":[["not a number",${body.slice(index + "\"data\":[[".length)}`
      }
      const { exchanges, http } = faultyClient(
        HttpClient.make((request) =>
          real.execute(request).pipe(
            Effect.flatMap((response) =>
              request.method !== "GET" || corrupted
                ? Effect.succeed(response)
                : Effect.map(response.text, (body) =>
                  HttpClientResponse.fromWeb(
                    request,
                    new Response(corrupt(body), { status: response.status, headers: response.headers })
                  ))
            )
          )
        )
      )
      const { source, sql } = yield* clientOn(container, http, { encodings: [] })
      const error = yield* sql.unsafe("SELECT orderkey FROM tpch.sf1.lineitem").stream.pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(error._tag, "SqlError")
      assert.isTrue(corrupted)
      assert.isAtLeast(deletes(exchanges).length, 1)
      assertCancelled(yield* settledQueries(source))
    }).pipe(Effect.scoped), timeout)

  // An orphan is a query still executing for nobody; never-dispatched QUEUED entries cannot run (see protocolHarness).
  it.effect("no query of this suite is left executing", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const running = yield* sql<{ query_id: string; state: string; source: string }>`
        SELECT query_id, state, source FROM system.runtime.queries
        WHERE source LIKE 'protocol-%'`.pipe(
        Effect.map((rows) => rows.filter((row) => isExecuting(row.state))),
        Effect.flatMap((rows) => rows.length === 0 ? Effect.succeed(rows) : Effect.fail(rows)),
        Effect.retry({ schedule: Schedule.spaced("250 millis").pipe(Schedule.upTo({ duration: "30 seconds" })) })
      )
      assert.deepStrictEqual(running, [])
    }), timeout)
})
