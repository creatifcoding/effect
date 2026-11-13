import { assert, it } from "@effect/vitest"
import { Effect, Layer, Stream } from "effect"
import * as HttpClient from "effect/http/HttpClient"
import { clientOn, faultyClient, settledQueries } from "./protocolHarness.ts"
import { startTrino, TrinoContainer } from "./utils.ts"

// The coordinator abandons a query whose client sent nothing for 20 s (the default is 5 minutes).
const container = Layer.effect(TrinoContainer)(startTrino({ config: "query.client.timeout=20s\n" }))

// Still producing pages long after the pause, so the query is live while the consumer waits.
const LONG_QUERY = "SELECT * FROM tpch.sf1.lineitem"
const PAUSE = "30 seconds"

const timeout = 120_000

/** Reads `rows` rows, pausing for `PAUSE` after the first one, the way a slow consumer would. */
const consumeWithPause = <A, E>(stream: Stream.Stream<A, E>, rows: number) => {
  let seen = 0
  return stream.pipe(
    Stream.tap(() => ++seen === 1 ? Effect.sleep(PAUSE) : Effect.void),
    Stream.take(rows),
    Stream.runCount
  )
}

it.layer(TrinoContainer.layerClientWith().pipe(Layer.provideMerge(container)), {
  timeout: "5 minutes",
  excludeTestServices: true
})("TrinoClient keep-alive", (it) => {
  it.effect("HEAD heartbeats keep a paused query alive past the client timeout", () =>
    Effect.gen(function*() {
      const trino = yield* TrinoContainer
      const { exchanges, http } = faultyClient(yield* HttpClient.HttpClient)
      const { source, sql } = yield* clientOn(trino, http, { heartbeatInterval: "5 seconds", encodings: [] })
      const count = yield* consumeWithPause(sql.unsafe(LONG_QUERY).stream, 20_000)
      assert.strictEqual(count, 20_000)
      const heads = exchanges.filter((exchange) => exchange.method === "HEAD")
      assert.isAtLeast(heads.length, 4)
      assert.isTrue(heads.every((exchange) => exchange.status === 200), JSON.stringify(heads))
      // Ended by the early `take`, not by the coordinator.
      const [query] = yield* settledQueries(source)
      assert.strictEqual(query.error_code, "USER_CANCELED")
    }).pipe(Effect.scoped), timeout)

  // Negative control: the same pause without timely heartbeats loses the query, so the test above
  // fails if the client stops sending them.
  it.effect("without heartbeats the coordinator abandons the paused query", () =>
    Effect.gen(function*() {
      const trino = yield* TrinoContainer
      const { exchanges, http } = faultyClient(yield* HttpClient.HttpClient)
      const { source, sql } = yield* clientOn(trino, http, { heartbeatInterval: "10 minutes", encodings: [] })
      const error = yield* consumeWithPause(sql.unsafe(LONG_QUERY).stream, 20_000).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SqlError")
      assert.isFalse(exchanges.some((exchange) => exchange.method === "HEAD"))
      const [query] = yield* settledQueries(source)
      assert.strictEqual(query.state, "FAILED")
      assert.strictEqual(query.error_code, "ABANDONED_QUERY")
    }).pipe(Effect.scoped), timeout)
})
