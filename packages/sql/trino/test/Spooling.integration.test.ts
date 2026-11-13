/**
 * The spooling protocol against Trino and MinIO: inline-only, spooled-only and
 * mixed results, every encoding this runtime decodes, multi-segment results
 * equal to the direct protocol row for row, every segment acknowledged (proved
 * on the storage side: the bucket is empty afterwards), no credential or
 * session header ever sent to storage, and the coordinator's 303 to storage
 * followed.
 */
import { TrinoClient } from "@effect/sql-trino"
import * as Spooled from "@effect/sql-trino/internal/spooling"
import { assert, describe, it } from "@effect/vitest"
import { BigDecimal, DateTime, Effect, Redacted, Stream } from "effect"
import { Spooling, storedSegments } from "./spooling-utils.ts"
import { FULL, layerClientFor, layerTimeout, RequestLog } from "./utils.ts"

// Every column kind that has its own decoder, so a segment is decoded the way a direct page is.
const query = `
  SELECT orderkey, custkey, CAST(totalprice AS decimal(12, 2)) AS totalprice, orderdate,
         CAST(orderdate AS timestamp(6)) + INTERVAL '0.000123' SECOND AS ts,
         comment, ARRAY[orderkey, custkey] AS keys, MAP(ARRAY[orderstatus], ARRAY[shippriority]) AS status
  FROM tpch.tiny.orders ORDER BY orderkey`
const totalRows = 15_000

// Small segments so one query spans many of them.
const manySegments = {
  spooling_initial_segment_size: "1kB",
  spooling_max_segment_size: "64kB"
}

const render = (rows: ReadonlyArray<ReadonlyArray<unknown>>) =>
  rows.map((row) =>
    JSON.stringify(row, (_, v) =>
      typeof v === "bigint"
        ? `${v}n`
        : BigDecimal.isBigDecimal(v)
        ? BigDecimal.format(v)
        : DateTime.isDateTime(v)
        ? DateTime.formatIso(v)
        : v instanceof Map
        ? [...v]
        : v)
  )

/** Runs `use` with a fresh client for the cluster's coordinator, scoped to `use`. */
const withClient = <A, E, R>(
  config: Partial<TrinoClient.TrinoClientConfig>,
  use: (sql: TrinoClient.TrinoClient, log: ReadonlyArray<string>) => Effect.Effect<A, E, R>
) =>
  Effect.flatMap(Spooling, ({ trino }) =>
    Effect.gen(function*() {
      return yield* use(yield* TrinoClient.TrinoClient, yield* RequestLog)
    }).pipe(Effect.provide(layerClientFor(trino, config))))

/** The same query over the direct protocol (no encodings offered). */
const direct = withClient({ encodings: [] }, (sql) => sql.unsafe(query).values)

/**
 * Runs `query` with a fresh client that holds credentials (a bearer token the
 * insecure coordinator ignores, an extra credential) and a request log.
 */
const run = (config: Partial<TrinoClient.TrinoClientConfig>) =>
  withClient({
    accessToken: Redacted.make("not-for-storage"),
    extraCredentials: { probe: Redacted.make("not-for-storage") },
    ...config
  }, (sql, log) => Effect.map(sql.unsafe(query).values, (rows) => ({ rows, log: [...log] })))

const downloads = (log: ReadonlyArray<string>, storageHost: string) =>
  log.filter((entry) =>
    entry.startsWith("GET ") && (entry.includes(storageHost) || entry.includes("/v1/spooled/download/"))
  )

const acks = (log: ReadonlyArray<string>) =>
  log.filter((entry) => entry.startsWith("GET ") && entry.includes("/v1/spooled/ack/"))

const expectEqualToDirect = Effect.fnUntraced(function*(rows: ReadonlyArray<ReadonlyArray<unknown>>) {
  const reference = yield* direct
  assert.strictEqual(rows.length, totalRows)
  assert.deepStrictEqual(render(rows), render(reference))
})

/** No storage request carried an Authorization or any X-Trino-* header. */
const assertNothingSensitive = (requests: ReadonlyArray<string>) => {
  for (const entry of requests) {
    const headers = entry.slice(entry.lastIndexOf("[") + 1, -1).split(",")
    assert.notInclude(headers, "authorization", entry)
    assert.isFalse(headers.some((name) => name.startsWith("x-trino-")), entry)
  }
}

const timeout = 120_000

const encodings = Spooled.supportedEncodings

describe("Spooling", () => {
  it.layer(Spooling.layer("storage"), { timeout: layerTimeout, excludeTestServices: true })(
    "pre-signed storage URIs",
    (it) => {
      for (const encoding of encodings) {
        it.effect(
          `spooled-only, ${encoding}: many segments equal the direct result and all are acknowledged`,
          () =>
            Effect.gen(function*() {
              const { storageHost } = yield* Spooling
              const { log, rows } = yield* run({
                encodings: [encoding],
                sessionProperties: { ...manySegments, spooling_inlining_enabled: "false" }
              })
              const fetched = downloads(log, storageHost)
              // only `encoding` was offered, so segments prove the server used it
              assert.isAtLeast(fetched.length, 3)
              assert.isTrue(fetched.every((entry) => entry.includes(storageHost)), "segments come from storage")
              assert.strictEqual(acks(log).length, fetched.length)
              assertNothingSensitive(fetched)
              // explicit acknowledgement deletes each object
              assert.strictEqual(yield* storedSegments, 0)
              yield* expectEqualToDirect(rows)
            }),
          timeout
        )
      }

      it.effect("mixed: early rows inline, the rest spooled, equal to the direct result", () =>
        Effect.gen(function*() {
          const { storageHost } = yield* Spooling
          const { log, rows } = yield* run({
            sessionProperties: { ...manySegments, spooling_inlining_max_rows: "100" }
          })
          assert.isAtLeast(downloads(log, storageHost).length, 1)
          yield* expectEqualToDirect(rows)
          assert.strictEqual(yield* storedSegments, 0)
        }), timeout)

      it.effect("inline-only: a small result needs no storage request", () =>
        Effect.gen(function*() {
          const { storageHost } = yield* Spooling
          yield* withClient({}, (sql, log) =>
            Effect.gen(function*() {
              const rows = yield* sql<{ n: bigint; name: string }>`
                SELECT nationkey AS n, name FROM tpch.tiny.nation ORDER BY nationkey`
              assert.strictEqual(rows.length, 25)
              assert.deepStrictEqual(rows[0], { n: BigInt(0), name: "ALGERIA" })
              assert.strictEqual(downloads(log, storageHost).length, 0)
            }))
        }), timeout)

      it.effect(
        "streams spooled rows in order",
        () =>
          withClient(
            { sessionProperties: { ...manySegments, spooling_inlining_enabled: "false" } },
            (sql) =>
              Effect.gen(function*() {
                const keys = yield* sql.unsafe<{ orderkey: bigint }>(
                  "SELECT orderkey FROM tpch.tiny.orders ORDER BY orderkey"
                ).stream.pipe(
                  Stream.map((row) => row.orderkey),
                  Stream.runCollect
                )
                assert.strictEqual(keys.length, totalRows)
                assert.isTrue(keys.every((key, i) => i === 0 || key > keys[i - 1]))
              })
          ),
        timeout
      )
    }
  )

  // The lean run keeps the pre-signed storage layer; the coordinator routes start a cluster each.
  if (FULL) {
    it.layer(Spooling.layer("coordinator_storage_redirect"), { timeout: layerTimeout, excludeTestServices: true })(
      "coordinator redirect to storage",
      (it) => {
        it.effect(
          "follows the coordinator's 303 to storage and acknowledges every segment",
          () =>
            Effect.gen(function*() {
              const { storageHost } = yield* Spooling
              const { log, rows } = yield* run({
                sessionProperties: { ...manySegments, spooling_inlining_enabled: "false" }
              })
              const fetched = downloads(log, storageHost)
              assert.isAtLeast(fetched.length, 3)
              // the client asked the coordinator; fetch followed the 303 to the bucket
              assert.isTrue(fetched.every((entry) => entry.includes("/v1/spooled/download/")))
              assert.strictEqual(acks(log).length, fetched.length)
              assertNothingSensitive([...fetched, ...acks(log)])
              assert.strictEqual(yield* storedSegments, 0)
              yield* expectEqualToDirect(rows)
            }),
          timeout
        )
      }
    )

    it.layer(Spooling.layer("coordinator_proxy"), { timeout: layerTimeout, excludeTestServices: true })(
      "coordinator proxy",
      (it) => {
        it.effect("reads segments through the coordinator", () =>
          Effect.gen(function*() {
            const { storageHost } = yield* Spooling
            const { log, rows } = yield* run({
              sessionProperties: { ...manySegments, spooling_inlining_enabled: "false" }
            })
            const fetched = downloads(log, storageHost)
            assert.isAtLeast(fetched.length, 3)
            assert.isFalse(log.some((entry) => entry.includes(storageHost)), "the client never talks to storage")
            assert.strictEqual(acks(log).length, fetched.length)
            assert.strictEqual(yield* storedSegments, 0)
            yield* expectEqualToDirect(rows)
          }), timeout)
      }
    )
  }
})
