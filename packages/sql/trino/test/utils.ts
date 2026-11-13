import { TrinoClient, TrinoTypes } from "@effect/sql-trino"
import { Context, Data, Effect, Layer } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpClient from "effect/http/HttpClient"
import { SqlClient } from "effect/sql"
import type { GenericContainer } from "testcontainers"
import { inject } from "vitest"
import { reachShared, sharedFiles, trinoContainer, type TrinoFiles, type TrinoServer } from "./coordinator.ts"

export { TRINO_IMAGE, TRINO_VERSION, type TrinoFiles, type TrinoServer } from "./coordinator.ts"

export class ContainerError extends Data.TaggedError("ContainerError")<{
  cause: unknown
}> {}

// The community build of MinIO: quay.io/minio/minio and docker.io/minio/minio
// are no longer pullable.
export const MINIO_IMAGE = "pgsty/minio@sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372"

/**
 * Starts a coordinator of the suite's own, configured by `options`, for suites
 * the shared coordinator cannot serve (access control, timeouts, networks).
 */
export const startTrino = (
  options: TrinoFiles & { readonly configure?: ((container: GenericContainer) => GenericContainer) | undefined } = {}
) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () => {
        const container = trinoContainer(options)
        return (options.configure ? options.configure(container) : container).start()
      },
      catch: (cause) => new ContainerError({ cause })
    }),
    (container) => Effect.promise(() => container.stop())
  )

export const coordinatorUrl = (server: TrinoServer) => `http://${server.getHost()}:${server.getMappedPort(8080)}`

/**
 * Every request the client sent, as `METHOD url [header-names]`, so suites can
 * assert which protocol paths a query took (spooled downloads, acks, cancels)
 * and which headers went where.
 */
export class RequestLog extends Context.Service<RequestLog, Array<string>>()("test/RequestLog") {}

/**
 * A `TrinoClient` for `container` on `FetchHttpClient` (the fetch-only path
 * Workers use), with every request recorded in `RequestLog`.
 */
export const layerClientFor = (server: TrinoServer, config?: Partial<TrinoClient.TrinoClientConfig>) => {
  const log: Array<string> = []
  const http = Layer.effect(HttpClient.HttpClient)(
    Effect.map(
      HttpClient.HttpClient,
      HttpClient.tapRequest((request) =>
        Effect.sync(() =>
          log.push(`${request.method} ${request.url} [${Object.keys(request.headers).sort().join(",")}]`)
        )
      )
    )
  ).pipe(Layer.provide(FetchHttpClient.layer))
  return TrinoClient.layer({ url: coordinatorUrl(server), user: "test", ...config }).pipe(
    Layer.provideMerge(http),
    Layer.merge(Layer.succeed(RequestLog, log))
  )
}

/**
 * The coordinator the integration suites share, started once per run by
 * `test/globalSetup.ts`; a run without it (a single file outside the repo's
 * vitest config) starts one for the file.
 */
export class TrinoContainer extends Context.Service<TrinoContainer, TrinoServer>()("test/TrinoContainer") {
  static readonly layer = Layer.effect(this)(Effect.suspend(() => {
    const shared = inject("trinoCoordinator")
    return shared === undefined ? startTrino(sharedFiles) : Effect.succeed(reachShared(shared))
  }))

  static readonly layerClientWith = (config?: Partial<TrinoClient.TrinoClientConfig>) =>
    Layer.unwrap(Effect.map(TrinoContainer, (container) => layerClientFor(container, config)))

  static readonly layerClient = this.layerClientWith().pipe(Layer.provideMerge(this.layer))
}

/**
 * The queries a client with `X-Trino-Source: source` ran, newest first, from
 * `system.runtime.queries`, leaving out these lookups themselves. Suites give
 * each client a unique `source` and use it to find the coordinator's view of
 * their statements (cancelled, still running, run twice).
 */
export const queriesFrom = Effect.fnUntraced(function*(source: string) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql<{ query_id: string; state: string; error_code: string | null; query: string }>`
    SELECT query_id, state, error_code, query FROM system.runtime.queries
    WHERE source = ${source} AND query NOT LIKE '%system.runtime.queries%'
    ORDER BY created DESC`
})

/**
 * Runs `sql` with the Trino CLI inside the container and returns its output
 * lines as text: with `JSON`, one object per row keyed by column name (read
 * as text so number literals stay exact); with `TSV`, tab-separated values.
 */
export const trinoCli = (server: TrinoServer, sql: string, format: "JSON" | "TSV" = "JSON") =>
  Effect.tryPromise({
    try: () =>
      server.exec([
        // The CLI's JVM otherwise prints with the container's default charset,
        // which mangles non-ASCII on 476.
        "env",
        "LANG=C.UTF-8",
        "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8",
        "trino",
        "--server",
        "http://localhost:8080",
        "--user",
        "oracle",
        "--output-format",
        format,
        "--execute",
        sql
      ]),
    catch: (cause) => new ContainerError({ cause })
  }).pipe(
    Effect.flatMap((result) =>
      result.exitCode === 0
        ? Effect.succeed(result.stdout.split("\n").filter((line) => line.length > 0))
        : Effect.fail(new ContainerError({ cause: new Error(`trino CLI exited ${result.exitCode}: ${result.stderr}`) }))
    )
  )

/**
 * `EFFECT_TRINO_FULL=1` runs every suite at full size: the suites that start servers of their own (access
 * control, faults, resume, keep-alive, the CLI oracle) and every property run. CI runs the lean subset.
 */
export const FULL = process.env.EFFECT_TRINO_FULL === "1"

/** A layer's time budget: generous for the full run, one a lean run fits in. */
export const layerTimeout = FULL ? "5 minutes" : "2 minutes"

/** A property's run count: `full` when `EFFECT_TRINO_FULL=1`, an eighth of it (at least 2) otherwise. */
export const runs = (full: number) => FULL ? full : Math.max(2, Math.ceil(full / 8))

/**
 * How result values decode: `default` is the pg-style decoding, `effect` the
 * `TrinoTypes.makeEffectRegistry()` codecs.
 */
export type ValueMode = "default" | "effect"

export const valueModes: ReadonlyArray<ValueMode> = ["default", "effect"]

export const typesIn = (mode: ValueMode): TrinoTypes.Registry | undefined =>
  mode === "effect" ? TrinoTypes.makeEffectRegistry() : undefined

/** The shared coordinator with a client that decodes values in `mode`. */
export const layerClientIn = (mode: ValueMode, config?: Partial<TrinoClient.TrinoClientConfig>) =>
  TrinoContainer.layerClientWith({ types: typesIn(mode), ...config }).pipe(Layer.provideMerge(TrinoContainer.layer))
