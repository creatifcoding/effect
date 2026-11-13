import { TrinoClient } from "@effect/sql-trino"
import { Clock, Effect, Schedule } from "effect"
import type * as Duration from "effect/Duration"
import * as HttpClient from "effect/http/HttpClient"
import { HttpClientError, TransportError } from "effect/http/HttpClientError"
import type * as HttpClientRequest from "effect/http/HttpClientRequest"
import * as HttpClientResponse from "effect/http/HttpClientResponse"
import * as Reactivity from "effect/reactivity/Reactivity"
import type { TrinoServer } from "./coordinator.ts"
import { coordinatorUrl, queriesFrom } from "./utils.ts"

/**
 * What a wrapped request does, applied around the real `FetchHttpClient`:
 * - `429` / `502` / `503` / `504`: answered by the "proxy" without reaching Trino
 * - `reset-before`: connection reset before the request is sent
 * - `reset-after`: Trino processed the request, then the connection dropped (lost response)
 * - `503-after`: Trino processed the request, then the proxy answered 503
 * - `delay`: Trino answers after a 300 ms delay
 * - `hold-after`: Trino processed the request; the response never arrives
 */
export type Fault =
  | "none"
  | "429"
  | "502"
  | "503"
  | "504"
  | "reset-before"
  | "reset-after"
  | "503-after"
  | "delay"
  | "hold-after"

export const faults: ReadonlyArray<Fault> = [
  "none",
  "429",
  "502",
  "503",
  "504",
  "reset-before",
  "reset-after",
  "503-after",
  "delay"
]

export interface Exchange {
  readonly method: string
  readonly url: string
  readonly fault: Fault
  /** Wall-clock millis when the request was issued. */
  readonly at: number
  /** Status the client saw, or "transport" for a reset. Set when the exchange completes. */
  status: number | "transport" | "pending"
}

/** Retry-After seconds a faulted 429 carries. */
export const retryAfterSeconds = 1

const synthetic = (
  request: HttpClientRequest.HttpClientRequest,
  status: number,
  headers: Record<string, string> = {}
) =>
  HttpClientResponse.fromWeb(request, new Response(status === 204 ? null : `injected ${status}`, { status, headers }))

const reset = (request: HttpClientRequest.HttpClientRequest) =>
  new HttpClientError({ reason: new TransportError({ request, cause: "injected connection reset" }) })

// The body is read so the server finished its side before the fault replaces the answer.
const processed = (forward: Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError>) =>
  forward.pipe(Effect.tap((response) => Effect.ignore(response.text)))

const applyFault = (
  fault: Fault,
  request: HttpClientRequest.HttpClientRequest,
  forward: Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError>
): Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError> => {
  switch (fault) {
    case "none":
      return forward
    case "429":
      return Effect.succeed(synthetic(request, 429, { "retry-after": String(retryAfterSeconds) }))
    case "502":
    case "503":
    case "504":
      return Effect.succeed(synthetic(request, Number(fault)))
    case "reset-before":
      return Effect.fail(reset(request))
    case "reset-after":
      return processed(forward).pipe(Effect.andThen(Effect.fail(reset(request))))
    case "503-after":
      return processed(forward).pipe(Effect.as(synthetic(request, 503)))
    case "delay":
      return Effect.delay(forward, "300 millis")
    case "hold-after":
      return processed(forward).pipe(Effect.andThen(Effect.never))
  }
}

/**
 * Wraps `real` so every request is recorded and `choose` decides its fault.
 * Responses with a body are buffered by `processed` only when a fault needs it.
 */
export const faultyClient = (
  real: HttpClient.HttpClient,
  choose: (request: HttpClientRequest.HttpClientRequest, exchanges: ReadonlyArray<Exchange>) => Fault = () => "none"
) => {
  const exchanges: Array<Exchange> = []
  const http = HttpClient.make((request) =>
    Effect.gen(function*() {
      const fault = choose(request, exchanges)
      const exchange: Exchange = {
        method: request.method,
        url: request.url,
        fault,
        at: yield* Clock.currentTimeMillis,
        status: "pending"
      }
      exchanges.push(exchange)
      return yield* applyFault(fault, request, real.execute(request)).pipe(
        Effect.tap((response) => Effect.sync(() => exchange.status = response.status)),
        Effect.tapError(() => Effect.sync(() => exchange.status = "transport"))
      )
    })
  )
  return { http, exchanges }
}

/** Statement-plane requests: the POST and every nextUri GET. */
export const isStatementRequest = (request: HttpClientRequest.HttpClientRequest) =>
  (request.method === "POST" && request.url.endsWith("/v1/statement")) ||
  (request.method === "GET" && request.url.includes("/v1/statement/"))

/**
 * A `TrinoClient` of its own (unique `X-Trino-Source`, so `queriesFrom(source)`
 * sees only its statements) on `http`, scoped to the caller.
 */
export const clientOn = (
  container: TrinoServer,
  http: HttpClient.HttpClient,
  config: Partial<TrinoClient.TrinoClientConfig> = {}
) => {
  const source = config.source ?? `protocol-${crypto.randomUUID()}`
  return TrinoClient.make({
    url: coordinatorUrl(container),
    user: "test",
    source,
    // Zero-delay retries: only a Retry-After makes the client wait, so its presence is measurable.
    retrySchedule: Schedule.recurs(20),
    ...config
  }).pipe(
    Effect.map((sql) => ({ sql, source })),
    Effect.provideService(HttpClient.HttpClient, http),
    Effect.provide(Reactivity.layer)
  )
}

// QUEUED is left out on purpose. A POST whose answer was lost and then re-sent leaves an entry the
// coordinator never dispatches (dispatch waits for the first nextUri GET); should it be listed, it stays
// QUEUED until query.client.timeout purges it and never runs. Every query the suites start runs past
// QUEUED, so only the states after it tell what executed.
const EXECUTING_STATES: Record<string, true> = {
  WAITING_FOR_RESOURCES: true,
  DISPATCHING: true,
  PLANNING: true,
  STARTING: true,
  RUNNING: true,
  FINISHING: true
}

/**
 * The queries of `source` that executed (never-dispatched QUEUED entries left
 * out), once none of them is still executing; fails if one still is after
 * `within` (cancellation is asynchronous). "Ran twice" shows as two rows.
 */
export const settledQueries = (source: string, within: Duration.Input = "30 seconds") =>
  queriesFrom(source).pipe(
    Effect.map((rows) => rows.filter((row) => row.state !== "QUEUED")),
    Effect.flatMap((rows) =>
      rows.some((row) => EXECUTING_STATES[row.state])
        ? Effect.fail(new Error(`still executing: ${JSON.stringify(rows)}`))
        : Effect.succeed(rows)
    ),
    Effect.retry({ schedule: Schedule.spaced("250 millis").pipe(Schedule.upTo({ duration: within })) })
  )

export const isExecuting = (state: string) => EXECUTING_STATES[state] === true
