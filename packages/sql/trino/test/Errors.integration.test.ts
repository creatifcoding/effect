import { TrinoClient } from "@effect/sql-trino"
import type { TrinoFailure } from "@effect/sql-trino/internal/failure"
import { assert, it } from "@effect/vitest"
import { Context, Effect, Fiber, Layer, Redacted, Schedule } from "effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as HttpClient from "effect/http/HttpClient"
import * as HttpClientRequest from "effect/http/HttpClientRequest"
import * as Reactivity from "effect/reactivity/Reactivity"
import { SqlClient, type SqlError } from "effect/sql"
import type { StartedTestContainer } from "testcontainers"
import { coordinatorUrl, queriesFrom, startTrino } from "./utils.ts"

// One real statement per mapped failure, against a server configured to
// produce it: file-based access control for PERMISSION_DENIED, password
// authentication for 401.

const rules = JSON.stringify({
  tables: [
    { user: "denied", catalog: "tpch", privileges: [] },
    { privileges: ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP", "GRANT_SELECT"] }
  ]
})

class AclTrino extends Context.Service<AclTrino, StartedTestContainer>()("test/AclTrino") {
  static readonly layer = Layer.effect(this)(startTrino({
    files: {
      "access-control.properties": "access-control.name=file\nsecurity.config-file=/etc/trino/rules.json\n",
      "rules.json": rules
    }
  }))
}

const PASSWORD = "correct horse"

// Trino's file authenticator accepts PBKDF2 entries `user:iterations:salt:hash`
// (lowercase hex, `PBKDF2WithHmacSHA1`, at least 1000 iterations; plugin
// `password/file/EncryptionUtil.java`), so the entry is derived here with
// WebCrypto instead of a bcrypt tool.
const passwordEntry = (user: string, password: string) =>
  Effect.promise(async () => {
    const salt = crypto.getRandomValues(new Uint8Array(16))
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(password), "PBKDF2", false, [
      "deriveBits"
    ])
    const hash = await crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-1", salt, iterations: 1000 }, key, 256)
    const hex = (bytes: Uint8Array) => Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")
    return `${user}:1000:${hex(salt)}:${hex(new Uint8Array(hash))}\n`
  })

// Trino refuses passwords on plain HTTP ("Password not allowed for insecure
// authentication") even with allow-insecure-over-http. Deployments put it
// behind a TLS-terminating proxy; the password suite plays that proxy in
// process: the client speaks to an https:// URL, and each request is
// forwarded over plain HTTP with `X-Forwarded-Proto: https`, which the
// coordinator honours (`http-server.process-forwarded`) and echoes in the
// https:// nextUris the client then follows.
class AuthTrino extends Context.Service<AuthTrino, StartedTestContainer>()("test/AuthTrino") {
  static readonly layer = Layer.effect(this)(Effect.gen(function*() {
    const entry = yield* passwordEntry("alice", PASSWORD)
    return yield* startTrino({
      config: `http-server.authentication.type=PASSWORD
http-server.process-forwarded=true
internal-communication.shared-secret=${btoa("effect-sql-trino-tests-shared-secret")}
`,
      files: {
        "password-authenticator.properties":
          "password-authenticator.name=file\nfile.password-file=/etc/trino/password.db\n",
        "password.db": entry
      }
    })
  }))
}

const clientFor = (container: StartedTestContainer, config?: Partial<TrinoClient.TrinoClientConfig>) =>
  TrinoClient.make({ url: coordinatorUrl(container), user: "test", ...config }).pipe(
    Effect.provide([FetchHttpClient.layer, Reactivity.layer])
  )

const tlsTerminatingProxy = Layer.effect(HttpClient.HttpClient)(
  Effect.map(
    HttpClient.HttpClient,
    HttpClient.mapRequest((request) =>
      request.pipe(
        HttpClientRequest.setUrl(request.url.replace(/^https:/, "http:")),
        HttpClientRequest.setHeader("X-Forwarded-Proto", "https")
      )
    )
  )
).pipe(Layer.provide(FetchHttpClient.layer))

const authClientFor = (container: StartedTestContainer, password: string) =>
  TrinoClient.make({
    url: coordinatorUrl(container).replace(/^http:/, "https:"),
    user: "alice",
    password: Redacted.make(password)
  }).pipe(Effect.provide([tlsTerminatingProxy, Reactivity.layer]))

const failureOf = (error: SqlError.SqlError) => error.reason.cause as TrinoFailure

const queryErrorOf = (error: SqlError.SqlError) => {
  const failure = failureOf(error)
  assert.strictEqual(failure._tag, "QueryFailed", String(error))
  return failure._tag === "QueryFailed" ? failure.error : undefined
}

/** What the server and the driver said, for assertion messages. */
const evidence = (error: SqlError.SqlError) => {
  const failure = failureOf(error)
  const queryError = failure._tag === "QueryFailed" ? failure.error : undefined
  const info = queryError?.failureInfo
  return JSON.stringify({
    reason: error.reason._tag,
    failure: failure._tag,
    errorName: queryError?.errorName,
    errorType: queryError?.errorType,
    errorCode: queryError?.errorCode,
    message: queryError?.message ?? error.reason.message,
    failureType: typeof info === "object" && info !== null && "type" in info ? info.type : undefined
  })
}

const timeout = 120_000

// 600 billion rows: the scan runs for hours, so a cancelled query can only have
// ended because of the cancel.
const UNBOUNDED_SCAN = "tpch.sf100000.lineitem"

it.layer(Layer.mergeAll(AclTrino.layer, AuthTrino.layer), { timeout: "4 minutes", excludeTestServices: true })(
  "TrinoClient errors",
  (it) => {
    it.effect(
      "missing table, schema and catalog are syntax errors carrying Trino's error name",
      () =>
        Effect.gen(function*() {
          const sql = yield* clientFor(yield* AclTrino)
          for (
            const [statement, name] of [
              ["SELECT * FROM tpch.tiny.no_such_table", "TABLE_NOT_FOUND"],
              ["SELECT * FROM tpch.no_such_schema.t", "SCHEMA_NOT_FOUND"],
              ["SELECT * FROM no_such_catalog.s.t", "CATALOG_NOT_FOUND"]
            ] as const
          ) {
            const error = yield* sql.unsafe(statement).pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, "SqlSyntaxError", statement)
            assert.strictEqual(queryErrorOf(error)?.errorName, name)
          }
        }),
      timeout
    )

    it.effect("a syntax error carries its location in the statement", () =>
      Effect.gen(function*() {
        const sql = yield* clientFor(yield* AclTrino)
        const error = yield* sql.unsafe("SELECT 1\nFROM WHERE").pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "SqlSyntaxError")
        const queryError = queryErrorOf(error)
        assert.strictEqual(queryError?.errorName, "SYNTAX_ERROR")
        assert.deepStrictEqual(queryError?.errorLocation, { lineNumber: 2, columnNumber: 6 })
      }), timeout)

    it.effect("a table the access control denies is an authorization error", () =>
      Effect.gen(function*() {
        const sql = yield* clientFor(yield* AclTrino, { user: "denied" })
        const error = yield* sql.unsafe("SELECT * FROM tpch.tiny.nation").pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "AuthorizationError")
        assert.strictEqual(queryErrorOf(error)?.errorName, "PERMISSION_DENIED")
      }), timeout)

    it.effect("a query cancelled by someone else fails with USER_CANCELED", () =>
      Effect.gen(function*() {
        const container = yield* AclTrino
        const source = `errors-${crypto.randomUUID()}`
        const sql = yield* clientFor(container, { source })
        // 600 billion rows: the scan runs for hours, so only the cancel can end it.
        const fiber = yield* Effect.forkChild(Effect.flip(sql.unsafe(`SELECT count(*) FROM ${UNBOUNDED_SCAN}`)))
        // Cancel only once the coordinator reports the query RUNNING, so the
        // kill lands on an executing query rather than one still being queued.
        const [query] = yield* queriesFrom(source).pipe(
          Effect.filterOrFail((rows) => rows.length > 0 && rows[0].state === "RUNNING"),
          Effect.retry({ schedule: Schedule.spaced("100 millis"), times: 300 }),
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        yield* Effect.promise(() =>
          fetch(`${coordinatorUrl(container)}/v1/query/${query.query_id}`, {
            method: "DELETE",
            headers: { "X-Trino-User": "test" }
          })
        )
        const error = yield* Fiber.join(fiber)
        assert.strictEqual(queryErrorOf(error)?.errorName, "USER_CANCELED", evidence(error))
        assert.strictEqual(error.reason._tag, "UnknownError", evidence(error))
      }), timeout)

    it.effect("a query over its memory limit fails with an INSUFFICIENT_RESOURCES error", () =>
      Effect.gen(function*() {
        const sql = yield* clientFor(yield* AclTrino, { sessionProperties: { query_max_memory: "1MB" } })
        const error = yield* sql.unsafe("SELECT array_agg(comment) FROM tpch.sf1.orders").pipe(Effect.flip)
        assert.strictEqual(queryErrorOf(error)?.errorType, "INSUFFICIENT_RESOURCES", evidence(error))
        assert.strictEqual(error.reason._tag, "UnknownError", evidence(error))
      }), timeout)

    it.effect("too few workers past the resume window is a connection error", () =>
      Effect.gen(function*() {
        const sql = yield* clientFor(yield* AclTrino, {
          sessionProperties: { required_workers_count: "2", required_workers_max_wait_time: "1s" },
          resumeWindow: "0 millis"
        })
        const error = yield* sql.unsafe("SELECT count(*) FROM tpch.tiny.nation").pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "ConnectionError")
        const queryError = queryErrorOf(error)
        assert.strictEqual(queryError?.errorName, "GENERIC_INSUFFICIENT_RESOURCES")
        assert.isTrue(queryError?.message.startsWith("Insufficient active worker nodes"))
      }), timeout)

    it.effect("a session query_max_run_time is a statement timeout", () =>
      Effect.gen(function*() {
        const sql = yield* clientFor(yield* AclTrino, { sessionProperties: { query_max_run_time: "1s" } })
        const error = yield* sql.unsafe("SELECT count(*) FROM tpch.sf100.lineitem").pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "StatementTimeoutError", evidence(error))
        assert.strictEqual(queryErrorOf(error)?.errorName, "EXCEEDED_TIME_LIMIT", evidence(error))
      }), timeout)

    it.effect("a wrong password is an authentication error; the right one runs", () =>
      Effect.gen(function*() {
        const container = yield* AuthTrino
        // `make` only reads the unauthenticated /v1/info; the first statement carries the credentials.
        const unauthorized = yield* authClientFor(container, "wrong")
        const wrong = yield* unauthorized.unsafe("SELECT 1").pipe(Effect.flip)
        assert.strictEqual(wrong.reason._tag, "AuthenticationError")
        const failure = failureOf(wrong)
        assert.strictEqual(failure._tag === "HttpStatusFailure" ? failure.status : undefined, 401)

        const sql = yield* authClientFor(container, PASSWORD)
        const [row] = yield* sql<{ user: string }>`SELECT current_user AS user`
        assert.strictEqual(row.user, "alice")
      }), timeout)

    it.effect("an unreachable coordinator is a connection error", () =>
      Effect.gen(function*() {
        const error = yield* TrinoClient.make({ url: "http://127.0.0.1:1", user: "test", connectTimeout: "5 seconds" })
          .pipe(Effect.provide([FetchHttpClient.layer, Reactivity.layer]), Effect.flip)
        assert.strictEqual(error.reason._tag, "ConnectionError")
      }), timeout)
  }
)
