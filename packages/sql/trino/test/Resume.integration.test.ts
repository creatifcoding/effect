import { assert, it } from "@effect/vitest"
import { Context, Data, Effect, Fiber, Layer, Schedule } from "effect"
import * as HttpClient from "effect/http/HttpClient"
import { SqlClient } from "effect/sql"
import {
  getContainerRuntimeClient,
  Network,
  type StartedNetwork,
  type StartedTestContainer,
  Wait
} from "testcontainers"
import { serverStartedProbe, trinoContainer, type TrinoServer } from "./coordinator.ts"
import { clientOn, faultyClient } from "./protocolHarness.ts"
import { ContainerError, queriesFrom, startTrino, TrinoContainer } from "./utils.ts"

/** The docker network the coordinator runs on; the worker joins it during the test. */
class ClusterNetwork extends Context.Service<ClusterNetwork, StartedNetwork>()("test/ClusterNetwork") {}

/** A started worker JVM parked on a network of its own, out of the coordinator's reach. */
class ParkedWorker extends Context.Service<ParkedWorker, StartedTestContainer>()("test/ParkedWorker") {}

class WorkerDidNotJoin extends Data.TaggedError("WorkerDidNotJoin")<{
  readonly nodes: string
  readonly reachability: string
  readonly log: string
}> {}

const startNetwork = Effect.acquireRelease(
  Effect.tryPromise({ try: () => new Network().start(), catch: (cause) => new ContainerError({ cause }) }),
  (started) => Effect.promise(() => started.stop())
)

const network = Layer.effect(ClusterNetwork)(startNetwork)

const coordinator = Layer.effect(TrinoContainer)(
  Effect.flatMap(
    ClusterNetwork,
    (started) =>
      startTrino({ configure: (container) => container.withNetwork(started).withNetworkAliases("coordinator") })
  )
)

// The worker starts with the suite (alone it can take a minute) on a network of its own, so it cannot announce;
// `joinWorker` attaches it to the cluster network under its alias, which makes the cluster ready. The launcher
// runs as a daemon with an explicit node id (the worker never became active without one). This command
// replaces `trinoContainer`'s, so it appends the heap cap itself, and never to the root-owned config.properties.
const startWorkerJvm =
  "cat /tmp/extra-jvm.config >> /etc/trino/jvm.config && /usr/lib/trino/bin/launcher start --etc-dir /etc/trino -Dnode.id=resume-worker -Dnode.internal-address=worker || exit 1"
const parkedWorker = Layer.effect(ParkedWorker)(
  Effect.gen(function*() {
    const parking = yield* startNetwork
    return yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          trinoContainer({
            files: {
              "config.properties": `coordinator=false
http-server.http.port=8080
discovery.uri=http://coordinator:8080
`
            }
          })
            .withNetwork(parking)
            .withNetworkAliases("worker")
            // A worker runs no queries of its own: it is ready once started.
            .withWaitStrategy(Wait.forSuccessfulCommand(serverStartedProbe).withStartupTimeout(180_000))
            .withCommand([
              "sh",
              "-c",
              `${startWorkerJvm}; trap 'exit 0' TERM; sleep infinity & wait`
            ])
            .start(),
        catch: (cause) => new ContainerError({ cause })
      }),
      (container) => Effect.promise(() => container.stop())
    )
  })
)

const nodes = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  return yield* sql<{ node_id: string; http_uri: string; state: string; coordinator: boolean }>`
    SELECT node_id, http_uri, state, coordinator FROM system.runtime.nodes`
})

const sh = (container: TrinoServer, script: string) =>
  Effect.tryPromise({
    try: () => container.exec(["sh", "-c", script]),
    catch: (cause) => new ContainerError({ cause })
  }).pipe(Effect.map((result) => result.output))

/**
 * Attaches the parked worker to the cluster network and waits until the
 * coordinator lists it as active; on timeout, fails with the coordinator's
 * node list, reachability both ways, and the worker's log.
 */
const joinWorker = Effect.gen(function*() {
  const worker = yield* ParkedWorker
  const coordinatorContainer = yield* TrinoContainer
  const cluster = yield* ClusterNetwork
  yield* Effect.tryPromise({
    try: async () => {
      const client = await getContainerRuntimeClient()
      await client.container.connectToNetwork(
        client.container.getById(worker.getId()),
        client.network.getById(cluster.getId()),
        ["worker"]
      )
    },
    catch: (cause) => new ContainerError({ cause })
  })
  yield* nodes.pipe(
    Effect.flatMap((rows) =>
      rows.filter((row) => row.state === "active").length >= 2 ? Effect.void : Effect.fail("worker not active yet")
    ),
    Effect.retry({ schedule: Schedule.spaced("500 millis").pipe(Schedule.upTo({ duration: "45 seconds" })) }),
    Effect.catch(() =>
      Effect.gen(function*() {
        const fetchInfo = (url: string) =>
          `curl -s -m 5 ${url}/v1/info || wget -q -T 5 -O - ${url}/v1/info || echo "unreachable ${url}"`
        return yield* new WorkerDidNotJoin({
          nodes: JSON.stringify(yield* nodes),
          reachability: [
            `worker -> coordinator: ${yield* sh(worker, fetchInfo("http://coordinator:8080"))}`,
            `coordinator -> worker: ${yield* sh(coordinatorContainer, fetchInfo("http://worker:8080"))}`
          ].join("\n"),
          log: yield* sh(
            worker,
            // The image has no grep.
            "tail -n 60 /data/trino/var/log/server.log; tail -n 20 /data/trino/var/log/launcher.log"
          )
        })
      })
    )
  )
})

// Two nodes required, 1 s wait: a single-node cluster answers GENERIC_INSUFFICIENT_RESOURCES, the not-ready state.
const notReadySession = { required_workers_count: "2", required_workers_max_wait_time: "1s" }

const NATION = "SELECT count(*) AS n FROM tpch.tiny.nation"

const timeout = 120_000

it.layer(
  TrinoContainer.layerClientWith().pipe(
    Layer.provideMerge(Layer.mergeAll(coordinator, parkedWorker)),
    Layer.provideMerge(network)
  ),
  { timeout: "5 minutes", excludeTestServices: true }
)("TrinoClient resume", (it) => {
  // Runs first: the worker is still parked, so the cluster has a single node.
  it.effect("past the resume window the statement fails with a connection error", () =>
    Effect.gen(function*() {
      const trino = yield* TrinoContainer
      const { exchanges, http } = faultyClient(yield* HttpClient.HttpClient)
      // The window must outlast one not-ready round trip (POST, queued GET, the 1 s worker wait, the
      // FAILED answer) plus the engine's first 1 s backoff; on 476 that round trip alone exceeded 2 s.
      const { sql } = yield* clientOn(trino, http, { sessionProperties: notReadySession, resumeWindow: "10 seconds" })
      const error = yield* Effect.flip(sql.unsafe(NATION))
      assert.strictEqual(error._tag, "SqlError")
      assert.strictEqual(error.reason._tag, "ConnectionError", error.reason.message)
      // Resubmitted at least once inside the window before giving up.
      const posts = exchanges.filter((exchange) => exchange.method === "POST")
      assert.isAtLeast(
        posts.length,
        2,
        `submissions at ${posts.map((exchange) => exchange.at - posts[0].at).join(", ")} ms; ${error.reason.message}`
      )
    }).pipe(Effect.scoped), timeout)

  it.effect("resubmits while the cluster is not ready and succeeds once a worker joins", () =>
    Effect.gen(function*() {
      const trino = yield* TrinoContainer
      const { exchanges, http } = faultyClient(yield* HttpClient.HttpClient)
      const { source, sql } = yield* clientOn(trino, http, {
        sessionProperties: notReadySession,
        resumeWindow: "100 seconds"
      })
      const posts = () => exchanges.filter((exchange) => exchange.method === "POST").length
      const fiber = yield* Effect.forkChild(sql.unsafe<{ n: bigint }>(NATION))
      // Bounds below sum to 100 s, under the 120 s test budget, so a slow step fails with its own
      // diagnostics instead of the test timeout: resubmission seen ≤15 s, worker active ≤45 s, statement ≤40 s.
      yield* Effect.suspend(() => posts() >= 2 ? Effect.void : Effect.fail("not resubmitted yet")).pipe(
        Effect.retry({ schedule: Schedule.spaced("100 millis").pipe(Schedule.upTo({ duration: "15 seconds" })) })
      )
      yield* joinWorker
      // After the join: the engine's next resubmission comes at most 10 s later (its backoff cap), and the
      // statement then runs on a worker that has not run a query yet.
      const [row] = yield* Fiber.join(fiber).pipe(
        Effect.timeout("40 seconds"),
        Effect.catchTag("TimeoutError", () =>
          Effect.gen(function*() {
            const attempts = exchanges.map((exchange) =>
              `${exchange.method} ${exchange.url.replace(/^https?:\/\/[^/]+/, "")} -> ${exchange.status}`
            )
            return yield* Effect.fail(
              new Error(
                `statement unfinished 40 s after the worker joined; ${posts()} submissions\n${
                  attempts.slice(-15).join("\n")
                }\nqueries: ${JSON.stringify(yield* queriesFrom(source))}\nnodes: ${JSON.stringify(yield* nodes)}`
              )
            )
          }))
      )
      assert.strictEqual(row.n, 25n)
      assert.isAtLeast(posts(), 2)
    }).pipe(Effect.scoped), timeout)
})
