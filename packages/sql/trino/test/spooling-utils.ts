/**
 * A spooling Trino backed by MinIO, for `Spooling.integration.test.ts`.
 *
 * Segments are stored unencrypted (`fs.segment.encryption=false`): S3 refuses
 * SSE-C, the spooling manager's default, over plain HTTP. MinIO is addressed by
 * its container IP on the shared network, which is the only address both the
 * coordinator (writing segments, signing URIs) and the test process (following
 * pre-signed URIs) can reach under the same `Host`, as the signature requires.
 * That needs the Linux Docker bridge, where the host routes to container IPs.
 */
import type { TrinoClient } from "@effect/sql-trino"
import { Context, Effect, Layer, Schedule } from "effect"
import { GenericContainer, Network, type StartedTestContainer, Wait } from "testcontainers"
import { ContainerError, layerClientFor, MINIO_IMAGE, startTrino } from "./utils.ts"

export type RetrievalMode = "storage" | "coordinator_storage_redirect" | "coordinator_proxy"

const ACCESS_KEY = "spooling"
const SECRET_KEY = "spooling-secret"
const REGION = "us-east-1"
const BUCKET = "spooling"
const SHARED_SECRET = "jxTKysfCBuMZtFqUf8UJDQ1w9ez8rynEJsJqgJf66u0="

const toHex = (bytes: ArrayBuffer) => Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("")

const sha256 = async (text: string) => toHex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)))

const hmac = async (key: ArrayBuffer | Uint8Array, text: string) => {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    key as BufferSource,
    { name: "HMAC", hash: "SHA-256" },
    false,
    [
      "sign"
    ]
  )
  return crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(text))
}

/** `PUT /<bucket>` signed with AWS Signature Version 4: MinIO has no anonymous bucket creation. */
const createBucket = async (endpoint: string) => {
  const url = new URL(`/${BUCKET}`, endpoint)
  const now = new Date().toISOString().replaceAll(/[-:]/g, "").replace(/\.\d{3}/, "")
  const day = now.slice(0, 8)
  const payloadHash = await sha256("")
  const signedHeaders = "host;x-amz-content-sha256;x-amz-date"
  const canonical = [
    "PUT",
    url.pathname,
    "",
    `host:${url.host}`,
    `x-amz-content-sha256:${payloadHash}`,
    `x-amz-date:${now}`,
    "",
    signedHeaders,
    payloadHash
  ].join("\n")
  const scope = `${day}/${REGION}/s3/aws4_request`
  const toSign = ["AWS4-HMAC-SHA256", now, scope, await sha256(canonical)].join("\n")
  let key = await hmac(new TextEncoder().encode(`AWS4${SECRET_KEY}`), day)
  for (const part of [REGION, "s3", "aws4_request"]) key = await hmac(key, part)
  const signature = toHex(await hmac(key, toSign))
  const response = await fetch(url, {
    method: "PUT",
    headers: {
      "x-amz-content-sha256": payloadHash,
      "x-amz-date": now,
      authorization:
        `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
    }
  })
  if (!response.ok) {
    throw new BucketRefused(response.status, await response.text())
  }
}

/** MinIO answered CreateBucket with an error status (it was reachable). */
class BucketRefused extends Error {
  readonly status: number
  readonly body: string
  constructor(status: number, body: string) {
    super(`CreateBucket answered ${status}: ${body}`)
    this.status = status
    this.body = body
  }
}

const acquire = <A>(start: () => Promise<A>, stop: (a: A) => Promise<unknown>) =>
  Effect.acquireRelease(
    Effect.tryPromise({ try: start, catch: (cause) => new ContainerError({ cause }) }),
    (a) => Effect.promise(() => stop(a))
  )

/** The coordinator, its storage, and the storage address segment URIs point at. */
export interface SpoolingCluster {
  readonly trino: StartedTestContainer
  readonly minio: StartedTestContainer
  /** `host:port` of MinIO as written into pre-signed URIs. */
  readonly storageHost: string
  readonly retrievalMode: RetrievalMode
}

export class Spooling extends Context.Service<Spooling, SpoolingCluster>()("test/Spooling") {
  static readonly layer = (retrievalMode: RetrievalMode) =>
    Layer.effect(this)(
      Effect.gen(function*() {
        const network = yield* acquire(() => new Network().start(), (n) => n.stop())
        const minio = yield* acquire(
          () =>
            new GenericContainer(MINIO_IMAGE)
              .withNetwork(network)
              .withEnvironment({ MINIO_ROOT_USER: ACCESS_KEY, MINIO_ROOT_PASSWORD: SECRET_KEY })
              .withCommand(["server", "/data"])
              .withExposedPorts(9000)
              // `ready`, not `live`: a live server still answers 503 XMinioServerNotInitialized
              .withWaitStrategy(Wait.forHttp("/minio/health/ready", 9000).forStatusCode(200))
              .start(),
          (c) => c.stop()
        )
        const storageHost = `${minio.getIpAddress(network.getName())}:9000`
        yield* Effect.tryPromise({
          try: () => createBucket(`http://${storageHost}`),
          catch: (cause) =>
            new ContainerError({
              cause: cause instanceof BucketRefused
                ? cause
                : new Error(`MinIO at its container IP ${storageHost} is unreachable from the test process`, {
                  cause
                })
            })
        }).pipe(
          // initialization can outlast the readiness probe; 503 means "not yet", bounded at 10 s
          Effect.retry({
            while: (error) => error.cause instanceof BucketRefused && error.cause.status === 503,
            schedule: Schedule.spaced("250 millis"),
            times: 40
          })
        )
        const trino = yield* startTrino({
          config: `protocol.spooling.enabled=true
protocol.spooling.shared-secret-key=${SHARED_SECRET}
protocol.spooling.retrieval-mode=${retrievalMode}
`,
          files: {
            "spooling-manager.properties": `spooling-manager.name=filesystem
fs.s3.enabled=true
fs.location=s3://${BUCKET}/
fs.segment.encryption=false
s3.endpoint=http://${storageHost}
s3.region=${REGION}
s3.aws-access-key=${ACCESS_KEY}
s3.aws-secret-key=${SECRET_KEY}
s3.path-style-access=true
`
          },
          configure: (container) => container.withNetwork(network)
        })
        return { trino, minio, storageHost, retrievalMode }
      })
    )

  static readonly layerClientWith = (config?: Partial<TrinoClient.TrinoClientConfig>) =>
    Layer.unwrap(Effect.map(Spooling, (cluster) => layerClientFor(cluster.trino, config)))
}

/** Segment objects still in the bucket (MinIO keeps one `xl.meta` per object). */
export const storedSegments = Effect.gen(function*() {
  const { minio } = yield* Spooling
  const result = yield* Effect.tryPromise({
    try: () => minio.exec(["sh", "-c", `find /data/${BUCKET} -name xl.meta | wc -l`]),
    catch: (cause) => new ContainerError({ cause })
  })
  return Number(result.stdout.trim())
})
