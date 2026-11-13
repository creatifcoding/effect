import {
  type ExecResult,
  GenericContainer,
  getContainerRuntimeClient,
  type StartedTestContainer,
  Wait
} from "testcontainers"

// Plain promises and no driver imports: globalSetup loads this module outside the test workers.

// `TRINO_VERSION` selects one; digests pin the image the suites run on.
const TRINO_IMAGES = {
  "484": "trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f",
  "476": "trinodb/trino@sha256:00125e40d063bc4816d165482f6044872b18b56026fb959d3b28ce1f96ffbbee"
} as const

export const TRINO_VERSION: keyof typeof TRINO_IMAGES = process.env.TRINO_VERSION === "476" ? "476" : "484"

export const TRINO_IMAGE = TRINO_IMAGES[TRINO_VERSION]

/** What the suites need from a coordinator: a started container, or the shared one reached by container id. */
export interface TrinoServer {
  readonly getHost: () => string
  readonly getMappedPort: (port: number) => number
  readonly exec: (command: Array<string>) => Promise<ExecResult>
}

/** Files written into `/etc/trino` before start; `config` lines are appended to the image's `config.properties`. */
export interface TrinoFiles {
  readonly config?: string | undefined
  readonly files?: Readonly<Record<string, string>> | undefined
}

/** The default access control refuses `SET SESSION AUTHORIZATION`, which the session suites use. */
export const sharedFiles: TrinoFiles = {
  files: { "access-control.properties": "access-control.name=allow-all\n" }
}

/** Succeeds inside a Trino container once `/v1/info` reports `starting: false` (the images lack grep). */
export const serverStartedProbe =
  `case "$(curl -s http://localhost:8080/v1/info)" in *'"starting":false'*) true ;; *) false ;; esac`

// Started is not ready: 476 fails its first queries with NO_NODES_AVAILABLE or "nodes is empty". Ready means a
// query that schedules splits runs, or the server refuses the CLI over plain HTTP (a password-authenticated one).
const coordinatorReadyProbe =
  `${serverStartedProbe} && { out=$(trino --execute 'SELECT count(*) FROM tpch.tiny.nation' 2>&1) || case "$out" in *'Authentication over HTTP is not enabled'*) true ;; *) false ;; esac; }`

/**
 * A coordinator that is also its only worker, ready once it runs queries. Readiness is probed inside the
 * container: testcontainers' HTTP wait fails the start under Bun.
 */
export const trinoContainer = (options: TrinoFiles = {}): GenericContainer =>
  new GenericContainer(TRINO_IMAGE)
    .withExposedPorts(8080)
    // 476's JDK 24 ignores the cgroup limit without cgroup v2 memory delegation and is OOM-killed;
    // an explicit -Xmx wins on every version.
    .withResourcesQuota({ memory: 2 })
    .withCopyContentToContainer([
      ...Object.entries(options.files ?? {}).map(([name, content]) => ({ content, target: `/etc/trino/${name}` })),
      { content: options.config ?? "", target: "/tmp/extra-config.properties" },
      { content: "-Xmx1536M\n", target: "/tmp/extra-jvm.config" }
    ])
    .withCommand([
      "sh",
      "-c",
      "cat /tmp/extra-config.properties >> /etc/trino/config.properties && cat /tmp/extra-jvm.config >> /etc/trino/jvm.config && exec /usr/lib/trino/bin/launcher run --etc-dir /etc/trino"
    ])
    .withWaitStrategy(Wait.forSuccessfulCommand(coordinatorReadyProbe).withStartupTimeout(180_000))

/** How a test worker finds the coordinator globalSetup started. */
export interface SharedCoordinator {
  readonly host: string
  readonly port: number
  readonly containerId: string
}

declare module "vitest" {
  export interface ProvidedContext {
    // Absent when a file runs outside the repo's vitest config (no globalSetup).
    readonly trinoCoordinator?: SharedCoordinator
  }
}

export const describeShared = (container: StartedTestContainer): SharedCoordinator => ({
  host: container.getHost(),
  port: container.getMappedPort(8080),
  containerId: container.getId()
})

export const reachShared = (shared: SharedCoordinator): TrinoServer => ({
  getHost: () => shared.host,
  getMappedPort: () => shared.port,
  exec: async (command) => {
    const client = await getContainerRuntimeClient()
    return client.container.exec(client.container.getById(shared.containerId), command)
  }
})
