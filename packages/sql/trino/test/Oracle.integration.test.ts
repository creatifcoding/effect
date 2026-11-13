import { TrinoClient, TrinoTypes } from "@effect/sql-trino"
import { assert, it } from "@effect/vitest"
import { BigDecimal, DateTime, Duration, Effect, Option, Schema } from "effect"
import type { TrinoServer } from "./coordinator.ts"
import { layerClientIn, trinoCli, TrinoContainer, type ValueMode, valueModes } from "./utils.ts"

// The Trino CLI is the outside reader: it decodes with Trino's own Java client
// and prints with its own formatter, inside the same container. For every
// column the driver's decoded value is rendered the way the CLI renders the
// Java client's value, and the two texts must be equal.
//
// Normalisation, from trino-cli and trino-client sources:
// - JSON output (`JsonPrinter`) writes numbers as JSON numbers (read here with
//   their source text), VARBINARY as space-separated hex pairs, every other
//   scalar as the Java client's string form; NaN/Infinity as strings.
// - JSON output cannot print ARRAY, MAP or ROW (`writeObject` without a codec;
//   observed on 484 for arrays too), so those columns are read from TSV output
//   (`FormatUtils.formatValue`: `{k=v, ...}`, `{name=value, ...}`, `[a, b]`,
//   nested null as `NULL`).
// - The CLI advertises every client capability; the driver its defaults. Day
//   to second intervals are therefore compared as milliseconds.
// - Both value modes are compared. The default decoding keeps the server's
//   text for decimals, dates, times and intervals, and drops sub-millisecond
//   digits and zones of timestamps, so those are compared as far as it keeps them.

type CliValue =
  | null
  | boolean
  | string
  | { readonly number: string }
  | ReadonlyArray<CliValue>
  | { readonly [key: string]: CliValue }

// JSON.parse loses number digits, so number tokens are quoted with a marker
// before parsing and turned back into their source text afterwards.
const NUMBER_MARKER = "\u0001n:"

const parseCliLine = (line: string): Record<string, CliValue> => {
  let out = ""
  let inString = false
  let escaped = false
  for (let i = 0; i < line.length; i++) {
    const c = line[i]
    if (inString) {
      out += c
      if (escaped) escaped = false
      else if (c === "\\") escaped = true
      else if (c === "\"") inString = false
    } else if (c === "\"") {
      inString = true
      out += c
    } else if (c === "-" || (c >= "0" && c <= "9")) {
      let j = i
      while (j < line.length && /[-+0-9.eE]/.test(line[j])) j++
      out += JSON.stringify(NUMBER_MARKER + line.slice(i, j))
      i = j - 1
    } else {
      out += c
    }
  }
  const revive = (value: unknown): CliValue => {
    if (typeof value === "string" && value.startsWith(NUMBER_MARKER)) {
      return { number: value.slice(NUMBER_MARKER.length) }
    }
    if (Array.isArray(value)) return value.map(revive)
    if (value !== null && typeof value === "object") {
      return Object.fromEntries(Object.entries(value).map(([key, inner]) => [key, revive(inner)]))
    }
    return value as CliValue
  }
  return revive(JSON.parse(out)) as Record<string, CliValue>
}

type TypeSignature = TrinoTypes.TypeSignature

const pad = (value: number | bigint, width = 2) => String(value).padStart(width, "0")

const precisionOf = (signature: TypeSignature) => {
  const argument = signature.arguments[0]
  return argument !== undefined && argument.kind === "LONG" ? argument.value : 3
}

const fraction = (picosOfSecond: bigint, precision: number) =>
  precision === 0 ? "" : `.${pad(picosOfSecond, 12).slice(0, precision)}`

const wallClock = (parts: DateTime.DateTime.Parts, picosOfMilli: number, precision: number) =>
  `${pad(parts.year, 4)}-${pad(parts.month)}-${pad(parts.day)} ${pad(parts.hour)}:${pad(parts.minute)}:${
    pad(parts.second)
  }${fraction(BigInt(parts.millisecond) * 1_000_000_000n + BigInt(picosOfMilli), precision)}`

const timeText = (time: TrinoTypes.Time, precision: number) => {
  const seconds = time.picosOfDay / 1_000_000_000_000n
  const offset = time.offsetMinutes === undefined
    ? ""
    : `${time.offsetMinutes < 0 ? "-" : "+"}${pad(Math.floor(Math.abs(time.offsetMinutes) / 60))}:${
      pad(Math.abs(time.offsetMinutes) % 60)
    }`
  return `${pad(seconds / 3600n)}:${pad((seconds / 60n) % 60n)}:${pad(seconds % 60n)}${
    fraction(time.picosOfDay % 1_000_000_000_000n, precision)
  }${offset}`
}

const hexPairs = (bytes: Uint8Array) => Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(" ")

// Java's Double.toString: plain notation in [1e-3, 1e7), otherwise
// d.dddE±n; always at least one fraction digit. Both runtimes print the
// shortest round-tripping digits.
const javaDouble = (value: number): string => {
  if (Number.isNaN(value)) return "NaN"
  if (!Number.isFinite(value)) return value > 0 ? "Infinity" : "-Infinity"
  if (value === 0) return Object.is(value, -0) ? "-0.0" : "0.0"
  const magnitude = Math.abs(value)
  if (magnitude >= 1e-3 && magnitude < 1e7) {
    const text = String(value)
    return text.includes(".") ? text : `${text}.0`
  }
  const [mantissa, exponent] = value.toExponential().split("e")
  return `${mantissa.includes(".") ? mantissa : `${mantissa}.0`}E${Number(exponent)}`
}

const typeArgument = (signature: TypeSignature, index: number): TypeSignature => {
  const argument = signature.arguments[index]
  if (argument === undefined || (argument.kind !== "TYPE" && argument.kind !== "NAMED_TYPE")) {
    throw new Error(`${signature.rawType} has no type argument ${index}`)
  }
  return argument.value
}

/** The Java client's string form of a scalar the CLI prints as a JSON string. */
const javaText = (mode: ValueMode, signature: TypeSignature, value: unknown): string => {
  switch (signature.rawType) {
    case "decimal":
      return mode === "default" ? value as string : BigDecimal.format(value as BigDecimal.BigDecimal)
    case "varbinary":
      return hexPairs(value as Uint8Array)
    case "date":
      return mode === "default"
        ? value as string
        : wallClock(DateTime.toPartsUtc(value as DateTime.Utc), 0, 0).slice(0, 10)
    case "time":
    case "time with time zone":
      return mode === "default" ? value as string : timeText(value as TrinoTypes.Time, precisionOf(signature))
    case "timestamp": {
      // the default decoding is a Date: milliseconds at most
      const precision = mode === "default" ? Math.min(precisionOf(signature), 3) : precisionOf(signature)
      if (value instanceof Date) return wallClock(DateTime.toPartsUtc(DateTime.makeUnsafe(value)), 0, precision)
      return value instanceof TrinoTypes.PreciseTimestamp
        ? wallClock(DateTime.toPartsUtc(value.dateTime), value.picosOfMilli, precision)
        : wallClock(DateTime.toPartsUtc(value as DateTime.Utc), 0, precision)
    }
    case "timestamp with time zone": {
      const precision = precisionOf(signature)
      const [zoned, picos] = value instanceof TrinoTypes.PreciseTimestamp
        ? [value.dateTime as DateTime.Zoned, value.picosOfMilli]
        : [value as DateTime.Zoned, 0]
      return `${wallClock(DateTime.toParts(zoned), picos, precision)} ${DateTime.zoneToString(zoned.zone)}`
    }
    case "interval year to month": {
      if (mode === "default") return value as string
      const months = (value as TrinoTypes.IntervalYearMonth).months
      return `${months < 0 ? "-" : ""}${Math.floor(Math.abs(months) / 12)}-${Math.abs(months) % 12}`
    }
    case "bigint":
    case "integer":
    case "smallint":
    case "tinyint":
    case "boolean":
      return String(value)
    case "double":
    case "real":
      return javaDouble(value as number)
    default:
      return value as string
  }
}

/** The CLI's TSV form (`FormatUtils.formatValue`), used for MAP and ROW columns. */
const tsvText = (mode: ValueMode, signature: TypeSignature, value: unknown, nested: boolean): string => {
  if (value === null) return nested ? "NULL" : ""
  switch (signature.rawType) {
    case "array":
      return `[${
        (value as ReadonlyArray<unknown>).map((item) => tsvText(mode, typeArgument(signature, 0), item, true)).join(
          ", "
        )
      }]`
    case "map":
      return `{${
        Array.from(value as ReadonlyMap<unknown, unknown>, ([key, item]) =>
          `${tsvText(mode, typeArgument(signature, 0), key, true)}=${
            tsvText(mode, typeArgument(signature, 1), item, true)
          }`).join(
            ", "
          )
      }}`
    case "row":
      return `{${
        signature.arguments.map((argument, index) => {
          if (argument.kind !== "NAMED_TYPE") throw new Error("row field without a type")
          const name = argument.fieldName ?? `field${index}`
          const item = tsvText(mode, argument.value, (value as Record<string, unknown>)[name], true)
          return argument.fieldName === undefined ? item : `${argument.fieldName}=${item}`
        }).join(", ")
      }}`
    case "interval day to second":
      return String(
        mode === "default" ? intervalMillis(value as string) : Duration.toMillis(value as Duration.Duration)
      )
    default:
      return javaText(mode, signature, value)
  }
}

// Day-to-second interval text, "[-]D HH:MM:SS.fff[fff]", as milliseconds.
const intervalMillis = (text: string) => {
  const match = /^(-)?(\d+) (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?$/.exec(text)
  if (match === null) throw new Error(`Unexpected interval text ${text}`)
  const [, sign, days, hours, minutes, seconds, digits = ""] = match
  const millis = ((Number(days) * 24 + Number(hours)) * 60 + Number(minutes)) * 60_000 + Number(seconds) * 1000 +
    Number(digits.padEnd(3, "0").slice(0, 3))
  return sign === "-" ? -millis : millis
}

/** Describes a mismatch between the CLI's and the driver's reading of one value, or undefined when they agree. */
const compare = (
  mode: ValueMode,
  signature: TypeSignature,
  cli: CliValue,
  value: unknown,
  path: string
): string | undefined => {
  const differ = (expected: string, actual: string) =>
    expected === actual ? undefined : `${path} (${signature.rawType}): CLI ${expected}, driver ${actual}`
  if (cli === null || value === null) return differ(cli === null ? "null" : JSON.stringify(cli), String(value))
  switch (signature.rawType) {
    case "array": {
      const items = cli as ReadonlyArray<CliValue>
      const values = value as ReadonlyArray<unknown>
      if (items.length !== values.length) return differ(`${items.length} items`, `${values.length} items`)
      for (let i = 0; i < items.length; i++) {
        const mismatch = compare(mode, typeArgument(signature, 0), items[i], values[i], `${path}[${i}]`)
        if (mismatch !== undefined) return mismatch
      }
      return undefined
    }
    case "double":
    case "real": {
      const text = typeof cli === "string" ? cli : (cli as { number: string }).number
      const expected = Number(text)
      return Object.is(expected, value) || (Number.isNaN(expected) && Number.isNaN(value))
        ? undefined
        : differ(text, String(value))
    }
    case "decimal": {
      const expected = BigDecimal.fromStringUnsafe(cli as string)
      const actual = mode === "default"
        ? BigDecimal.fromStringUnsafe(value as string)
        : value as BigDecimal.BigDecimal
      return BigDecimal.equals(expected, actual)
        ? undefined
        : differ(cli as string, BigDecimal.format(actual))
    }
    case "interval day to second":
      return differ(
        String(intervalMillis(cli as string)),
        String(mode === "default" ? intervalMillis(value as string) : Duration.toMillis(value as Duration.Duration))
      )
    case "timestamp": {
      // the default decoding keeps milliseconds: compare the CLI's digits as far as the driver has them
      const actual = javaText(mode, signature, value)
      return differ((cli as string).slice(0, actual.length), actual)
    }
    case "timestamp with time zone": {
      if (mode === "effect") break
      // the default decoding is the instant: resolve the CLI's wall clock and zone to one
      const match = /^(\S+ \S+) (\S+)$/.exec(cli as string)
      const zone = match === null ? Option.none() : DateTime.zoneFromString(match[2])
      const instant = match === null || Option.isNone(zone)
        ? Option.none()
        : DateTime.makeZoned(Date.parse(`${match[1].slice(0, 23).replace(" ", "T")}Z`), {
          timeZone: zone.value,
          adjustForTimeZone: true
        })
      return Option.isSome(instant)
        ? differ(String(instant.value.epochMilliseconds), String((value as Date).getTime()))
        : differ(String(cli), "an unparsable zoned timestamp")
    }
  }
  return differ(
    typeof cli === "object" && cli !== null && "number" in cli ? String(cli.number) : String(cli),
    javaText(mode, signature, value)
  )
}

const driverRows = Effect.fnUntraced(function*(sql: string) {
  const client = yield* TrinoClient.TrinoClient
  return (yield* client.unsafe(sql).raw) as TrinoClient.RawResult
})

/** Runs `sql` through the driver and the CLI (JSON) and fails with every mismatch, both renderings included. */
const agreeJson = Effect.fnUntraced(function*(mode: ValueMode, container: TrinoServer, sql: string) {
  const raw = yield* driverRows(sql)
  const cliRows = (yield* trinoCli(container, sql, "JSON")).map(parseCliLine)
  assert.strictEqual(cliRows.length, raw.rows.length, `row count for ${sql}`)
  const mismatches: Array<string> = []
  raw.rows.forEach((row, r) =>
    raw.columns.forEach((column, c) => {
      const mismatch = compare(
        mode,
        column.typeSignature,
        cliRows[r][column.name] ?? null,
        row[c],
        `row ${r} ${column.name}`
      )
      if (mismatch !== undefined) mismatches.push(mismatch)
    })
  )
  assert.deepStrictEqual(mismatches, [], `${sql}\nCLI: ${JSON.stringify(cliRows)}`)
})

/** Runs `sql` through the driver and the CLI (TSV) and compares every value's text. */
const agreeTsv = Effect.fnUntraced(function*(mode: ValueMode, container: TrinoServer, sql: string) {
  const raw = yield* driverRows(sql)
  const cliRows = (yield* trinoCli(container, sql, "TSV")).map((line) => line.split("\t"))
  const driver = raw.rows.map((row) =>
    raw.columns.map((column, c) => tsvText(mode, column.typeSignature, row[c], false))
  )
  assert.deepStrictEqual(driver, cliRows, sql)
})

const timeout = 120_000

const corpus = [
  // integers at their edges
  `SELECT TINYINT '-128' AS a, TINYINT '127' AS b, SMALLINT '-32768' AS c, INTEGER '2147483647' AS d,
     BIGINT '-9223372036854775808' AS e, BIGINT '9223372036854775807' AS f, BIGINT '9007199254740993' AS g`,
  // floating point
  `SELECT DOUBLE '1.5' AS a, DOUBLE '-0.0' AS b, nan() AS c, infinity() AS d, -infinity() AS e,
     DOUBLE '1e300' AS f, DOUBLE '5e-324' AS g, REAL '1.1' AS h, REAL '3.4028235e38' AS i, DOUBLE '0.1' AS j`,
  // decimals across precision and scale
  `SELECT DECIMAL '0' AS a, DECIMAL '-12.30' AS b, DECIMAL '99999999999999999999999999999999999999' AS c,
     DECIMAL '0.00000000000000000000000000000000000001' AS d, CAST(1 AS decimal(38, 10)) AS e,
     DECIMAL '-123456789012345678.123456789012345678' AS f`,
  // text, binary and text-backed types
  `SELECT 'O''Brien' AS a, '' AS b, 'naïve 東京 🚀' AS c, CAST('ab' AS char(5)) AS d, X'' AS e, X'00ff10' AS f,
     JSON '{"a":[1,2.5,"x"]}' AS g, UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS h,
     IPADDRESS '2001:db8::1' AS i, IPADDRESS '10.0.0.1' AS j, true AS k, CAST(NULL AS varchar) AS l`,
  // dates and times
  `SELECT DATE '1970-01-01' AS a, DATE '1582-10-15' AS b, DATE '9999-12-31' AS c, DATE '0001-01-01' AS d,
     TIME '00:00:00' AS e, TIME '23:59:59.999999999999' AS f, TIME '10:00:00.123+05:30' AS g,
     CAST(TIME '01:02:03' AS time(0)) AS h`,
  // timestamps at every precision class
  `SELECT TIMESTAMP '2026-11-11 10:00:00' AS a, TIMESTAMP '2026-11-11 10:00:00.1' AS b,
     TIMESTAMP '2026-11-11 10:00:00.123' AS c, TIMESTAMP '2026-11-11 10:00:00.123456' AS d,
     TIMESTAMP '2026-11-11 10:00:00.123456789' AS e, TIMESTAMP '2026-11-11 10:00:00.123456789012' AS f,
     TIMESTAMP '1969-12-31 23:59:59.999999' AS g, TIMESTAMP '1582-10-15 00:00:00.000' AS h`,
  // zoned timestamps: IANA zones, offsets, DST edges
  `SELECT TIMESTAMP '2026-11-11 10:00:00.123 Europe/Dublin' AS a, TIMESTAMP '2026-07-11 10:00:00 Europe/Dublin' AS b,
     TIMESTAMP '2026-03-29 00:59:59.999 Europe/London' AS c, TIMESTAMP '2026-03-29 02:00:00 Europe/London' AS c2,
     TIMESTAMP '2026-10-25 01:30:00 Europe/London' AS d,
     TIMESTAMP '2026-11-11 10:00:00.123456 +05:30' AS e, TIMESTAMP '2026-11-11 10:00:00 UTC' AS f,
     TIMESTAMP '2026-11-11 10:00:00.123456789 America/New_York' AS g`,
  // intervals, both kinds, both signs
  `SELECT INTERVAL '3' MONTH AS a, INTERVAL '-1-2' YEAR TO MONTH AS b, INTERVAL '1 02:03:04.005' DAY TO SECOND AS c,
     INTERVAL '-0 00:00:00.001' DAY TO SECOND AS d, INTERVAL '100' DAY AS e`
]

// The CLI's JSON printer fails on any collection on 484 ("No ObjectCodec
// defined"), so arrays, maps and rows are read from its TSV output.
// The default decoding of a timestamp stops at milliseconds, so there the array holds a millisecond timestamp.
const tsvCorpus = (mode: ValueMode) => [
  // arrays of scalars, nested, with nulls
  `SELECT ARRAY[1, NULL, 3] AS a, ARRAY[ARRAY[BIGINT '9007199254740993'], ARRAY[]] AS b,
     ARRAY[DOUBLE '1.5', NULL, DOUBLE '1e10'] AS c, ARRAY[TIMESTAMP '2026-11-11 10:00:00.123${
    mode === "default" ? "" : "456"
  }'] AS d,
     ARRAY['a''b', ''] AS e`,
  `SELECT MAP(ARRAY['a'], ARRAY[BIGINT '1']) AS a, MAP(ARRAY[1], ARRAY[ARRAY['x', NULL]]) AS b,
     MAP(ARRAY[DATE '2026-11-11'], ARRAY[true]) AS c`,
  `SELECT CAST(ROW(1, 'x') AS ROW(id integer, name varchar)) AS a, ROW(1, ROW(2, NULL)) AS b,
     CAST(ROW(ARRAY[ROW(1, MAP(ARRAY['k'], ARRAY[NULL]))]) AS ROW(items array(row(n integer, m map(varchar, bigint))))) AS c`,
  `SELECT ARRAY[MAP(ARRAY['k'], ARRAY[ROW(1, 'v')])] AS a, CAST(NULL AS map(varchar, bigint)) AS b`
]

const Generated = Schema.Struct({
  big: Schema.BigInt.check(Schema.isBetweenBigInt({ minimum: -(2n ** 63n), maximum: 2n ** 63n - 1n })),
  dbl: Schema.Finite,
  text: Schema.String,
  unscaled: Schema.BigInt.check(Schema.isBetweenBigInt({ minimum: -(10n ** 38n - 1n), maximum: 10n ** 38n - 1n })),
  day: Schema.Int.check(Schema.isBetween({ minimum: -719_162, maximum: 2_932_896 })),
  micros: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 86_400_000_000 - 1 })),
  precision: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 12 }))
})

const quote = (text: string) => `'${text.replaceAll("'", "''")}'`

const decimalText = (unscaled: bigint, scale: number) => {
  const digits = (unscaled < 0n ? -unscaled : unscaled).toString().padStart(scale + 1, "0")
  const body = scale === 0 ? digits : `${digits.slice(0, digits.length - scale)}.${digits.slice(digits.length - scale)}`
  return unscaled < 0n ? `-${body}` : body
}

// VALUES unifies each column to one type, so every row of a run shares one
// decimal(38, scale); a mix of scales would widen past 38 digits and fail.
const generatedSql = (scale: number, rows: ReadonlyArray<typeof Generated.Type>) => {
  const values = rows.map((row, index) => {
    const date = new Date(row.day * 86_400_000)
    const day = date.toISOString().slice(0, 10)
    const micros = row.micros
    const time = `${pad(Math.floor(micros / 3_600_000_000))}:${pad(Math.floor(micros / 60_000_000) % 60)}:${
      pad(Math.floor(micros / 1_000_000) % 60)
    }.${pad(micros % 1_000_000, 6)}`
    // Strings without NUL and lone surrogates, which Trino's UTF-8 varchar cannot carry.
    const text = row.text.replaceAll("\u0000", "").replace(
      /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g,
      "\uFFFD"
    )
    return `(${index}, BIGINT '${row.big}', DOUBLE '${row.dbl}', ${quote(text)}, CAST(DECIMAL '${
      decimalText(row.unscaled, scale)
    }' AS decimal(38, ${scale})), DATE '${day}', CAST(TIMESTAMP '${day} ${time}' AS timestamp(${row.precision})))`
  })
  return `SELECT * FROM (VALUES ${values.join(", ")}) AS t(idx, big, dbl, text, dec, day, ts) ORDER BY idx`
}

for (const mode of valueModes) {
  it.layer(layerClientIn(mode), { timeout: "3 minutes", excludeTestServices: true })(
    `TrinoClient oracle (${mode})`,
    (it) => {
      it.effect(
        "decodes a fixed corpus of every scalar type exactly as the Trino CLI reads it",
        () =>
          Effect.gen(function*() {
            const container = yield* TrinoContainer
            for (const sql of corpus) {
              yield* agreeJson(mode, container, sql)
            }
          }),
        timeout
      )

      it.effect("decodes maps, rows and nesting exactly as the Trino CLI prints them", () =>
        Effect.gen(function*() {
          const container = yield* TrinoContainer
          for (const sql of tsvCorpus(mode)) {
            yield* agreeTsv(mode, container, sql)
          }
        }), timeout)

      // Each run costs one CLI start (a JVM, a few seconds), so few runs carry many
      // rows, and shrinking is bounded so a counterexample is reported within the timeout.
      it.effect.prop(
        "decodes generated rows exactly as the Trino CLI reads them",
        {
          scale: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 38 })),
          rows: Schema.Array(Generated).check(Schema.isMinLength(1), Schema.isMaxLength(25))
        },
        Effect.fnUntraced(function*({ rows, scale }) {
          const container = yield* TrinoContainer
          yield* agreeJson(mode, container, generatedSql(scale, rows))
        }),
        { timeout, arbitrary: { runs: 4, maxShrinks: 10 } }
      )
    }
  )
}
