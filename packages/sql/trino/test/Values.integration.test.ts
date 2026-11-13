/**
 * Every Trino type, generated, in both directions against a real coordinator:
 *
 * - literal: the test writes the SQL literal itself (never the driver), the
 *   server evaluates it, and the driver's decoded value must equal the value
 *   the generator started from;
 * - parameter: the expected value is bound through the driver
 *   (`EXECUTE IMMEDIATE … USING`), cast to the column type, selected back, and
 *   must decode to itself.
 *
 * Every property runs in both value modes: the pg-style default (`Date`,
 * decimals and the other date and time types as text) and the Effect types of
 * `TrinoTypes.makeEffectRegistry()`. Values are compared through `canon`,
 * which spells every type out (instant and zone, normalized decimal digits,
 * nanoseconds, map entries, row fields in order), so `-0`, `NaN`, zones and
 * field order all count.
 */
import { TrinoClient, TrinoTypes } from "@effect/sql-trino"
import { QueryFailed } from "@effect/sql-trino/internal/failure"
import { TypedValue } from "@effect/sql-trino/internal/literals"
import { assert, it } from "@effect/vitest"
import { Arbitrary, BigDecimal, DateTime, Duration, Effect, Result, Schema } from "effect"
import { SqlError } from "effect/sql"
import { layerClientIn, layerTimeout, runs, TRINO_VERSION, type ValueMode, valueModes } from "./utils.ts"

// -----------------------------------------------------------------------------
// comparison
// -----------------------------------------------------------------------------

/**
 * Trino 484 refuses a zoned wall time inside a daylight-saving gap: INVALID_LITERAL,
 * with the daylight-saving reason only in the failure's cause chain.
 */
const isDaylightSavingRefusal = (error: unknown) => {
  if (!(error instanceof SqlError.SqlError)) return false
  const failure = error.reason.cause
  return failure instanceof QueryFailed && failure.error.errorName === "INVALID_LITERAL" &&
    /daylight sav/i.test(JSON.stringify(failure.error.failureInfo))
}

const hex = (bytes: Uint8Array) => Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")

const canon = (value: unknown): unknown => {
  if (value === null || value === undefined) return null
  switch (typeof value) {
    case "number":
      return Number.isNaN(value) ? "NaN" : Object.is(value, -0) ? "-0" : value
    case "bigint":
      return `${value}n`
    case "string":
    case "boolean":
      return value
  }
  if (value instanceof Uint8Array) return { bytes: hex(value) }
  if (value instanceof Date) return { date: value.getTime() }
  if (BigDecimal.isBigDecimal(value)) return { decimal: BigDecimal.format(BigDecimal.normalize(value)) }
  if (DateTime.isDateTime(value)) {
    return {
      instant: DateTime.toEpochMillis(value),
      zone: DateTime.isZoned(value) ? DateTime.zoneToString(value.zone) : "Utc"
    }
  }
  if (value instanceof TrinoTypes.PreciseTimestamp) {
    return { precise: canon(value.dateTime), picosOfMilli: value.picosOfMilli }
  }
  if (value instanceof TrinoTypes.Time) {
    return { picosOfDay: `${value.picosOfDay}`, offsetMinutes: value.offsetMinutes ?? null }
  }
  if (value instanceof TrinoTypes.IntervalYearMonth) return { months: value.months + 0 }
  if (Duration.isDuration(value)) return { nanos: `${Duration.toNanosUnsafe(value)}` }
  if (Array.isArray(value)) return value.map(canon)
  if (value instanceof Map) {
    // Trino's map order is its hash order, not insertion order
    return {
      map: [...value].map(([k, v]) => [canon(k), canon(v)]).sort(([a], [b]) =>
        JSON.stringify(a) < JSON.stringify(b) ? -1 : 1
      )
    }
  }
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    // a ROW: field names and positions both matter
    return { row: Object.entries(value).map(([k, v]) => [k, canon(v)]) }
  }
  return { unexpected: Object.prototype.toString.call(value) }
}

/** An object whose keys may include `__proto__`, as decoded rows are. */
const record = (entries: ReadonlyArray<readonly [string, unknown]>): Record<string, unknown> => {
  const out: Record<string, unknown> = {}
  for (const [key, value] of entries) {
    Object.defineProperty(out, key, { value, enumerable: true, writable: true, configurable: true })
  }
  return out
}

// -----------------------------------------------------------------------------
// samples
// -----------------------------------------------------------------------------

/** What the driver must decode in each value mode. */
interface Modal {
  readonly default: unknown
  readonly effect: unknown
}

const both = (value: unknown): Modal => ({ default: value, effect: value })

const modal = (expected: (mode: ValueMode) => unknown): Modal => ({
  default: expected("default"),
  effect: expected("effect")
})

interface Sample {
  /** Column type; parameters are cast to it. */
  readonly type: string
  /** SQL that makes the server produce the value, written by the test. */
  readonly literal: string
  readonly expected: Modal
  /** What is bound through the driver; defaults to the Effect-mode `expected`, which the encoder accepts in both modes. */
  readonly param?: unknown
  /** `raw`: the parameter is selected as is (a `TypedValue` already typed). */
  readonly bind?: "cast" | "raw"
  /** Applied to the decoded value before comparison. */
  readonly project?: (decoded: unknown, mode: ValueMode) => unknown
}

const quote = (text: string) => `'${text.replaceAll("'", "''")}'`
const pad = (n: number | bigint, width: number) => `${n}`.padStart(width, "0")

const int = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })))

const pick = <A>(values: ReadonlyArray<A>): Arbitrary.Arbitrary<A> =>
  Arbitrary.map(int(0, values.length - 1), (i) => values[i])

const digitString = (length: number): Arbitrary.Arbitrary<string> =>
  length === 0
    ? Arbitrary.Constant("")
    : Arbitrary.map(Arbitrary.array(int(0, 9), { minLength: length, maxLength: length }), (ds) => ds.join(""))

const sample = <const Input extends Record<string, Arbitrary.Arbitrary<any>>>(
  input: Input,
  f: (
    value: { -readonly [K in keyof Input]: Input[K] extends Arbitrary.Arbitrary<infer A> ? A : never }
  ) => Sample
): Arbitrary.Arbitrary<Sample> => Arbitrary.map(Arbitrary.all(input), f as (value: unknown) => Sample)

// --- numbers -----------------------------------------------------------------

const booleanSample = sample({ b: Arbitrary.schema(Schema.Boolean) }, ({ b }) => ({
  type: "boolean",
  literal: String(b),
  expected: both(b)
}))

const integerSample = (type: "tinyint" | "smallint" | "integer", bits: number) =>
  sample({ n: int(-(2 ** (bits - 1)), 2 ** (bits - 1) - 1) }, ({ n }) => ({
    type,
    literal: `${type.toUpperCase()} '${n}'`,
    expected: both(n)
  }))

const BIGINT_MIN = -(BigInt(2) ** BigInt(63))
const BIGINT_MAX = BigInt(2) ** BigInt(63) - BigInt(1)
const bigintEdges = [BIGINT_MIN, BIGINT_MIN + BigInt(1), BigInt(-1), BigInt(0), BigInt(2 ** 53) + BigInt(1), BIGINT_MAX]

const bigintValue = Arbitrary.flatMap(
  Arbitrary.schema(Schema.Boolean),
  (edge) =>
    edge
      ? pick(bigintEdges)
      : Arbitrary.schema(Schema.BigInt.check(Schema.isBetweenBigInt({ minimum: BIGINT_MIN, maximum: BIGINT_MAX })))
)

const bigintSample = sample(
  { n: bigintValue },
  ({ n }) => ({ type: "bigint", literal: `BIGINT '${n}'`, expected: both(n) })
)

const doubleText = (n: number) =>
  Number.isNaN(n)
    ? "nan()"
    : n === Infinity
    ? "infinity()"
    : n === -Infinity
    ? "-infinity()"
    : `DOUBLE '${Object.is(n, -0) ? "-0" : n}'`

const doubleSample = (finite: boolean) =>
  sample({ n: Arbitrary.schema(finite ? Schema.Finite : Schema.Number) }, ({ n }) => ({
    type: "double",
    literal: doubleText(n),
    expected: both(n)
  }))

const realSample = sample(
  { n: Arbitrary.filter(Arbitrary.map(Arbitrary.schema(Schema.Number), Math.fround), (f) => Number.isFinite(f)) },
  ({ n }) => ({
    type: "real",
    literal: `REAL '${Object.is(n, -0) ? "-0" : n}'`,
    expected: both(n),
    // REAL travels as Java's Float.toString digits; equal as a float
    project: (decoded) => typeof decoded === "number" ? Math.fround(decoded) : decoded
  })
)

const decimalText = (unscaled: bigint, scale: number) => {
  const negative = unscaled < BigInt(0)
  const digits = pad(negative ? -unscaled : unscaled, scale + 1)
  const text = scale === 0 ? digits : `${digits.slice(0, -scale)}.${digits.slice(-scale)}`
  return negative ? `-${text}` : text
}

const decimalSampleOf = (precision: number, scale: number) =>
  sample(
    { digits: Arbitrary.flatMap(int(1, precision), digitString), negative: Arbitrary.schema(Schema.Boolean) },
    ({ digits, negative }) => {
      const unscaled = negative ? -BigInt(digits) : BigInt(digits)
      return {
        type: `decimal(${precision},${scale})`,
        literal: `CAST('${decimalText(unscaled, scale)}' AS decimal(${precision},${scale}))`,
        expected: { default: decimalText(unscaled, scale), effect: BigDecimal.make(unscaled, scale) }
      }
    }
  )

const decimalSample = Arbitrary.flatMap(
  int(1, 38),
  (precision) => Arbitrary.flatMap(int(0, precision), (scale) => decimalSampleOf(precision, scale))
)

const specialNumbers = { NaN, Infinity, "-Infinity": -Infinity } as const

/** The default decoding of NUMBER is its text; the test compares the value it denotes. */
const numberValue = (decoded: unknown, mode: ValueMode): unknown => {
  if (mode !== "default" || typeof decoded !== "string") return decoded
  if (decoded === "NaN") return NaN
  if (decoded === "Infinity" || decoded === "+Infinity") return Infinity
  if (decoded === "-Infinity") return -Infinity
  return BigDecimal.fromStringUnsafe(decoded)
}

const numberSample = Arbitrary.flatMap(
  Arbitrary.schema(Schema.Boolean),
  (special): Arbitrary.Arbitrary<Sample> =>
    special
      ? Arbitrary.map(pick(["NaN", "Infinity", "-Infinity"] as const), (text) => ({
        type: "number",
        literal: `NUMBER '${text}'`,
        expected: both(specialNumbers[text]),
        project: numberValue
      }))
      : Arbitrary.map(decimalSampleOf(38, 10), (s) => ({
        type: "number",
        literal: `NUMBER '${decimalText((s.expected.effect as BigDecimal.BigDecimal).value, 10)}'`,
        expected: both(s.expected.effect),
        project: numberValue
      }))
)

// --- text and bytes ----------------------------------------------------------

const textValue = Arbitrary.map(
  Arbitrary.all({
    s: Arbitrary.filter(Arbitrary.schema(Schema.String), (s) => !/\p{Cs}/u.test(s) && !s.includes("\u0000")),
    quoted: Arbitrary.schema(Schema.Boolean)
  }),
  // a quote in every other value: an undoubled quote breaks the statement
  ({ quoted, s }) => quoted ? `'${s}''` : s
)

const varcharSample = sample({ s: textValue }, ({ s }) => ({ type: "varchar", literal: quote(s), expected: both(s) }))

const asciiValue = Arbitrary.map(
  Arbitrary.array(int(32, 126), { maxLength: 10 }),
  (codes) => String.fromCharCode(...codes)
)

const charSample = sample({ s: asciiValue, extra: int(0, 5) }, ({ extra, s }) => {
  const length = Math.max(1, s.length + extra)
  return {
    type: `char(${length})`,
    literal: `CAST(${quote(s)} AS char(${length}))`,
    expected: both(s.padEnd(length))
  }
})

const bytesValue = Arbitrary.map(Arbitrary.array(int(0, 255), { maxLength: 24 }), (b) => new Uint8Array(b))

const varbinarySample = sample({ bytes: bytesValue }, ({ bytes }) => ({
  type: "varbinary",
  literal: `X'${hex(bytes)}'`,
  expected: both(bytes)
}))

const jsonValue: Arbitrary.Arbitrary<unknown> = Arbitrary.map(
  Arbitrary.array(
    Arbitrary.all({ key: pick(["a", "b", "c", "d"]), value: int(-1000, 1000), text: asciiValue }),
    { maxLength: 4 }
  ),
  (entries) => Object.fromEntries(entries.map(({ key, text, value }) => [key, [value, text, value % 2 === 0, null]]))
)

/** JSON text with keys sorted at every level, so key order does not count. */
const sortedJson = (value: unknown): string =>
  JSON.stringify(value, (_, v) =>
    v !== null && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a < b ? -1 : 1))
      : v)

const jsonSample = sample({ value: jsonValue }, ({ value }) => {
  const text = JSON.stringify(value)
  return {
    type: "json",
    literal: `JSON ${quote(text)}`,
    expected: both(sortedJson(value)),
    param: new TypedValue("json", text),
    bind: "raw",
    project: (decoded) => typeof decoded === "string" ? sortedJson(JSON.parse(decoded)) : decoded
  }
})

const uuidSample = sample({ bytes: Arbitrary.array(int(0, 255), { minLength: 16, maxLength: 16 }) }, ({ bytes }) => {
  const h = hex(new Uint8Array(bytes))
  const text = `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
  return { type: "uuid", literal: `UUID '${text}'`, expected: both(text) }
})

const ipSample = sample({ parts: Arbitrary.array(int(0, 255), { minLength: 4, maxLength: 4 }) }, ({ parts }) => {
  const text = parts.join(".")
  return { type: "ipaddress", literal: `IPADDRESS '${text}'`, expected: both(text) }
})

// --- dates and times ---------------------------------------------------------

interface Civil {
  readonly year: number
  readonly month: number
  readonly day: number
}

const daysIn = (year: number, month: number) => {
  const lastDay = new Date(0)
  lastDay.setUTCFullYear(year, month, 0)
  return lastDay.getUTCDate()
}

const civilValue = (minYear: number, maxYear: number): Arbitrary.Arbitrary<Civil> =>
  Arbitrary.flatMap(
    Arbitrary.all({ year: int(minYear, maxYear), month: int(1, 12) }),
    ({ month, year }) => Arbitrary.map(int(1, daysIn(year, month)), (day) => ({ year, month, day }))
  )

/** Epoch milliseconds of a proleptic Gregorian date at midnight UTC. */
const civilMillis = ({ day, month, year }: Civil) => new Date(0).setUTCFullYear(year, month - 1, day)

const civilText = ({ day, month, year }: Civil) => `${pad(year, 4)}-${pad(month, 2)}-${pad(day, 2)}`

interface Clock {
  readonly hour: number
  readonly minute: number
  readonly second: number
  /** Fraction digits, `precision` of them. */
  readonly fraction: string
}

const clockValue = (precision: number): Arbitrary.Arbitrary<Clock> =>
  Arbitrary.all({ hour: int(0, 23), minute: int(0, 59), second: int(0, 59), fraction: digitString(precision) })

const clockText = ({ fraction, hour, minute, second }: Clock) =>
  `${pad(hour, 2)}:${pad(minute, 2)}:${pad(second, 2)}${fraction.length > 0 ? `.${fraction}` : ""}`

const PICOS_PER_SECOND = BigInt("1000000000000")

const clockPicos = ({ fraction, hour, minute, second }: Clock) =>
  BigInt((hour * 60 + minute) * 60 + second) * PICOS_PER_SECOND + BigInt(fraction.padEnd(12, "0"))

/** Milliseconds of the day and the picoseconds within the last millisecond. */
const clockMillis = (clock: Clock) =>
  [
    ((clock.hour * 60 + clock.minute) * 60 + clock.second) * 1000 + Number(clock.fraction.padEnd(3, "0").slice(0, 3)),
    Number(clock.fraction.padEnd(12, "0").slice(3))
  ] as const

const dateSample = sample({ civil: civilValue(1, 9999) }, ({ civil }) => ({
  type: "date",
  literal: `DATE '${civilText(civil)}'`,
  expected: { default: civilText(civil), effect: DateTime.makeUnsafe(civilMillis(civil)) }
}))

const offsetText = (minutes: number) =>
  `${minutes < 0 ? "-" : "+"}${pad(Math.floor(Math.abs(minutes) / 60), 2)}:${pad(Math.abs(minutes) % 60, 2)}`

const timeText = (clock: Clock, withZone: boolean, offset: number) =>
  `${clockText(clock)}${withZone ? offsetText(offset) : ""}`

const timeSample = (withZone: boolean) =>
  Arbitrary.flatMap(
    int(0, 12),
    (precision) =>
      sample({ clock: clockValue(precision), offset: int(-14 * 60, 14 * 60) }, ({ clock, offset }) => ({
        type: withZone ? `time(${precision}) with time zone` : `time(${precision})`,
        literal: `TIME '${timeText(clock, withZone, offset)}'`,
        expected: {
          default: timeText(clock, withZone, offset),
          effect: new TrinoTypes.Time({ picosOfDay: clockPicos(clock), offsetMinutes: withZone ? offset : undefined })
        }
      }))
  )

const instantOf = (epochMillis: number, picosOfMilli: number, precision: number, zone?: string) => {
  const dateTime = zone === undefined
    ? DateTime.makeUnsafe(epochMillis)
    : DateTime.makeZonedUnsafe(epochMillis, { timeZone: zone })
  return precision <= 3 ? dateTime : new TrinoTypes.PreciseTimestamp({ dateTime, picosOfMilli })
}

const timestampSampleOf = (precision: number) =>
  sample({ civil: civilValue(1, 9999), clock: clockValue(precision) }, ({ civil, clock }) => {
    const [millis, picos] = clockMillis(clock)
    return {
      type: `timestamp(${precision})`,
      literal: `TIMESTAMP '${civilText(civil)} ${clockText(clock)}'`,
      expected: {
        default: new Date(civilMillis(civil) + millis),
        effect: instantOf(civilMillis(civil) + millis, picos, precision)
      }
    }
  })

/** Zones whose instant the test computes itself: UTC and fixed offsets (never `+00:00`, which Trino calls UTC). */
const fixedZone = Arbitrary.flatMap(Arbitrary.schema(Schema.Boolean), (utc) =>
  utc
    ? Arbitrary.Constant({ text: "UTC", minutes: 0 })
    : Arbitrary.map(
      Arbitrary.filter(int(-14 * 60, 14 * 60), (minutes) => minutes !== 0),
      (minutes) => ({ text: offsetText(minutes), minutes })
    ))

const zonedTimestampSampleOf = (precision: number) =>
  sample(
    { civil: civilValue(2, 9998), clock: clockValue(precision), zone: fixedZone },
    ({ civil, clock, zone }) => {
      const [millis, picos] = clockMillis(clock)
      return {
        type: `timestamp(${precision}) with time zone`,
        literal: `TIMESTAMP '${civilText(civil)} ${clockText(clock)} ${zone.text}'`,
        expected: {
          default: new Date(civilMillis(civil) + millis - zone.minutes * 60_000),
          effect: instantOf(civilMillis(civil) + millis - zone.minutes * 60_000, picos, precision, zone.text)
        }
      }
    }
  )

const timestampSample = Arbitrary.flatMap(int(0, 12), timestampSampleOf)
const zonedTimestampSample = Arbitrary.flatMap(int(0, 12), zonedTimestampSampleOf)

// Trino 476 renders day-time intervals in milliseconds; 484 keeps the six
// digits of a DAY TO SECOND literal (and only with PARAMETRIC_INTERVAL).
const intervalDigits = TRINO_VERSION === "476" ? 3 : 6

const DAY_TIME = /^(-)?(\d+) (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?$/

const intervalPicos = (decoded: unknown) => {
  const match = typeof decoded === "string" ? DAY_TIME.exec(decoded) : null
  if (match === null) return decoded
  const picos = clockPicos({
    hour: Number(match[3]),
    minute: Number(match[4]),
    second: Number(match[5]),
    fraction: match[6] ?? ""
  }) + BigInt(match[2]) * BigInt(86_400) * PICOS_PER_SECOND
  return { intervalPicos: `${match[1] === "-" ? -picos : picos}` }
}

const dayTimeIntervalSample = sample(
  {
    negative: Arbitrary.schema(Schema.Boolean),
    days: int(0, 99),
    clock: Arbitrary.flatMap(int(0, intervalDigits), clockValue)
  },
  ({ clock, days, negative }) => {
    const text = `${days} ${clockText(clock)}`
    const picos = BigInt(days) * BigInt(86_400) * PICOS_PER_SECOND + clockPicos(clock)
    return {
      type: "interval day to second",
      literal: `${negative ? "-" : ""}INTERVAL '${text}' DAY TO SECOND`,
      expected: {
        default: { intervalPicos: `${negative ? -picos : picos}` },
        effect: Duration.nanos((negative ? -picos : picos) / BigInt(1000))
      },
      // the digits of a fraction depend on the server version; the value does not
      project: (decoded, mode) => mode === "default" ? intervalPicos(decoded) : decoded
    }
  }
)

const yearMonthIntervalSample = sample(
  { negative: Arbitrary.schema(Schema.Boolean), years: int(0, 99), months: int(0, 11) },
  ({ months, negative, years }) => {
    const total = years * 12 + months
    return {
      type: "interval year to month",
      literal: `${negative ? "-" : ""}INTERVAL '${years}-${months}' YEAR TO MONTH`,
      expected: {
        default: `${negative && total > 0 ? "-" : ""}${years}-${months}`,
        effect: new TrinoTypes.IntervalYearMonth({ months: negative && total > 0 ? -total : total })
      }
    }
  }
)

// --- nested ------------------------------------------------------------------

interface LeafShape {
  readonly _tag: "Leaf"
  readonly leaf: Arbitrary.Arbitrary<Sample>
  readonly type: string
}

type Shape =
  | LeafShape
  | { readonly _tag: "Array"; readonly element: Shape }
  | { readonly _tag: "Map"; readonly key: LeafShape; readonly value: Shape }
  | { readonly _tag: "Row"; readonly fields: ReadonlyArray<{ readonly name: string; readonly shape: Shape }> }

const leaf = (type: string, arbitrary: Arbitrary.Arbitrary<Sample>): LeafShape => ({
  _tag: "Leaf",
  leaf: arbitrary,
  type
})

const valueLeaves: ReadonlyArray<Shape> = [
  leaf("bigint", bigintSample),
  leaf("varchar", varcharSample),
  leaf("decimal(12,3)", decimalSampleOf(12, 3)),
  leaf("date", dateSample),
  leaf("double", doubleSample(true)),
  leaf("timestamp(6) with time zone", zonedTimestampSampleOf(6)),
  leaf("varbinary", varbinarySample)
]

// keys whose string form the server writes with Java's toString and the codec reads back by key type
const keyLeaves: ReadonlyArray<LeafShape> = [
  leaf("bigint", bigintSample),
  leaf("varchar", varcharSample),
  leaf("date", dateSample),
  leaf("decimal(12,3)", decimalSampleOf(12, 3)),
  leaf("boolean", booleanSample)
]

// out of alphabetical order, one of them `__proto__`: decoding by position must keep this order
const fieldNames = ["zulu", "alpha", "__proto__", "mike", "bravo"]

const shapeOf = (depth: number): Arbitrary.Arbitrary<Shape> =>
  depth === 0
    ? pick(valueLeaves)
    : Arbitrary.flatMap(int(0, 3), (kind): Arbitrary.Arbitrary<Shape> => {
      switch (kind) {
        case 0:
          return Arbitrary.map(shapeOf(depth - 1), (element) => ({ _tag: "Array", element }))
        case 1:
          return Arbitrary.map(
            Arbitrary.all({ key: pick(keyLeaves), value: shapeOf(depth - 1) }),
            ({ key, value }) => ({ _tag: "Map", key, value })
          )
        case 2:
          return Arbitrary.map(
            Arbitrary.all({
              count: int(1, 3),
              offset: int(0, fieldNames.length - 1),
              shapes: Arbitrary.array(shapeOf(depth - 1), { minLength: 3, maxLength: 3 })
            }),
            ({ count, offset, shapes }) => ({
              _tag: "Row",
              fields: shapes.slice(0, count).map((shape, i) => ({
                name: fieldNames[(offset + i) % fieldNames.length],
                shape
              }))
            })
          )
        default:
          return pick(valueLeaves)
      }
    })

const typeOf = (shape: Shape): string => {
  switch (shape._tag) {
    case "Leaf":
      return shape.type
    case "Array":
      return `array(${typeOf(shape.element)})`
    case "Map":
      return `map(${typeOf(shape.key)}, ${typeOf(shape.value)})`
    case "Row":
      return `row(${shape.fields.map((f) => `"${f.name}" ${typeOf(f.shape)}`).join(", ")})`
  }
}

interface Node {
  readonly literal: string
  readonly expected: Modal
}

const nodeOf = (shape: Shape): Arbitrary.Arbitrary<Node> => {
  const type = typeOf(shape)
  const present: Arbitrary.Arbitrary<Node> = (() => {
    switch (shape._tag) {
      case "Leaf":
        return Arbitrary.map(shape.leaf, (s) => ({ literal: s.literal, expected: s.expected }))
      case "Array":
        return Arbitrary.map(Arbitrary.array(nodeOf(shape.element), { maxLength: 3 }), (items) => ({
          literal: `CAST(ARRAY[${items.map((n) => n.literal).join(", ")}] AS ${type})`,
          expected: modal((mode) => items.map((n) => n.expected[mode]))
        }))
      case "Map":
        return Arbitrary.map(
          Arbitrary.array(
            Arbitrary.all({ key: shape.key.leaf, value: nodeOf(shape.value) }),
            { maxLength: 3 }
          ),
          (entries) => {
            const unique = new Map<string, (typeof entries)[number]>()
            for (const entry of entries) unique.set(JSON.stringify(canon(entry.key.expected.effect)), entry)
            const kept = [...unique.values()]
            return {
              literal: kept.length === 0
                ? `CAST(MAP() AS ${type})`
                : `CAST(MAP(ARRAY[${kept.map((e) => e.key.literal).join(", ")}], ARRAY[${
                  kept.map((e) => e.value.literal).join(", ")
                }]) AS ${type})`,
              expected: modal((mode) => new Map(kept.map((e) => [e.key.expected[mode], e.value.expected[mode]])))
            }
          }
        )
      case "Row":
        return Arbitrary.map(Arbitrary.all(shape.fields.map((f) => nodeOf(f.shape))), (values) => ({
          literal: `CAST(ROW(${values.map((n) => n.literal).join(", ")}) AS ${type})`,
          expected: modal((mode) => record(shape.fields.map((f, i) => [f.name, values[i].expected[mode]])))
        }))
    }
  })()
  return Arbitrary.flatMap(
    int(0, 9),
    (roll) => roll === 0 ? Arbitrary.Constant({ literal: `CAST(NULL AS ${type})`, expected: both(null) }) : present
  )
}

const nestedSample: Arbitrary.Arbitrary<Sample> = Arbitrary.flatMap(
  shapeOf(3),
  (shape) =>
    Arbitrary.map(nodeOf(shape), (node) => ({ type: typeOf(shape), literal: node.literal, expected: node.expected }))
)

// -----------------------------------------------------------------------------
// properties
// -----------------------------------------------------------------------------

const roundTrip = Effect.fnUntraced(function*(mode: ValueMode, samples: ReadonlyArray<Sample>) {
  const sql = yield* TrinoClient.TrinoClient
  const literalSql = `SELECT ${samples.map((s, i) => `${s.literal} AS c${i}`).join(", ")}`
  const [fromLiteral] = yield* sql.unsafe(literalSql).values
  const paramSql = `SELECT ${
    samples.map((s, i) => s.bind === "raw" ? `? AS c${i}` : `CAST(? AS ${s.type}) AS c${i}`).join(", ")
  }`
  const [fromParam] = yield* sql.unsafe(paramSql, samples.map((s) => s.param ?? s.expected.effect)).values
  samples.forEach((s, i) => {
    const project = s.project ?? ((v: unknown) => v)
    const expected = canon(s.expected[mode])
    assert.deepStrictEqual(canon(project(fromLiteral[i], mode)), expected, `${s.type} literal ${s.literal}`)
    assert.deepStrictEqual(canon(project(fromParam[i], mode)), expected, `${s.type} parameter of ${s.literal}`)
  })
})

const batch = (arbitrary: Arbitrary.Arbitrary<Sample>, maxLength = 5) =>
  Arbitrary.array(arbitrary, { minLength: 1, maxLength })

// A JVM coordinator shared by every property; each run is two statements.
const options = { timeout: 120_000, arbitrary: { runs: runs(40) } }

const scalars: ReadonlyArray<readonly [string, Arbitrary.Arbitrary<Sample>]> = [
  ["boolean", booleanSample],
  ["tinyint", integerSample("tinyint", 8)],
  ["smallint", integerSample("smallint", 16)],
  ["integer", integerSample("integer", 32)],
  ["bigint, ±2^63 edges included", bigintSample],
  ["double, NaN, ±Infinity and -0 included", doubleSample(false)],
  ["real", realSample],
  ["decimal(p, s) for every p ≤ 38", decimalSample],
  ["varchar with quotes, unicode and control characters", varcharSample],
  ["char(n) padding", charSample],
  ["varbinary", varbinarySample],
  ["json", jsonSample],
  ["uuid", uuidSample],
  ["ipaddress", ipSample],
  ["date, years 1 to 9999", dateSample],
  ["time(p), p = 0..12", timeSample(false)],
  ["time(p) with time zone", timeSample(true)],
  ["timestamp(p), p = 0..12", timestampSample],
  ["timestamp(p) with time zone, UTC and offsets", zonedTimestampSample],
  ["interval day to second, negatives included", dayTimeIntervalSample],
  ["interval year to month, negatives included", yearMonthIntervalSample],
  ...(TRINO_VERSION === "484" ? [["number (NUMBER capability)", numberSample] as const] : [])
]

// Region zones: the test cannot compute their offsets, so the server's own
// arithmetic is the oracle (milliseconds since the epoch of the truncated
// instant), around daylight-saving transitions included.
const regionZones = [
  "America/New_York",
  "Europe/Dublin",
  "Australia/Lord_Howe",
  "Asia/Kathmandu",
  "America/Sao_Paulo",
  "Pacific/Chatham"
]

const regionTimestamp = Arbitrary.flatMap(int(0, 12), (precision) =>
  Arbitrary.all({
    precision: Arbitrary.Constant(precision),
    // March, April, October and November hold most transitions
    civil: Arbitrary.flatMap(
      Arbitrary.all({ year: int(1950, 2050), month: pick([3, 4, 10, 11]) }),
      ({ month, year }) => Arbitrary.map(int(1, 28), (day) => ({ year, month, day }))
    ),
    clock: clockValue(precision),
    zone: pick(regionZones)
  }))

for (const mode of valueModes) {
  it.layer(layerClientIn(mode), { timeout: layerTimeout, excludeTestServices: true })(
    `Values (Trino ${TRINO_VERSION}, ${mode})`,
    (it) => {
      // the instant a zoned `DateTime` denotes, as the mode decodes it
      const asMode = (dateTime: DateTime.Zoned) =>
        mode === "default" ? new Date(DateTime.toEpochMillis(dateTime)) : dateTime

      for (const [name, arbitrary] of scalars) {
        it.effect.prop(name, { samples: batch(arbitrary) }, ({ samples }) => roundTrip(mode, samples), options)
      }

      it.effect.prop(
        "array, map and row nested 3 deep, nulls at every level",
        { samples: batch(nestedSample, 2) },
        ({ samples }) => roundTrip(mode, samples),
        { timeout: 120_000, arbitrary: { runs: runs(30) } }
      )

      it.effect.prop(
        "timestamp(p) with a region zone keeps the instant and the zone",
        { value: regionTimestamp },
        Effect.fnUntraced(function*({ value }) {
          const sql = yield* TrinoClient.TrinoClient
          const { civil, clock, precision, zone } = value
          const literal = `TIMESTAMP '${civilText(civil)} ${clockText(clock)} ${zone}'`
          const selected = yield* sql.unsafe(
            `SELECT ${literal}, date_diff('millisecond', TIMESTAMP '1970-01-01 00:00:00 UTC', date_trunc('millisecond', ${literal}))`
          ).values.pipe(
            Effect.map((rows) => rows[0]),
            // a wall time inside a daylight-saving gap does not exist: the server refuses the literal
            Effect.catchIf((error) => isDaylightSavingRefusal(error), () => Effect.succeed(undefined))
          )
          if (selected === undefined) return
          const [decoded, epochMillis] = selected
          const [, picos] = clockMillis(clock)
          const expected = mode === "default"
            ? new Date(Number(epochMillis))
            : instantOf(Number(epochMillis), picos, precision, zone)
          assert.deepStrictEqual(canon(decoded), canon(expected), literal)
          const type = `timestamp(${precision}) with time zone`
          const [[back]] = yield* sql.unsafe(`SELECT CAST(? AS ${type})`, [decoded]).values
          assert.deepStrictEqual(canon(back), canon(expected), `parameter of ${literal}`)
        }),
        options
      )

      it.effect("fixed edge values decode exactly", () =>
        roundTrip(mode, [
          { type: "bigint", literal: "BIGINT '-9223372036854775808'", expected: both(BIGINT_MIN) },
          { type: "bigint", literal: "BIGINT '9223372036854775807'", expected: both(BIGINT_MAX) },
          { type: "double", literal: "DOUBLE '-0'", expected: both(-0) },
          { type: "double", literal: "DOUBLE '4.9E-324'", expected: both(5e-324) },
          { type: "varchar", literal: "''", expected: both("") },
          { type: "varbinary", literal: "X''", expected: both(new Uint8Array()) },
          ...([[1582, 10, 4], [1582, 10, 15], [1, 1, 1]] as const).map(([year, month, day]): Sample => {
            const civil = { year, month, day }
            return {
              type: "date",
              literal: `DATE '${civilText(civil)}'`,
              expected: { default: civilText(civil), effect: DateTime.makeUnsafe(civilMillis(civil)) }
            }
          }),
          {
            type: "timestamp(12)",
            literal: "TIMESTAMP '9999-12-31 23:59:59.999999999999'",
            expected: {
              default: new Date(civilMillis({ year: 9999, month: 12, day: 31 }) + 86_399_999),
              effect: new TrinoTypes.PreciseTimestamp({
                dateTime: DateTime.makeUnsafe(civilMillis({ year: 9999, month: 12, day: 31 }) + 86_399_999),
                picosOfMilli: 999_999_999
              })
            }
          },
          { type: "ipaddress", literal: "IPADDRESS '2001:db8::1'", expected: both("2001:db8::1") },
          {
            type: "map(bigint, varchar)",
            literal: "MAP(ARRAY[BIGINT '9007199254740993'], ARRAY['x'])",
            expected: both(new Map([[BigInt("9007199254740993"), "x"]]))
          }
        ]), 120_000)

      it.effect(
        "the repeated hour of a fall-back: bound instants stay apart, the wire renders both as one wall time",
        () =>
          Effect.gen(function*() {
            const sql = yield* TrinoClient.TrinoClient
            // 05:30Z and 06:30Z are both 01:30 in New York on 2021-11-07
            const instants = [Date.UTC(2021, 10, 7, 5, 30), Date.UTC(2021, 10, 7, 6, 30)].map((ms) =>
              DateTime.makeZonedUnsafe(ms, { timeZone: "America/New_York" })
            )
            const [[earlier, later, apart]] = yield* sql.unsafe(
              `SELECT CAST(? AS timestamp(3) with time zone), CAST(? AS timestamp(3) with time zone),
               date_diff('millisecond', CAST(? AS timestamp(3) with time zone), CAST(? AS timestamp(3) with time zone))`,
              [...instants, ...instants]
            ).values
            // the parameters reached the server as two instants an hour apart
            assert.strictEqual(apart, BigInt(3_600_000))
            // Trino writes a zoned timestamp as wall clock + zone id, with no offset, so the
            // repeated hour cannot be told apart on the wire; both resolve to the earlier instant
            assert.deepStrictEqual(canon(earlier), canon(asMode(instants[0])))
            assert.deepStrictEqual(canon(later), canon(asMode(instants[0])))
          }),
        120_000
      )

      it.effect(
        "a wall time inside a spring-forward gap is refused, or decodes to the instant the server chose",
        () =>
          Effect.gen(function*() {
            const sql = yield* TrinoClient.TrinoClient
            const literal = "TIMESTAMP '2021-03-14 02:30:00.000 America/New_York'"
            const result = yield* Effect.result(
              sql.unsafe(
                `SELECT ${literal}, date_diff('millisecond', TIMESTAMP '1970-01-01 00:00:00 UTC', ${literal})`
              ).values
            )
            if (Result.isFailure(result)) {
              assert.isTrue(isDaylightSavingRefusal(result.failure), String(result.failure))
              return
            }
            // older servers move the wall time past the gap; the decoded value must be that instant
            const [[decoded, epochMillis]] = result.success
            assert.deepStrictEqual(
              canon(decoded),
              canon(asMode(DateTime.makeZonedUnsafe(Number(epochMillis), { timeZone: "America/New_York" })))
            )
          }),
        120_000
      )

      it.effect("columns keep their parametric types", () =>
        Effect.gen(function*() {
          const sql = yield* TrinoClient.TrinoClient
          const raw = (yield* sql.unsafe(
            `SELECT TIMESTAMP '2020-01-01 00:00:00.123456789' AS ts, TIME '00:00:00.123456' AS t${
              TRINO_VERSION === "484" ? ", NUMBER '1.5' AS n, INTERVAL '0 00:00:00.000001' DAY TO SECOND AS i" : ""
            }`
          ).raw) as TrinoClient.RawResult
          const types = raw.columns.map((column) => column.type)
          assert.strictEqual(types[0], "timestamp(9)")
          assert.strictEqual(types[1], "time(6)")
          if (TRINO_VERSION === "484") {
            // without NUMBER the column would be varchar; without PARAMETRIC_INTERVAL the value rounds to 0 ms
            assert.strictEqual(types[2], "number")
            assert.deepStrictEqual(
              mode === "default" ? intervalPicos(raw.rows[0][3]) : canon(raw.rows[0][3]),
              mode === "default" ? { intervalPicos: "1000000" } : { nanos: "1000" }
            )
          }
        }), 120_000)
    }
  )
}
