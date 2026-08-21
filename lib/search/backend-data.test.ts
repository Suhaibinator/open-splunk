import assert from "node:assert/strict";
import test from "node:test";

import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultColumn,
  type ResultRow,
  type ResultSchema,
} from "../../gen/ts/open_splunk/result";
import {
  NullValue,
  ValueType,
  type TypedValue,
} from "../../gen/ts/open_splunk/value";
import {
  adaptSearchResults,
  timechartValueFields,
  timechartRowsForExport,
} from "./backend-data";

function column(
  fieldName: string,
  valueType: ValueType,
  semanticType = ColumnSemanticType.COLUMN_SEMANTIC_TYPE_UNSPECIFIED,
): ResultColumn {
  return {
    fieldName,
    displayName: fieldName,
    valueType,
    semanticType,
    nullable: false,
    multivalue: false,
    hiddenByDefault: false,
	statsSparkline: false,
  };
}

function stringValue(value: string): TypedValue {
  return { kind: { $case: "stringValue", value } };
}

function uint64Value(value: bigint): TypedValue {
  return { kind: { $case: "uint64Value", value } };
}

function doubleValue(value: number): TypedValue {
  return { kind: { $case: "doubleValue", value } };
}

function timestampValue(value: string): TypedValue {
  return { kind: { $case: "timestampValue", value: new Date(value) } };
}

function countedTimestampValue(
  value: string,
  counter: { calls: number },
): TypedValue {
  const date = new Date(value);
  const toISOString = date.toISOString.bind(date);
  Object.defineProperty(date, "toISOString", {
    value() {
      counter.calls += 1;
      return toISOString();
    },
  });
  return { kind: { $case: "timestampValue", value: date } };
}

function row(rowId: string, ordinal: bigint, cells: TypedValue[]): ResultRow {
  return { rowId, ordinal, cells };
}

function trackDateTimeFormatConstructions<T>(
  action: () => T,
): { value: T; constructions: number } {
  const descriptor = Object.getOwnPropertyDescriptor(Intl, "DateTimeFormat");
  assert.ok(descriptor);
  let constructions = 0;
  const trackedDateTimeFormat = new Proxy(Intl.DateTimeFormat, {
    apply(target, thisArgument, argumentsList) {
      constructions += 1;
      return Reflect.apply(target, thisArgument, argumentsList);
    },
    construct(target, argumentsList) {
      constructions += 1;
      return Reflect.construct(target, argumentsList);
    },
  });
  Object.defineProperty(Intl, "DateTimeFormat", {
    ...descriptor,
    value: trackedDateTimeFormat,
  });
  try {
    return { value: action(), constructions };
  } finally {
    Object.defineProperty(Intl, "DateTimeFormat", descriptor);
  }
}

test("result adaptation rejects schemas wider than the browser contract", () => {
  const schema: ResultSchema = {
    schemaId: "too-wide-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: Array.from(
      { length: 65 },
      (_, index) => column(`field_${index}`, ValueType.VALUE_TYPE_UINT64),
    ),
  };
  assert.throws(
    () => adaptSearchResults(schema, []),
    /65 columns.*supports 1–64/,
  );
});

test("result adaptation rejects unsupported result kinds", () => {
  for (const resultKind of [
    ResultSetKind.RESULT_SET_KIND_UNSPECIFIED,
    ResultSetKind.UNRECOGNIZED,
  ]) {
    const schema: ResultSchema = {
      schemaId: `unsupported-${resultKind}`,
      revision: 1n,
      resultKind,
      columns: [column("message", ValueType.VALUE_TYPE_STRING)],
    };
    assert.throws(
      () => adaptSearchResults(schema, []),
      new RegExp(`unsupported result kind ${resultKind}`),
    );
  }
});

test("result adaptation rejects forged flat multivalue presentation metadata", () => {
  const schema: ResultSchema = {
    schemaId: "invalid-delimiter-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [{
      ...column("users", ValueType.VALUE_TYPE_STRING),
      multivalue: true,
      flatMultivalueDelimiter: ",",
    }],
  };
  assert.throws(
    () => adaptSearchResults(schema, []),
    /invalid multivalue presentation metadata/,
  );
});

test("event adaptation renders native nomv lists with canonical newlines", () => {
  const schema: ResultSchema = {
    schemaId: "events-nomv-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [{
      ...column("users", ValueType.VALUE_TYPE_LIST),
      multivalue: true,
      flatMultivalueDelimiter: "\n",
    }],
  };
  const adapted = adaptSearchResults(schema, [
    row("native", 0n, [{
      kind: {
        $case: "listValue",
        value: {
          values: [
            stringValue("alice"),
            { kind: { $case: "sint64Value", value: -7n } },
            { kind: { $case: "doubleValue", value: -0 } },
            { kind: { $case: "boolValue", value: true } },
            { kind: { $case: "nullValue", value: NullValue.NULL_VALUE_NULL } },
          ],
        },
      },
    }]),
  ]);

  assert.equal(adapted.events[0]?.fields.users, "alice\n-7\n-0\ntrue\nnull");
});

test("top message results retain count and percent as categorical series", () => {
  const schema: ResultSchema = {
    schemaId: "top-message-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column(
        "message",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_MESSAGE,
      ),
      column("count", ValueType.VALUE_TYPE_UINT64),
      column("percent", ValueType.VALUE_TYPE_DOUBLE),
    ],
  };
  const rows: ResultRow[] = [
    row("request-metrics", 0n, [
      stringValue("Request metrics"),
      uint64Value(4n),
      doubleValue(66.66666666666667),
    ]),
    row("heartbeat", 1n, [
      stringValue("heartbeat"),
      uint64Value(2n),
      doubleValue(33.333333333333336),
    ]),
  ];

  const adapted = adaptSearchResults(schema, rows);

  assert.equal(adapted.statisticDimension, "message");
  assert.deepEqual(
    adapted.statistics.map(({ level, count, percent, measureLabel }) => ({
      level,
      count,
      percent,
      measureLabel,
    })),
    [
      {
        level: "Request metrics",
        count: 4,
        percent: "66.7%",
        measureLabel: "count",
      },
      {
        level: "heartbeat",
        count: 2,
        percent: "33.3%",
        measureLabel: "count",
      },
    ],
  );
  assert.deepEqual(
    adapted.statisticsTable?.columns.map(({ fieldName, pivotable }) => ({
      fieldName,
      pivotable,
    })),
    [
      { fieldName: "message", pivotable: true },
      { fieldName: "count", pivotable: false },
      { fieldName: "percent", pivotable: false },
    ],
  );
  assert.equal(adapted.statisticsTable?.rows[0]?.values.percent, 66.66666666666667);
  assert.deepEqual(
    adapted.statistics[0]?.series?.map(({ key, label, value }) => ({ key, label, value })),
    [
      { key: "count", label: "count", value: 4 },
      { key: "percent", label: "percent", value: 66.66666666666667 },
    ],
  );
});

test("chronological aggregate names are not offered as pivot dimensions", () => {
  const schema: ResultSchema = {
    schemaId: "stats-chronological-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("service", ValueType.VALUE_TYPE_STRING),
      column("earliest(status)", ValueType.VALUE_TYPE_STRING),
      column("latest(path)", ValueType.VALUE_TYPE_STRING),
    ],
  };
  const adapted = adaptSearchResults(schema, [
    row("api", 0n, [
      stringValue("api"),
      stringValue("starting"),
      stringValue("/ready"),
    ]),
  ]);

  assert.deepEqual(
    adapted.statisticsTable?.columns.map(({ fieldName, pivotable }) => ({
      fieldName,
      pivotable,
    })),
    [
      { fieldName: "service", pivotable: true },
      { fieldName: "earliest(status)", pivotable: false },
      { fieldName: "latest(path)", pivotable: false },
    ],
  );
});

test("statistics adaptation retains optional flat multivalue metadata and typed cells", () => {
  const valuesColumn = {
    ...column("users", ValueType.VALUE_TYPE_LIST),
    multivalue: true,
    flatMultivalueDelimiter: "",
  };
  const listColumn = {
    ...column("hosts", ValueType.VALUE_TYPE_LIST),
    multivalue: true,
  };
  const schema: ResultSchema = {
    schemaId: "stats-delimiter-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [valuesColumn, listColumn],
  };
  const adapted = adaptSearchResults(schema, [
    row("all", 0n, [
      {
        kind: {
          $case: "listValue",
          value: { values: [stringValue("alice"), stringValue("bob")] },
        },
      },
      {
        kind: {
          $case: "listValue",
          value: { values: [stringValue("web-1"), stringValue("web-2")] },
        },
      },
    ]),
  ]);

  assert.equal(adapted.statisticsTable?.columns[0]?.flatMultivalueDelimiter, "");
  assert.equal(adapted.statisticsTable?.columns[1]?.flatMultivalueDelimiter, undefined);
  assert.deepEqual(adapted.statisticsTable?.rows[0]?.values.users, ["alice", "bob"]);
  assert.deepEqual(adapted.statisticsTable?.rows[0]?.values.hosts, ["web-1", "web-2"]);
});

test("runtime-wide chart results retain every split series in schema order", () => {
  const schema: ResultSchema = {
    schemaId: "chart-path-by-level-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("path", ValueType.VALUE_TYPE_STRING),
      column("ERROR", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("INFO", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("NULL", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("OTHER", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
    ],
  };
  const rows: ResultRow[] = [
    row("grade", 0n, [
      stringValue("/api/submissions/grade"),
      uint64Value(4n),
      uint64Value(18n),
      uint64Value(0n),
      uint64Value(2n),
    ]),
    row("courses", 1n, [
      stringValue("/api/courses"),
      uint64Value(9_007_199_254_740_993n),
      uint64Value(11n),
      uint64Value(1n),
      uint64Value(0n),
    ]),
  ];

  const adapted = adaptSearchResults(schema, rows);

  assert.equal(adapted.statisticDimension, "path");
  assert.equal(adapted.statistics.length, 2);
  assert.deepEqual(
    adapted.statistics[0]?.series?.map(({ key, value }) => ({ key, value })),
    [
      { key: "ERROR", value: 4 },
      { key: "INFO", value: 18 },
      { key: "NULL", value: 0 },
      { key: "OTHER", value: 2 },
    ],
  );
  assert.deepEqual(
    adapted.statistics[1]?.series?.map(({ key, exactValue, coordinateApproximate }) => ({
      key,
      exactValue,
      coordinateApproximate: coordinateApproximate ?? false,
    })),
    [
      {
        key: "ERROR",
        exactValue: "9007199254740993",
        coordinateApproximate: true,
      },
      { key: "INFO", exactValue: undefined, coordinateApproximate: false },
      { key: "NULL", exactValue: undefined, coordinateApproximate: false },
      { key: "OTHER", exactValue: undefined, coordinateApproximate: false },
    ],
  );
  assert.deepEqual(
    adapted.statisticsTable?.columns.map(({ fieldName }) => fieldName),
    ["path", "ERROR", "INFO", "NULL", "OTHER"],
  );
});

test("aliased multi-measure statistics retain every numeric measure including negative values", () => {
  const schema: ResultSchema = {
    schemaId: "multi-measure-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("level", ValueType.VALUE_TYPE_STRING, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_LEVEL),
      column("requests", ValueType.VALUE_TYPE_UINT64),
      column("latency", ValueType.VALUE_TYPE_DOUBLE),
      column("delta", ValueType.VALUE_TYPE_DOUBLE),
    ],
  };
  const rows: ResultRow[] = [
    row("info", 0n, [
      stringValue("INFO"),
      uint64Value(42n),
      doubleValue(18.75),
      doubleValue(-3.5),
    ]),
  ];

  const adapted = adaptSearchResults(schema, rows);

  assert.equal(adapted.statisticDimension, "level");
  assert.deepEqual(
    adapted.statistics[0]?.series?.map(({ label, value }) => ({ label, value })),
    [
      { label: "requests", value: 42 },
      { label: "latency", value: 18.75 },
      { label: "delta", value: -3.5 },
    ],
  );
});

test("explicit metrics retain an untagged aggregate sibling when one category remains", () => {
  const schema: ResultSchema = {
    schemaId: "mixed-metric-semantics-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("level", ValueType.VALUE_TYPE_STRING, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_LEVEL),
      column("count", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("avg(duration_ms)", ValueType.VALUE_TYPE_DOUBLE),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("warn", 0n, [stringValue("WARN"), uint64Value(12n), doubleValue(48.5)]),
  ]);

  assert.deepEqual(
    adapted.statistics[0]?.series?.map(({ key, value }) => ({ key, value })),
    [
      { key: "count", value: 12 },
      { key: "avg(duration_ms)", value: 48.5 },
    ],
  );
});

test("multiple grouping dimensions are rejected instead of silently collapsing one", () => {
  const schema: ResultSchema = {
    schemaId: "two-dimensions-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("host", ValueType.VALUE_TYPE_STRING, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_HOST),
      column("path", ValueType.VALUE_TYPE_STRING),
      column("count", ValueType.VALUE_TYPE_UINT64),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("host-path", 0n, [stringValue("api-01"), stringValue("/health"), uint64Value(8n)]),
  ]);

  assert.deepEqual(adapted.statistics, []);
  assert.deepEqual(
    adapted.statisticsTable?.columns.map(({ fieldName }) => fieldName),
    ["host", "path", "count"],
  );
});

test("runtime chart can use a numeric dimension named count without reclassifying it", () => {
  const schema: ResultSchema = {
    schemaId: "chart-count-dimension-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("count", ValueType.VALUE_TYPE_UINT64),
      column("ERROR", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("INFO", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("count-200", 0n, [uint64Value(200n), uint64Value(4n), uint64Value(12n)]),
  ]);

  assert.equal(adapted.statisticDimension, "count");
  assert.equal(adapted.statistics[0]?.level, "200");
  assert.deepEqual(
    adapted.statistics[0]?.series?.map(({ key }) => key),
    ["ERROR", "INFO"],
  );
});

test("numeric stats BY count is rejected instead of inverting an aliased measure", () => {
  const schema: ResultSchema = {
    schemaId: "stats-by-count-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("count", ValueType.VALUE_TYPE_UINT64),
      column("events", ValueType.VALUE_TYPE_DOUBLE),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("count-200", 0n, [uint64Value(200n), doubleValue(4.5)]),
  ]);

  assert.deepEqual(adapted.statistics, []);
  assert.deepEqual(
    adapted.statisticsTable?.columns.map(({ fieldName }) => fieldName),
    ["count", "events"],
  );
});

test("ambiguous all-numeric statistics do not invent a dimension or drop a measure", () => {
  const schema: ResultSchema = {
    schemaId: "ambiguous-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column("left", ValueType.VALUE_TYPE_DOUBLE),
      column("right", ValueType.VALUE_TYPE_DOUBLE),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("pair", 0n, [doubleValue(1), doubleValue(2)]),
  ]);

  assert.deepEqual(adapted.statistics, []);
});

test("statistics adaptation skips event-only projections", () => {
  const schema: ResultSchema = {
    schemaId: "statistics-only-projection-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "level",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_LEVEL,
      ),
      column(
        "count",
        ValueType.VALUE_TYPE_UINT64,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
      ),
    ],
  };
  const rows: ResultRow[] = [
    row("info", 0n, [
      timestampValue("2026-07-21T22:00:00.000Z"),
      stringValue("INFO"),
      uint64Value(4n),
    ]),
    row("error", 1n, [
      timestampValue("2026-07-21T22:05:00.000Z"),
      stringValue("ERROR"),
      uint64Value(2n),
    ]),
  ];
  const measured = trackDateTimeFormatConstructions(
    () => adaptSearchResults(schema, rows),
  );
  const adapted = measured.value;

  assert.equal(measured.constructions, 0);
  assert.deepEqual(adapted.events, []);
  assert.deepEqual(adapted.fields, []);
  assert.deepEqual(adapted.timeline, []);
  assert.equal(adapted.statisticDimension, "level");
  assert.deepEqual(
    adapted.statistics.map(({ level, count }) => ({ level, count })),
    [
      { level: "INFO", count: 4 },
      { level: "ERROR", count: 2 },
    ],
  );
  assert.deepEqual(
    adapted.statisticsTable?.columns.map(({ fieldName }) => fieldName),
    ["_time", "level", "count"],
  );
});

test("statistics adaptation does not decode raw event payloads", () => {
  let rawValueReads = 0;
  const guardedRawValue: TypedValue = {
    kind: {
      $case: "objectValue",
      get value() {
        rawValueReads += 1;
        return { fields: [] };
      },
    },
  };
  const schema: ResultSchema = {
    schemaId: "statistics-raw-sentinel-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      column(
        "_raw",
        ValueType.VALUE_TYPE_OBJECT,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      ),
      column(
        "level",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_LEVEL,
      ),
      column(
        "count",
        ValueType.VALUE_TYPE_UINT64,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
      ),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("error", 0n, [guardedRawValue, stringValue("ERROR"), uint64Value(2n)]),
  ]);

  assert.equal(rawValueReads, 0);
  assert.equal(adapted.statistics[0]?.level, "ERROR");
  assert.equal(adapted.statistics[0]?.count, 2);
  assert.equal(adapted.statisticsTable, null);
});

test("event adaptation builds only the event projection", () => {
  const schema: ResultSchema = {
    schemaId: "event-projections-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "_raw",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      ),
      column(
        "level",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_LEVEL,
      ),
    ],
  };

  const adapted = adaptSearchResults(schema, [
    row("event-1", 0n, [
      timestampValue("2026-07-21T22:00:00.000Z"),
      stringValue("request failed"),
      stringValue("ERROR"),
    ]),
  ]);

  assert.equal(adapted.events[0]?.raw, "request failed");
  assert.deepEqual(adapted.fields, []);
  assert.deepEqual(adapted.statistics, []);
  assert.deepEqual(adapted.timeline, []);
  assert.equal(adapted.statisticsTable, null);
});

test("event adaptation preserves non-finite doubles as non-pivotable table values", () => {
  const schema: ResultSchema = {
    schemaId: "event-ieee-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      column("_raw", ValueType.VALUE_TYPE_STRING, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW),
      column("nan_value", ValueType.VALUE_TYPE_DOUBLE),
      column("positive_infinity", ValueType.VALUE_TYPE_DOUBLE),
      column("negative_infinity", ValueType.VALUE_TYPE_DOUBLE),
    ],
  };
  const adapted = adaptSearchResults(schema, [
    row("ieee", 0n, [
      stringValue("computed IEEE values"),
      doubleValue(Number.NaN),
      doubleValue(Number.POSITIVE_INFINITY),
      doubleValue(Number.NEGATIVE_INFINITY),
    ]),
  ]);

  const event = adapted.events[0];
  assert.ok(event);
  assert.ok(Object.is(event.fields.nan_value, Number.NaN));
  assert.equal(event.fields.positive_infinity, Number.POSITIVE_INFINITY);
  assert.equal(event.fields.negative_infinity, Number.NEGATIVE_INFINITY);
  assert.equal(event.pivotableFields?.nan_value, false);
  assert.equal(event.pivotableFields?.positive_infinity, false);
  assert.equal(event.pivotableFields?.negative_infinity, false);
});

test("event adaptation decodes each nested timestamp value once", () => {
  const schema: ResultSchema = {
    schemaId: "event-single-decode-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "_raw",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      ),
      column("fields", ValueType.VALUE_TYPE_OBJECT),
      column("payload", ValueType.VALUE_TYPE_LIST),
    ],
  };
  const eventTime = { calls: 0 };
  const flattenedFieldTime = { calls: 0 };
  const listTime = { calls: 0 };
  const timestamp = "2026-07-21T22:00:00.000Z";
  const adapted = adaptSearchResults(schema, [
    row("single-decode", 0n, [
      countedTimestampValue(timestamp, eventTime),
      stringValue("single decode"),
      {
        kind: {
          $case: "objectValue",
          value: {
            fields: [{
              name: "nested_time",
              value: countedTimestampValue(timestamp, flattenedFieldTime),
            }],
          },
        },
      },
      {
        kind: {
          $case: "listValue",
          value: {
            values: [countedTimestampValue(timestamp, listTime)],
          },
        },
      },
    ]),
  ]);

  assert.equal(eventTime.calls, 1);
  assert.equal(flattenedFieldTime.calls, 1);
  assert.equal(listTime.calls, 1);
  assert.equal(adapted.events[0]?.fields.nested_time, timestamp);
  assert.equal(adapted.events[0]?.fields.payload, JSON.stringify([timestamp]));
});

test("event adaptation reuses one date-time formatter", () => {
  const schema: ResultSchema = {
    schemaId: "event-formatter-reuse-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "_raw",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      ),
      column(
        "level",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_LEVEL,
      ),
    ],
  };
  const firstTimestamp = Date.parse("2026-07-21T22:00:00.000Z");
  const rows = Array.from({ length: 1_000 }, (_, index) => {
    const timestamp = new Date(firstTimestamp + index * 1_000).toISOString();
    return row(`event-${index}`, BigInt(index), [
      timestampValue(timestamp),
      stringValue(`event ${index}`),
      stringValue(index % 2 === 0 ? "INFO" : "ERROR"),
    ]);
  });
  const expectedFirstLabel = new Intl.DateTimeFormat("en-US", {
    month: "numeric",
    day: "numeric",
    year: "2-digit",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  }).format(new Date(firstTimestamp));
  const measured = trackDateTimeFormatConstructions(
    () => adaptSearchResults(schema, rows),
  );

  assert.equal(measured.constructions, 1);
  assert.equal(measured.value.events.length, rows.length);
  assert.equal(measured.value.events[0]?.timeLabel, expectedFirstLabel);
  assert.equal(
    measured.value.events.at(-1)?.time,
    new Date(firstTimestamp + (rows.length - 1) * 1_000).toISOString(),
  );
  assert.deepEqual(measured.value.fields, []);
  assert.deepEqual(measured.value.statistics, []);
  assert.deepEqual(measured.value.timeline, []);
});

test("empty and invalid event times do not construct date-time formatters", () => {
  const schema: ResultSchema = {
    schemaId: "invalid-event-times-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "_raw",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      ),
    ],
  };

  const empty = trackDateTimeFormatConstructions(
    () => adaptSearchResults(schema, []),
  );
  const invalid = trackDateTimeFormatConstructions(
    () => adaptSearchResults(schema, [
      row("invalid-time", 0n, [
        stringValue("not-a-timestamp"),
        stringValue("invalid timestamp"),
      ]),
    ]),
  );

  assert.equal(empty.constructions, 0);
  assert.equal(invalid.constructions, 0);
  assert.equal(invalid.value.events[0]?.time, "");
  assert.equal(invalid.value.events[0]?.timeLabel, "Time unavailable");
  assert.deepEqual(invalid.value.timeline, []);
});

test("empty and invalid time-series rows do not construct date-time formatters", () => {
  const schema: ResultSchema = {
    schemaId: "invalid-time-series-times-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "count",
        ValueType.VALUE_TYPE_UINT64,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
      ),
    ],
  };

  const empty = trackDateTimeFormatConstructions(
    () => adaptSearchResults(schema, []),
  );
  const invalid = trackDateTimeFormatConstructions(
    () => adaptSearchResults(schema, [
      row("invalid-bucket-time", 0n, [
        stringValue("not-a-timestamp"),
        uint64Value(1n),
      ]),
    ]),
  );

  assert.equal(empty.constructions, 0);
  assert.deepEqual(empty.value.timeline, []);
  assert.equal(invalid.constructions, 0);
  assert.deepEqual(invalid.value.timeline, []);
});

test("empty fixed timecharts retain canonical and aliased metric schema names", () => {
  for (const metricName of ["count(payload.value)", "Occurrences"]) {
    const schema: ResultSchema = {
      schemaId: `empty-fixed-timechart-${metricName}`,
      revision: 1n,
      resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
      columns: [
        column(
          "_time",
          ValueType.VALUE_TYPE_TIMESTAMP,
          ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
        ),
        column(
          metricName,
          ValueType.VALUE_TYPE_UINT64,
          ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
        ),
      ],
    };

    const adapted = adaptSearchResults(schema, []);

    assert.deepEqual(adapted.timeline, []);
    assert.deepEqual(timechartValueFields(adapted.timeline, schema), [metricName]);
  }
});

test("empty dynamic timecharts do not invent a count series", () => {
  const schema: ResultSchema = {
    schemaId: "empty-dynamic-timechart-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
    ],
  };

  const adapted = adaptSearchResults(schema, []);

  assert.deepEqual(adapted.timeline, []);
  assert.deepEqual(timechartValueFields(adapted.timeline, schema), []);
  assert.deepEqual(timechartValueFields(adapted.timeline), ["count"]);
});

test("formatter reuse refreshes the local timezone for each adaptation", (context) => {
  const originalTimezone = process.env.TZ;
  const schema: ResultSchema = {
    schemaId: "event-timezone-refresh-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "_raw",
        ValueType.VALUE_TYPE_STRING,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      ),
    ],
  };
  const timestamp = "2026-07-21T22:00:00.000Z";
  const rows = [row("timezone", 0n, [
    timestampValue(timestamp),
    stringValue("timezone"),
  ])];
  const timeSeriesSchema: ResultSchema = {
    schemaId: "time-series-timezone-refresh-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
    columns: [
      column(
        "_time",
        ValueType.VALUE_TYPE_TIMESTAMP,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      ),
      column(
        "count",
        ValueType.VALUE_TYPE_UINT64,
        ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
      ),
    ],
  };
  const timeSeriesRows = [row("timezone-bucket", 0n, [
    timestampValue(timestamp),
    uint64Value(1n),
  ])];
  const eventOptions: Intl.DateTimeFormatOptions = {
    month: "numeric",
    day: "numeric",
    year: "2-digit",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  };
  const timelineOptions: Intl.DateTimeFormatOptions = {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  };

  try {
    process.env.TZ = "UTC";
    if (new Intl.DateTimeFormat().resolvedOptions().timeZone !== "UTC") {
      context.skip("The runtime does not apply process.env.TZ changes.");
      return;
    }
    const utcExpectedEvent = new Intl.DateTimeFormat("en-US", {
      ...eventOptions,
      timeZone: "UTC",
    }).format(new Date(timestamp));
    const utcExpectedTimeline = new Intl.DateTimeFormat("en-US", {
      ...timelineOptions,
      timeZone: "UTC",
    }).format(new Date(timestamp));
    const utcEvents = trackDateTimeFormatConstructions(
      () => adaptSearchResults(schema, rows),
    );
    const utcTimeSeries = trackDateTimeFormatConstructions(
      () => adaptSearchResults(timeSeriesSchema, timeSeriesRows, 60_000),
    );

    process.env.TZ = "America/Los_Angeles";
    if (
      new Intl.DateTimeFormat().resolvedOptions().timeZone
      !== "America/Los_Angeles"
    ) {
      context.skip("The runtime does not apply process.env.TZ changes.");
      return;
    }
    const pacificExpectedEvent = new Intl.DateTimeFormat("en-US", {
      ...eventOptions,
      timeZone: "America/Los_Angeles",
    }).format(new Date(timestamp));
    const pacificExpectedTimeline = new Intl.DateTimeFormat("en-US", {
      ...timelineOptions,
      timeZone: "America/Los_Angeles",
    }).format(new Date(timestamp));
    const pacificEvents = trackDateTimeFormatConstructions(
      () => adaptSearchResults(schema, rows),
    );
    const pacificTimeSeries = trackDateTimeFormatConstructions(
      () => adaptSearchResults(timeSeriesSchema, timeSeriesRows, 60_000),
    );

    assert.equal(utcEvents.constructions, 1);
    assert.equal(utcTimeSeries.constructions, 1);
    assert.equal(pacificEvents.constructions, 1);
    assert.equal(pacificTimeSeries.constructions, 1);
    assert.equal(utcEvents.value.events[0]?.timeLabel, utcExpectedEvent);
    assert.equal(utcTimeSeries.value.timeline[0]?.label, utcExpectedTimeline);
    assert.equal(pacificEvents.value.events[0]?.timeLabel, pacificExpectedEvent);
    assert.equal(
      pacificTimeSeries.value.timeline[0]?.label,
      pacificExpectedTimeline,
    );
    assert.notEqual(utcExpectedEvent, pacificExpectedEvent);
    assert.notEqual(utcExpectedTimeline, pacificExpectedTimeline);
  } finally {
    if (originalTimezone === undefined) delete process.env.TZ;
    else process.env.TZ = originalTimezone;
  }
});

test("timechart keeps siblings when a runtime series is named count", () => {
  const schema: ResultSchema = {
    schemaId: "timechart-count-sibling-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
    columns: [
      column("_time", ValueType.VALUE_TYPE_TIMESTAMP, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME),
      column("count", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("ERROR", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
      column("WARN", ValueType.VALUE_TYPE_UINT64, ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC),
    ],
  };
  const rows: ResultRow[] = [
    row("bucket-1", 0n, [
      timestampValue("2026-07-21T22:00:00.000Z"),
      uint64Value(2n),
      uint64Value(9_007_199_254_740_993n),
      uint64Value(3n),
    ]),
    row("bucket-2", 1n, [
      timestampValue("2026-07-21T22:05:00.000Z"),
      uint64Value(1n),
      uint64Value(2n),
      uint64Value(0n),
    ]),
  ];
  const expectedFirstTimelineLabel = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date("2026-07-21T22:00:00.000Z"));

  const measured = trackDateTimeFormatConstructions(() => [
    adaptSearchResults(schema, rows, 300_000),
    adaptSearchResults(schema, rows.slice(0, 1), 300_000),
  ]);
  const adapted = measured.value[0];

  assert.equal(measured.constructions, 2);
  assert.deepEqual(adapted.events, []);
  assert.deepEqual(adapted.fields, []);
  assert.deepEqual(adapted.statistics, []);
  assert.equal(adapted.timeline[0]?.label, expectedFirstTimelineLabel);
  assert.deepEqual(Object.keys(adapted.timeline[0]?.series ?? {}), ["count", "ERROR", "WARN"]);
  assert.deepEqual(timechartValueFields(adapted.timeline, schema), ["count", "ERROR", "WARN"]);
  assert.deepEqual(timechartValueFields(adapted.timeline), ["count", "ERROR", "WARN"]);
  assert.equal(adapted.timeline[0]?.exactSeries?.ERROR, "9007199254740993");
  assert.deepEqual(timechartRowsForExport(adapted.timeline), [{
    _time: "2026-07-21T22:00:00.000Z",
    count: 2,
    ERROR: "9007199254740993",
    WARN: 3,
  }, {
    _time: "2026-07-21T22:05:00.000Z",
    count: 1,
    ERROR: 2,
    WARN: 0,
  }]);
});
