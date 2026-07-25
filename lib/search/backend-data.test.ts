import assert from "node:assert/strict";
import test from "node:test";

import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultColumn,
  type ResultRow,
  type ResultSchema,
} from "../../gen/ts/open_splunk/v1/result";
import {
  ValueType,
  type TypedValue,
} from "../../gen/ts/open_splunk/v1/value";
import { adaptSearchResults } from "./backend-data";

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

function row(rowId: string, ordinal: bigint, cells: TypedValue[]): ResultRow {
  return { rowId, ordinal, cells };
}

test("top message results adapt to one categorical message series", () => {
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

  const adapted = adaptSearchResults(schema, rows, false);

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
});
