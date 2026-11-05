import assert from "node:assert/strict";
import test from "node:test";

import type { ComponentProps } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { ColumnSemanticType, ResultRow, ResultSchema, ResultSetKind } from "@/gen/ts/open_splunk/result";
import { ValueType } from "@/gen/ts/open_splunk/value";
import type { TimelinePoint } from "@/lib/demo/search-data";
import { adaptSearchResults, type WorkspaceStatistic, type WorkspaceStatisticsTable } from "@/lib/search/backend-data";

import { StatisticsColumnLayoutStore } from "./statistics-column-layout";
import { StatisticsPanel } from "./statistics-panel";

const statistics: WorkspaceStatistic[] = [{
  level: "INFO",
  count: 12,
  percent: "100%",
  avgDuration: 4.5,
}];

function renderStatisticsPanel(
  menu: "statistics-columns" | null,
  overrides: Partial<ComponentProps<typeof StatisticsPanel>> = {},
): string {
  return renderToStaticMarkup(
    <StatisticsPanel
      columnLayoutStore={new StatisticsColumnLayoutStore()}
      elapsed="0.2 seconds"
      genericStatisticsTable={null}
      genericStatsSort={null}
      isPreview={false}
      isTimechartResult={false}
      menu={menu}
      pageNumber={1}
      pageStart={1}
      previewTruncated={false}
      resultIdentity={1}
      resultTotalExact
      resultTotalRows={1}
      sortedGenericStatisticsRows={[]}
      sortedStatistics={statistics}
      sortedTimechartRows={[]}
      statisticsDimension="level"
      statisticsRows={statistics}
      statsDensity="compact"
      statsSort={{ key: "count", direction: "desc" }}
      submittedQuery="index=main | stats count by level"
      timechartSort={{ key: "time", direction: "asc" }}
      timechartValueColumns={[]}
      timelinePoints={[]}
      onApplyPivot={() => undefined}
      onExport={() => undefined}
      onGenericStatsSortChange={() => undefined}
      onMenuChange={() => undefined}
      onStatsDensityChange={() => undefined}
      onStatsSortChange={() => undefined}
      onTimechartSortChange={() => undefined}
      {...overrides}
    />,
  );
}

test("statistics table renders visible columns with accessible resize separators", () => {
  const markup = renderStatisticsPanel(null);

  assert.equal((markup.match(/<col\/>/gu) ?? []).length, 4);
  assert.equal((markup.match(/role="separator"/gu) ?? []).length, 4);
  assert.match(markup, /aria-label="Resize level column"/u);
  assert.match(markup, /aria-orientation="vertical"/u);
  assert.match(markup, /tabindex="0"/u);
});

test("statistics columns menu exposes every column as a checkbox item", () => {
  const markup = renderStatisticsPanel("statistics-columns");

  assert.equal((markup.match(/role="menuitemcheckbox"/gu) ?? []).length, 4);
  assert.equal((markup.match(/aria-checked="true"/gu) ?? []).length, 4);
  assert.match(markup, /aria-label="Statistics table columns"/u);
  assert.match(markup, /<strong>avg\(duration_ms\)<\/strong>/u);
});

test("wide statistics tables page columns and bound rendered cells", () => {
  const columns = Array.from({ length: 70 }, (_, index) => ({
    key: `field-${index + 1}`,
    fieldName: `field-${index + 1}`,
    label: `Field ${index + 1}`,
    valueType: ValueType.VALUE_TYPE_UINT64,
    semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
    numeric: true,
    pivotable: false,
    statsSparkline: false,
  }));
  const genericStatisticsTable: WorkspaceStatisticsTable = {
    columns,
    rows: [{
      id: "wide-row",
      pivotValues: {},
      values: Object.fromEntries(columns.map((column, index) => [column.key, index + 1])),
    }],
  };
  const markup = renderStatisticsPanel("statistics-columns", {
    genericStatisticsTable,
    sortedGenericStatisticsRows: genericStatisticsTable.rows,
    submittedQuery: "index=main | stats count by wide_field",
  });

  assert.match(markup, /Showing columns 1–24 of 70/u);
  assert.match(markup, />Previous columns<\/button>/u);
  assert.match(markup, />Next columns<\/button>/u);
  assert.equal((markup.match(/role="menuitemcheckbox"/gu) ?? []).length, 24);
  assert.equal((markup.match(/<th(?:\s|>)/gu) ?? []).length, 24);
  assert.equal((markup.match(/<td(?:\s|>)/gu) ?? []).length, 24);
  assert.doesNotMatch(markup, /Field 25/u);
});

test("timechart statistics preserve all-null rows and only legacy rows fall back to count", () => {
  const points: TimelinePoint[] = [
    {
      id: "owned-null",
      label: "12:00 AM",
      count: 0,
      series: { average: null } as unknown as Record<string, number>,
      earliest: "2026-01-01T00:00:00Z",
      latest: "2026-01-01T01:00:00Z",
    },
    {
      id: "owned-missing",
      label: "1:00 AM",
      count: 11,
      series: {},
      earliest: "2026-01-01T01:00:00Z",
      latest: "2026-01-01T02:00:00Z",
    },
    {
      id: "legacy",
      label: "2:00 AM",
      count: 7,
      earliest: "2026-01-01T02:00:00Z",
      latest: "2026-01-01T03:00:00Z",
    },
  ];
  const markup = renderStatisticsPanel(null, {
    isTimechartResult: true,
    pageStart: 1,
    resultTotalRows: 3,
    sortedTimechartRows: points,
    timechartValueColumns: ["average"],
    timelinePoints: points,
  });

  assert.match(markup, /<time dateTime="2026-01-01T00:00:00Z">12:00 AM<\/time><\/td><td class="numeric-cell">—<\/td>/u);
  assert.match(markup, /<time dateTime="2026-01-01T01:00:00Z">1:00 AM<\/time><\/td><td class="numeric-cell">—<\/td>/u);
  assert.match(markup, /<time dateTime="2026-01-01T02:00:00Z">2:00 AM<\/time><\/td><td class="numeric-cell">7<\/td>/u);
  assert.equal((markup.match(/<tr/gu) ?? []).length, 4);
});

test("backend table projections render raw payloads and authoritative row counts", () => {
  const schema = ResultSchema.fromPartial({
    schemaId: "slow-query-table",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      { fieldName: "elapsed_ms", valueType: ValueType.VALUE_TYPE_UINT64 },
      { fieldName: "message", valueType: ValueType.VALUE_TYPE_STRING },
      { fieldName: "_raw", valueType: ValueType.VALUE_TYPE_STRING, semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW },
    ],
  });
  const adapted = adaptSearchResults(schema, [ResultRow.fromPartial({
    rowId: "slow-query",
    ordinal: 0n,
    cells: [
      { kind: { $case: "uint64Value", value: 2500n } },
      { kind: { $case: "stringValue", value: "slow query" } },
      { kind: { $case: "stringValue", value: '<script>alert("raw")</script>' } },
    ],
  })]);
  const markup = renderStatisticsPanel(null, {
    genericStatisticsTable: adapted.statisticsTable,
    sortedGenericStatisticsRows: adapted.statisticsTable?.rows ?? [],
    sortedStatistics: adapted.statistics,
    statisticsRows: adapted.statistics,
    submittedQuery: "index=main | sort 50 -num(elapsed_ms) | table elapsed_ms message _raw",
  });

  assert.match(markup, /aria-label="Resize elapsed_ms column"/u);
  assert.match(markup, /aria-label="Resize _raw column"/u);
  assert.match(markup, /slow query/u);
  assert.match(markup, /&lt;script&gt;alert\(&quot;raw&quot;\)&lt;\/script&gt;/u);
  assert.match(markup, /Showing 1–1/u);
  assert.doesNotMatch(markup, /Showing 0 rows|avg\(duration_ms\)|<script>/u);
});
