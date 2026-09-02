import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import type { WorkspaceStatistic } from "@/lib/search/backend-data";

import { StatisticsPanel } from "./statistics-panel";

const statistics: WorkspaceStatistic[] = [{
  level: "INFO",
  count: 12,
  percent: "100%",
  avgDuration: 4.5,
}];

function renderStatisticsPanel(menu: "statistics-columns" | null): string {
  return renderToStaticMarkup(
    <StatisticsPanel
      columnLayoutStore={new Map()}
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
