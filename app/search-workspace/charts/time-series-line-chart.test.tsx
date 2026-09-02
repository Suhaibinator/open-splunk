import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import type { TimelinePoint } from "@/lib/demo/search-data";

import { TimeSeriesLineChart, formatTimelineSeriesValue } from "./time-series-line-chart";

const splitPoints: TimelinePoint[] = [
  {
    id: "first",
    label: "00:00",
    count: 5,
    series: { east: 2, west: -3 },
    exactSeries: { east: "2", west: "-3" },
  },
  {
    id: "second",
    label: "01:00",
    count: 5,
    series: { east: 4, west: -1 },
    exactSeries: { east: "4", west: "-1" },
  },
];

test("area time series render fills before cumulative line strokes", () => {
  const markup = renderToStaticMarkup(
    <TimeSeriesLineChart chartStyle="area" points={splitPoints} stackMode="stacked100" />,
  );

  assert.match(markup, /data-chart-style="area"/u);
  assert.match(markup, /data-stack-mode="stacked100"/u);
  assert.equal((markup.match(/class="time-series-chart__area time-series-chart__series"/gu) ?? []).length, 2);
  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 2);
  assert.ok(
    markup.indexOf("time-series-chart__area") < markup.indexOf("time-series-chart__line"),
    "area polygons must paint before their line strokes",
  );
  assert.match(markup, /<polygon[^>]+data-series-color="1"/u);
  assert.match(markup, /<polygon[^>]+data-series-color="2"/u);
  assert.match(markup, />100%<\/span>/u);
  assert.match(markup, />-100%<\/span>/u);
});

test("missing series values split both area fills and line strokes", () => {
  const points: TimelinePoint[] = [
    { id: "first", label: "00:00", count: 2, series: { east: 2 } },
    { id: "gap", label: "01:00", count: 0, series: {} },
    { id: "last", label: "02:00", count: 4, series: { east: 4 } },
  ];
  const markup = renderToStaticMarkup(
    <TimeSeriesLineChart chartStyle="area" points={points} />,
  );

  assert.equal((markup.match(/class="time-series-chart__area time-series-chart__series"/gu) ?? []).length, 2);
  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 2);
});

test("time-series value formatting keeps exact raw server values", () => {
  const point: TimelinePoint = {
    id: "exact",
    label: "00:00",
    count: Number.MAX_SAFE_INTEGER,
    series: { east: Number.MAX_SAFE_INTEGER },
    exactSeries: { east: "900719925474099312345" },
    coordinateApproximate: true,
  };

  assert.equal(formatTimelineSeriesValue(point, "east"), "900,719,925,474,099,312,345");
});
