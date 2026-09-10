import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import type { TimelinePoint } from "@/lib/demo/search-data";

import {
  TimeSeriesLineChart,
  formatTimelineSeriesValue,
  timelinePointInspectionLabel,
  timelineXCoordinates,
} from "./time-series-line-chart";

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

test("time-series x coordinates preserve sub-millisecond spacing from a nearby BigInt origin", () => {
  const origin = 1_789_027_750_123_456_789n;
  const points: TimelinePoint[] = [0n, 100n, 1_000n].map((offset, index) => ({
    id: `point-${index}`,
    label: `${index}`,
    count: index,
    timeCoordinateNanoseconds: origin + offset,
  }));
  assert.deepEqual(timelineXCoordinates(points), [0, 100, 1_000]);
  assert.deepEqual(timelineXCoordinates(points.map(({ timeCoordinateNanoseconds: _time, ...point }) => point)), [0, 500, 1_000]);

  const markup = renderToStaticMarkup(<TimeSeriesLineChart points={points} />);
  assert.match(markup, /class="time-series-chart__x-axis"[^>]*>.*left:0%/u);
  assert.match(markup, /class="time-series-chart__x-axis"[^>]*>.*left:10%/u);
  assert.match(markup, /class="time-series-chart__x-axis"[^>]*>.*left:100%/u);
});

test("time-series inspection exposes exact sub-millisecond bucket bounds", () => {
  const first: TimelinePoint = {
    id: "first",
    label: "12:00:00 AM",
    count: 1,
    earliest: "2026-09-10T00:00:00.000000001Z",
    latest: "2026-09-10T00:00:00.000000002Z",
  };
  const second: TimelinePoint = {
    ...first,
    id: "second",
    earliest: "2026-09-10T00:00:00.000000002Z",
    latest: "2026-09-10T00:00:00.000000003Z",
  };

  assert.equal(
    timelinePointInspectionLabel(first),
    "12:00:00 AM, exact bucket 2026-09-10T00:00:00.000000001Z to 2026-09-10T00:00:00.000000002Z",
  );
  assert.notEqual(timelinePointInspectionLabel(first), timelinePointInspectionLabel(second));
});

test("reordered exact points anchor axis labels by coordinate edge", () => {
  const origin = 1_789_027_750_123_456_789n;
  const points: TimelinePoint[] = [500n, 1_000n, 0n].map((offset, index) => ({
    id: `point-${index}`,
    label: `${index}`,
    count: index,
    timeCoordinateNanoseconds: origin + offset,
  }));
  const markup = renderToStaticMarkup(<TimeSeriesLineChart points={points} />);

  assert.match(markup, /data-edge="end" style="left:100%">1</u);
  assert.match(markup, /data-edge="start" style="left:0%">2</u);
  assert.doesNotMatch(markup, /data-edge="start" style="left:50%">0</u);
});

test("wide stacked windows retain global baselines while bounding rendered series", () => {
  const series = Object.fromEntries(Array.from({ length: 30 }, (_, index) => [`series-${index + 1}`, 1]));
  const markup = renderToStaticMarkup(
    <TimeSeriesLineChart
      points={[{ id: "wide", label: "00:00", count: 30, series }]}
      seriesStart={24}
      seriesEnd={30}
      stackMode="stacked100"
    />,
  );

  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 6);
  assert.doesNotMatch(markup, /data-series-name="series-1"/u);
  assert.match(markup, /data-series-name="series-25"/u);
  assert.match(markup, /data-series-name="series-30"/u);
  // The first visible series starts after the 24 hidden series, at 80% of the stack.
  assert.match(markup, /data-series-name="series-25"[^>]*points="500\.00,50\.00"/u);
});
