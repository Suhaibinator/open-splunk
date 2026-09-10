import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import type { TimelinePoint } from "@/lib/demo/search-data";

import {
  TimeSeriesLineChart,
  formatTimelineSeriesValue,
  nearestTimelineCoordinateIndex,
  timelineColumnCoordinates,
  timelineCoordinateIndex,
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

test("column coordinates use exact sparse bucket extents including a single final bucket", () => {
  const origin = 1_789_027_750_123_456_789n;
  const points: TimelinePoint[] = [
    {
      id: "first",
      label: "first",
      count: 1,
      timeCoordinateNanoseconds: origin,
      timeLatestCoordinateNanoseconds: origin + 250_000_000n,
    },
    {
      id: "sparse",
      label: "sparse",
      count: 1,
      timeCoordinateNanoseconds: origin + 750_000_000n,
      timeLatestCoordinateNanoseconds: origin + 1_000_000_000n,
    },
  ];

  assert.deepEqual(timelineColumnCoordinates(points), [
    { centerPercent: 12.5, leftPercent: 0, widthPercent: 25 },
    { centerPercent: 87.5, leftPercent: 75, widthPercent: 25 },
  ]);
  assert.deepEqual(timelineColumnCoordinates([points[1]]), [
    { centerPercent: 50, leftPercent: 0, widthPercent: 100 },
  ]);
});

test("pointer coordinate lookup is logarithmic-ready and preserves ordinal tie behavior", () => {
  const coordinates = [1_000, 0, 500, 500];
  const index = timelineCoordinateIndex(coordinates);

  assert.deepEqual(index, [
    { index: 1, x: 0 },
    { index: 2, x: 500 },
    { index: 0, x: 1_000 },
  ]);
  assert.equal(nearestTimelineCoordinateIndex(index, 750), 0);
  assert.equal(nearestTimelineCoordinateIndex(index, 501), 2);
  assert.equal(nearestTimelineCoordinateIndex(index, -1), 1);
});

test("pointer coordinate lookup reads logarithmically from a maximum-size grid", () => {
  const coordinateIndex = timelineCoordinateIndex(Array.from(
    { length: 10_000 },
    (_, index) => index / 10,
  ));
  let entryReads = 0;
  const observed = new Proxy(coordinateIndex, {
    get(target, property, receiver) {
      if (/^\d+$/u.test(String(property))) entryReads += 1;
      return Reflect.get(target, property, receiver);
    },
  });

  assert.equal(nearestTimelineCoordinateIndex(observed, 678.94), 6_789);
  assert.ok(entryReads < 32, `nearest lookup read ${entryReads} coordinate entries`);
});

test("maximum-size column grids render one memoizable path and bounded labels", () => {
  const origin = 1_789_027_750_123_456_789n;
  const points: TimelinePoint[] = Array.from({ length: 10_000 }, (_, index) => ({
    id: `point-${index}`,
    label: `${index}`,
    count: index % 7,
    timeCoordinateNanoseconds: origin + BigInt(index),
    timeLatestCoordinateNanoseconds: origin + BigInt(index + 1),
  }));
  const markup = renderToStaticMarkup(
    <TimeSeriesLineChart chartStyle="column" points={points} showDataLabels />,
  );

  assert.equal((markup.match(/time-series-chart__columns/gu) ?? []).length, 1);
  assert.equal((markup.match(/time-series-chart__data-label/gu) ?? []).length, 5);
  assert.equal((markup.match(/time-series-chart__inspect/gu) ?? []).length, 1);
});

test("a single final column bucket spans its authoritative interval", () => {
  const point: TimelinePoint = {
    id: "final",
    label: "final",
    count: 4,
    earliest: "2026-09-10T00:00:00.000000001Z",
    latest: "2026-09-10T00:00:00.000000002Z",
    timeCoordinateNanoseconds: 1_789_027_750_000_000_001n,
    timeLatestCoordinateNanoseconds: 1_789_027_750_000_000_002n,
  };
  const markup = renderToStaticMarkup(<TimeSeriesLineChart chartStyle="column" points={[point]} />);

  assert.match(markup, /d="M0\.00,[\d.]+V[\d.]+H1000\.00V[\d.]+Z"/u);
  assert.equal(
    timelinePointInspectionLabel(point),
    "final, exact bucket 2026-09-10T00:00:00.000000001Z to 2026-09-10T00:00:00.000000002Z",
  );
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
