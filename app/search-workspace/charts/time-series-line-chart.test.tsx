import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: react-dom/client detects the fake DOM at module load.
import { browser } from "@/lib/testing/fake-browser";
import { FakeElement, fakeEvent } from "@/lib/testing/fake-dom";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";

import type { TimelinePoint } from "@/lib/demo/search-data";

import {
  TimeSeriesLineChart,
  formatTimelineSeriesValue,
  nearestTimelineCoordinateIndex,
  timelineChartModel,
  timelineColumnCoordinates,
  timelineCoordinateIndex,
  timelinePointInspectionLabel,
  timelineVisibleStackWindow,
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

Object.assign(globalThis, {
  ResizeObserver: class {
    disconnect() {}
    observe() {}
  },
});
Object.assign(window, {
  cancelAnimationFrame() {},
  requestAnimationFrame() { return 0; },
});
(FakeElement.prototype as unknown as {
  getBoundingClientRect: () => { left: number; top: number; width: number; height: number };
}).getBoundingClientRect = () => ({ left: 0, top: 0, width: 1_000, height: 300 });

function renderedPathData(markup: string, className: string): string[] {
  const elements = markup.match(new RegExp(`<path[^>]+class="${className}"[^>]*>`, "gu")) ?? [];
  return elements.map((element) => {
    const path = /\sd="([^"]*)"/u.exec(element)?.[1];
    assert.ok(path !== undefined, `missing path data for ${className}`);
    return path;
  });
}

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
    "area paths must paint before their line strokes",
  );
  assert.match(markup, /<path[^>]+time-series-chart__area[^>]+data-series-color="1"/u);
  assert.match(markup, /<path[^>]+time-series-chart__area[^>]+data-series-color="2"/u);
  assert.match(markup, />100%<\/span>/u);
  assert.match(markup, />-100%<\/span>/u);
});

test("missing series values start new strokes and close each area against its reversed baseline", () => {
  const points: TimelinePoint[] = [
    { id: "first", label: "00:00", count: 2, series: { east: 2 } },
    { id: "second", label: "01:00", count: 3, series: { east: 3 } },
    { id: "gap", label: "02:00", count: 0, series: {} },
    { id: "fourth", label: "03:00", count: 4, series: { east: 4 } },
    { id: "last", label: "04:00", count: 1, series: { east: 1 } },
  ];
  const markup = renderToStaticMarkup(
    <TimeSeriesLineChart chartStyle="area" points={points} />,
  );

  const [areaPath] = renderedPathData(markup, "time-series-chart__area time-series-chart__series");
  const [linePath] = renderedPathData(markup, "time-series-chart__line time-series-chart__series");
  assert.equal((areaPath.match(/Z/gu) ?? []).length, 2);
  assert.equal(
    areaPath,
    "M0.00,150.00L250.00,75.00L250.00,300.00L0.00,300.00ZM750.00,0.00L1000.00,225.00L1000.00,300.00L750.00,300.00Z",
  );
  assert.equal(linePath, "M0.00,150.00L250.00,75.00M750.00,0.00L1000.00,225.00");
});

test("pointer and keyboard inspection expose exact all-null buckets without markers", async () => {
  const points: TimelinePoint[] = Array.from({ length: 3 }, (_value, index) => ({
    id: `bucket-${index}`,
    label: `hour ${index}`,
    count: 0,
    series: { "avg(latency)": null },
    earliest: `2026-09-10T0${index}:00:00Z`,
    latest: `2026-09-10T0${index + 1}:00:00Z`,
    timeCoordinateNanoseconds: BigInt(index) * 3_600_000_000_000n,
    timeLatestCoordinateNanoseconds: BigInt(index + 1) * 3_600_000_000_000n,
  }));
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => root.render(<TimeSeriesLineChart points={points} />));
    const elementsWithClass = () => container.querySelectorAll("[class]");
    const elementsByClass = (className: string) => elementsWithClass().filter((element) =>
      (element.getAttribute("class") ?? "").split(" ").includes(className)
    );
    const inspect = container.querySelector("button");
    assert.ok(inspect);

    await act(async () => {
      inspect.dispatchEvent(Object.assign(fakeEvent("pointermove"), {
        clientX: 500,
        pointerType: "mouse",
      }));
    });
    let tooltip = container.querySelector('[role="tooltip"]');
    assert.ok(tooltip);
    assert.match(tooltip.textContent, /hour 1, exact bucket 2026-09-10T01:00:00Z to 2026-09-10T02:00:00Z/u);
    assert.match(tooltip.textContent, /avg\(latency\)No value/u);
    assert.equal(elementsByClass("time-series-chart__marker").length, 0);
    const [crosshair] = elementsByClass("time-series-chart__crosshair");
    assert.ok(crosshair);
    assert.equal((crosshair.style as unknown as { left: string }).left, "50%");
    assert.match(inspect.getAttribute("aria-label") ?? "", /hour 1.*No value/u);

    await act(async () => {
      inspect.dispatchEvent(Object.assign(fakeEvent("keydown"), { key: "ArrowRight" }));
    });
    tooltip = container.querySelector('[role="tooltip"]');
    assert.ok(tooltip);
    assert.match(tooltip.textContent, /hour 2, exact bucket 2026-09-10T02:00:00Z to 2026-09-10T03:00:00Z/u);
    assert.match(inspect.getAttribute("aria-label") ?? "", /hour 2.*No value/u);
    assert.equal(elementsByClass("time-series-chart__marker").length, 0);
  } finally {
    await act(async () => root.unmount());
    browser.document.body.removeChild(container);
  }
});

test("pinned chart values stay stable, copy visible labels, and restore inspector focus", async () => {
  const copiedLabels: string[] = [];
  const visibleLabel = 'Request summary.statistics "cached"';
  const points: TimelinePoint[] = [
    { id: "first", label: "hour 0", count: 3, series: { [visibleLabel]: 3 } },
    { id: "second", label: "hour 1", count: 5, series: { [visibleLabel]: 5 } },
  ];
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => root.render(
      <TimeSeriesLineChart onCopySeriesLabel={(label) => copiedLabels.push(label)} points={points} />,
    ));
    const inspect = container.querySelector("button");
    assert.ok(inspect);

    await act(async () => {
      inspect.dispatchEvent(Object.assign(fakeEvent("pointerdown"), {
        clientX: 1_000,
        pointerType: "mouse",
      }));
    });
    let pinned = container.querySelector('[role="group"]');
    assert.ok(pinned);
    assert.equal(pinned.getAttribute("aria-label"), "Pinned chart values for hour 1");

    await act(async () => {
      inspect.dispatchEvent(Object.assign(fakeEvent("pointermove"), {
        clientX: 0,
        pointerType: "mouse",
      }));
    });
    pinned = container.querySelector('[role="group"]');
    assert.equal(pinned?.getAttribute("aria-label"), "Pinned chart values for hour 1");
    assert.equal(
      container.querySelector('[class="time-series-chart__pinned-label"]')?.textContent,
      visibleLabel,
    );

    const copy = container.querySelectorAll("[aria-label]").find((element) =>
      element.getAttribute("aria-label") === `Copy series label ${visibleLabel}`
    );
    assert.ok(copy);
    await act(async () => copy.dispatchEvent(fakeEvent("click")));
    assert.deepEqual(copiedLabels, [visibleLabel]);

    async function repinAfterFocusRoundTrip(
      key: " " | "Enter",
      inspectButton: FakeElement,
      copyButton: FakeElement,
    ) {
      copyButton.focus();
      await act(async () => inspectButton.dispatchEvent(fakeEvent("focusout")));
      inspectButton.focus();
      await act(async () => inspectButton.dispatchEvent(fakeEvent("focusin")));
      await act(async () => inspectButton.dispatchEvent(Object.assign(fakeEvent("keydown"), { key })));
      assert.equal(
        container.querySelector('[role="group"]')?.getAttribute("aria-label"),
        "Pinned chart values for hour 1",
      );
    }
    await repinAfterFocusRoundTrip("Enter", inspect, copy);
    await repinAfterFocusRoundTrip(" ", inspect, copy);

    await act(async () => copy.dispatchEvent(Object.assign(fakeEvent("keydown"), { key: "Escape" })));
    assert.equal(container.querySelector('[role="group"]'), null);
    assert.equal(browser.document.activeElement, inspect);

    await act(async () => inspect.dispatchEvent(Object.assign(fakeEvent("keydown"), { key: "Enter" })));
    const close = container.querySelector('[aria-label="Close pinned chart values"]');
    assert.ok(close);
    close.focus();
    await act(async () => close.dispatchEvent(fakeEvent("click")));
    assert.equal(container.querySelector('[role="group"]'), null);
    assert.equal(browser.document.activeElement, inspect);

    await act(async () => inspect.dispatchEvent(Object.assign(fakeEvent("keydown"), { key: "Enter" })));
    assert.ok(container.querySelector('[role="group"]'));
    await act(async () => browser.document.body.dispatchEvent(fakeEvent("pointerdown")));
    assert.equal(container.querySelector('[role="group"]'), null);
    assert.equal(container.querySelector('[role="tooltip"]'), null);
  } finally {
    await act(async () => root.unmount());
    browser.document.body.removeChild(container);
  }
});

test("explicit null Events values stay gaps instead of using the legacy count fallback", () => {
  const points: TimelinePoint[] = [
    { id: "first", label: "00:00", count: 1, series: { Events: 1 } },
    { id: "gap", label: "01:00", count: 99, series: { Events: null } },
    { id: "last", label: "02:00", count: 2, series: { Events: 2 } },
  ];
  const model = timelineChartModel(points);
  const window = timelineVisibleStackWindow(model.points, model.series, "Events", 0, 1, "none");
  const markup = renderToStaticMarkup(<TimeSeriesLineChart model={model} points={points} />);

  assert.deepEqual(model.series.domains.none, [0, 2]);
  assert.deepEqual(window.rows[1], [{ end: 0, raw: null, start: 0 }]);
  assert.equal(formatTimelineSeriesValue(points[1], "Events"), "No value");
  assert.match(
    renderedPathData(markup, "time-series-chart__line time-series-chart__series")[0],
    /^M0\.00,[\d.]+M1000\.00,[\d.]+$/u,
  );
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

test("maximum-size gapped timecharts bound SVG paths to the visible series", () => {
  const seriesNames = Array.from({ length: 24 }, (_value, index) => `series-${index + 1}`);
  const points: TimelinePoint[] = Array.from({ length: 10_000 }, (_value, pointIndex) => ({
    id: `point-${pointIndex}`,
    label: `${pointIndex}`,
    count: pointIndex % 2 === 0 ? 24 : 0,
    series: Object.fromEntries(seriesNames.map((name, seriesIndex) => [
      name,
      pointIndex % 2 === 0 ? pointIndex + seriesIndex : null,
    ])),
    timeCoordinateNanoseconds: BigInt(pointIndex),
  }));

  for (const chartStyle of ["line", "area"] as const) {
    const markup = renderToStaticMarkup(<TimeSeriesLineChart chartStyle={chartStyle} points={points} />);
    const linePaths = renderedPathData(markup, "time-series-chart__line time-series-chart__series");
    const areaPaths = renderedPathData(markup, "time-series-chart__area time-series-chart__series");
    assert.equal(linePaths.length, 24);
    assert.equal(areaPaths.length, chartStyle === "area" ? 24 : 0);
    assert.equal((linePaths[0].match(/M/gu) ?? []).length, 5_000);
    assert.equal((linePaths[0].match(/L/gu) ?? []).length, 0);
    if (chartStyle === "area") {
      assert.equal((areaPaths[0].match(/M/gu) ?? []).length, 5_000);
      assert.equal((areaPaths[0].match(/Z/gu) ?? []).length, 5_000);
    }
  }
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

test("line and area geometry follow exact chronology without mutating server order", () => {
  const origin = 1_789_027_750_123_456_789n;
  const points: TimelinePoint[] = [2n, 0n, 1n].map((offset) => ({
    id: `point-${offset}`,
    label: `${offset}`,
    count: Number(offset) + 1,
    timeCoordinateNanoseconds: origin + offset,
  }));
  const model = timelineChartModel(points);

  assert.deepEqual(points.map((point) => point.id), ["point-2", "point-0", "point-1"]);
  assert.deepEqual(model.points.map((point) => point.id), ["point-0", "point-1", "point-2"]);
  assert.equal(model.points[0], points[1], "chart sorting must preserve source point identity");
  for (const chartStyle of ["line", "area"] as const) {
    const markup = renderToStaticMarkup(<TimeSeriesLineChart chartStyle={chartStyle} points={points} />);
    assert.match(markup, /<path[^>]+d="M0\.00,[\d.]+L500\.00,[\d.]+L1000\.00,[\d.]+"/u);
    assert.ok(
      markup.indexOf("left:0%\">0") < markup.indexOf("left:50%\">1")
      && markup.indexOf("left:50%\">1") < markup.indexOf("left:100%\">2"),
      `${chartStyle} inspection labels must follow chronological point indices`,
    );
  }
});

test("chronological plotting keeps a reordered null bucket as a path gap", () => {
  const points: TimelinePoint[] = [
    { id: "late", label: "late", count: 3, series: { east: 3 }, timeCoordinateNanoseconds: 2n },
    { id: "early", label: "early", count: 1, series: { east: 1 }, timeCoordinateNanoseconds: 0n },
    { id: "gap", label: "gap", count: 0, series: {}, timeCoordinateNanoseconds: 1n },
  ];
  const model = timelineChartModel(points);
  const markup = renderToStaticMarkup(<TimeSeriesLineChart chartStyle="area" model={model} points={points} />);

  assert.deepEqual(model.points.map((point) => point.id), ["early", "gap", "late"]);
  const [areaPath] = renderedPathData(markup, "time-series-chart__area time-series-chart__series");
  const [linePath] = renderedPathData(markup, "time-series-chart__line time-series-chart__series");
  assert.equal((areaPath.match(/Z/gu) ?? []).length, 2);
  assert.match(linePath, /^M0\.00,[\d.]+M1000\.00,[\d.]+$/u);
});

test("chart model scans the full domain once and unstacked windows read only visible series", () => {
  const seriesCount = 80;
  const pointCount = 5;
  let seriesReads = 0;
  let seriesKeyScans = 0;
  const points: TimelinePoint[] = Array.from({ length: pointCount }, (_, pointIndex) => {
    const values = Object.fromEntries(Array.from(
      { length: seriesCount },
      (_value, seriesIndex) => [`series-${seriesIndex}`, pointIndex + seriesIndex],
    ));
    return {
      id: `point-${pointIndex}`,
      label: `${pointIndex}`,
      count: 0,
      series: new Proxy(values, {
        get(target, property, receiver) {
          if (String(property).startsWith("series-")) seriesReads += 1;
          return Reflect.get(target, property, receiver);
        },
        ownKeys(target) {
          seriesKeyScans += 1;
          return Reflect.ownKeys(target);
        },
      }),
      timeCoordinateNanoseconds: BigInt(pointIndex),
    };
  });
  const model = timelineChartModel(points);
  assert.equal(seriesKeyScans, pointCount);
  assert.equal(seriesReads, pointCount * seriesCount);
  assert.equal(model.series.names.length, seriesCount);

  seriesReads = 0;
  const window = timelineVisibleStackWindow(model.points, model.series, "Events", 24, 36, "none");
  assert.equal(seriesReads, pointCount * 12);
  assert.equal(window.rows.length, pointCount);
  assert.ok(window.rows.every((row) => row.length === 12));
  assert.deepEqual(window.domain, model.series.domains.none);

  seriesKeyScans = 0;
  seriesReads = 0;
  const markup = renderToStaticMarkup(
    <TimeSeriesLineChart model={model} points={points} seriesStart={24} seriesEnd={36} />,
  );
  assert.equal(seriesKeyScans, 0, "the chart must reuse the supplied series domain");
  assert.equal(seriesReads, pointCount * 12);
  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 12);
});

test("mixed Events series include missing-entry count fallbacks in cached domains", () => {
  const points: TimelinePoint[] = [
    { id: "explicit", label: "explicit", count: 2, series: { Events: 2, other: 5 } },
    { id: "fallback", label: "fallback", count: 100, series: { other: 1 } },
  ];
  const model = timelineChartModel(points);
  const window = timelineVisibleStackWindow(model.points, model.series, "Events", 0, 2, "none");

  assert.deepEqual(model.series.domains.none, [0, 100]);
  assert.deepEqual(window.rows[1], [
    { end: 100, raw: 100, start: 0 },
    { end: 1, raw: 1, start: 0 },
  ]);
});

test("bounds-only chart points retain exact nanosecond chronology", () => {
  const points: TimelinePoint[] = [
    { id: "later", label: "later", count: 1, earliest: "2026-09-01T12:00:00.000000002Z" },
    { id: "earlier", label: "earlier", count: 1, earliest: "2026-09-01T12:00:00.000000001Z" },
  ];

  assert.deepEqual(timelineChartModel(points).points.map((point) => point.id), ["earlier", "later"]);
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
  assert.match(markup, /data-series-name="series-25"[^>]*d="M500\.00,50\.00"/u);
});

test("extreme finite series keep chart paths and axes finite", () => {
  const maximum = Number.MAX_VALUE;
  const sameSign: TimelinePoint[] = [{
    id: "maximum",
    label: "maximum",
    count: maximum,
    series: { first: maximum, second: maximum },
  }];
  const oppositeSigns: TimelinePoint[] = [{
    id: "opposite",
    label: "opposite",
    count: 0,
    series: { negative: -maximum, positive: maximum },
  }];

  assert.equal(timelineChartModel(sameSign).hasApproximateCoordinates, true);
  for (const [points, stackMode] of [
    [sameSign, "stacked"],
    [sameSign, "stacked100"],
    [oppositeSigns, "none"],
  ] as const) {
    const markup = renderToStaticMarkup(<TimeSeriesLineChart points={points} stackMode={stackMode} />);
    assert.doesNotMatch(markup, /(?:NaN|Infinity)/u);
  }

  const normalized = timelineVisibleStackWindow(
    sameSign,
    timelineChartModel(sameSign).series,
    "Events",
    0,
    2,
    "stacked100",
  );
  assert.deepEqual(normalized.rows[0], [
    { end: 50, raw: maximum, start: 0 },
    { end: 100, raw: maximum, start: 50 },
  ]);
});
