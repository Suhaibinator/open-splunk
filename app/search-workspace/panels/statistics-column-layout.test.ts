import assert from "node:assert/strict";
import test from "node:test";

import {
  createColumnLayout,
  createColumnLayoutDomain,
  reconcileColumnLayout,
  resizeColumn,
  selectColumnLayoutWindow,
  StatisticsColumnLayoutStore,
  toggleColumn,
  visibleColumns,
  visibleColumnWidth,
} from "./statistics-column-layout";

const layout = createColumnLayout([
  { id: "host", defaultWidth: 180, minimumWidth: 96, maximumWidth: 480 },
  { id: "count", defaultWidth: 140, minimumWidth: 96, maximumWidth: 480 },
]);

test("statistics columns resize without mutating the previous layout", () => {
  const resized = resizeColumn(layout, "host", 16);

  assert.equal(layout[0]?.width, 180);
  assert.equal(resized[0]?.width, 196);
  assert.equal(resized[1]?.width, 140);
});

test("statistics column resize clamps invalid and extreme widths", () => {
  assert.equal(resizeColumn(layout, "host", -10_000)[0]?.width, 96);
  assert.equal(resizeColumn(layout, "host", 10_000)[0]?.width, 480);
  assert.deepEqual(resizeColumn(layout, "host", Number.NaN), layout);
});

test("statistics columns cannot hide the final visible column", () => {
  const hostHidden = toggleColumn(layout, "host");
  const allHidden = toggleColumn(hostHidden, "count");

  assert.deepEqual(visibleColumns(hostHidden).map((column) => column.id), ["count"]);
  assert.deepEqual(visibleColumns(allHidden).map((column) => column.id), ["count"]);
  assert.deepEqual(visibleColumns(toggleColumn(allHidden, "host")).map((column) => column.id), ["host", "count"]);
  assert.equal(visibleColumnWidth(hostHidden), 140);
});

test("statistics layout reconciliation follows the latest schema order", () => {
  const customized = resizeColumn(toggleColumn(layout, "count"), "host", 20);
  const reconciled = reconcileColumnLayout(customized, [
    { id: "count", defaultWidth: 120, minimumWidth: 96, maximumWidth: 480 },
    { id: "duration", defaultWidth: 160, minimumWidth: 96, maximumWidth: 480 },
    { id: "host", defaultWidth: 200, minimumWidth: 96, maximumWidth: 480 },
  ]);

  assert.deepEqual(reconciled, [
    { id: "count", maximumWidth: 480, minimumWidth: 96, visible: false, width: 140 },
    { id: "duration", maximumWidth: 480, minimumWidth: 96, visible: true, width: 160 },
    { id: "host", maximumWidth: 480, minimumWidth: 96, visible: true, width: 200 },
  ]);
});

test("statistics layout reconciliation keeps a column visible after schema removal", () => {
  const hostHidden = toggleColumn(layout, "host");
  const reconciled = reconcileColumnLayout(hostHidden, [
    { id: "host", defaultWidth: 180, minimumWidth: 96, maximumWidth: 480 },
  ]);

  assert.deepEqual(visibleColumns(reconciled).map((column) => column.id), ["host"]);
});

test("wide column windows use one indexed lookup per candidate", () => {
  const columns = Array.from({ length: 4_096 }, (_, index) => ({
    id: `field-${index}`,
    defaultWidth: 140,
    minimumWidth: 96,
    maximumWidth: 480,
  }));
  const domain = createColumnLayoutDomain(createColumnLayout(columns), columns);
  let lookupCount = 0;
  const countingIndex: ReadonlyMap<string, (typeof domain.layout)[number]> = {
    get size() {
      return domain.byId.size;
    },
    entries: () => domain.byId.entries(),
    forEach: (callback, thisArgument) => domain.byId.forEach(callback, thisArgument),
    get: (id) => {
      lookupCount += 1;
      return domain.byId.get(id);
    },
    has: (id) => domain.byId.has(id),
    keys: () => domain.byId.keys(),
    values: () => domain.byId.values(),
    [Symbol.iterator]: () => domain.byId[Symbol.iterator](),
  };

  const window = selectColumnLayoutWindow(columns, countingIndex, 2_040, 24);

  assert.equal(lookupCount, 24);
  assert.equal(window.columns.length, 24);
  assert.equal(window.layout.length, 24);
  assert.equal(window.columns[0]?.id, "field-2040");
  assert.equal(window.columns.at(-1)?.id, "field-2063");
});

test("layout store retains sparse overrides with bounded whole-query LRU eviction", () => {
  const store = new StatisticsColumnLayoutStore({ maximumBytes: 4_096, maximumEntries: 2 });
  const columns = [
    { id: "host", defaultWidth: 180, minimumWidth: 96, maximumWidth: 480 },
    { id: "count", defaultWidth: 140, minimumWidth: 96, maximumWidth: 480 },
  ];
  const defaultLayout = createColumnLayout(columns);
  const customized = resizeColumn(toggleColumn(defaultLayout, "count"), "host", 20);

  store.set("query-one", customized, columns);
  store.set("query-two", resizeColumn(defaultLayout, "count", 10), columns);
  assert.deepEqual(store.get("query-one", columns), customized);

  store.set("query-three", resizeColumn(defaultLayout, "host", -10), columns);

  assert.equal(store.size, 2);
  assert.equal(store.get("query-two", columns), undefined);
  assert.deepEqual(store.get("query-one", columns), customized);
  assert.ok(store.retainedBytes <= 4_096);
});

test("layout store does not retain default schemas or partial oversized overrides", () => {
  const columns = Array.from({ length: 1_024 }, (_, index) => ({
    id: `field-${index}`,
    defaultWidth: 140,
    minimumWidth: 96,
    maximumWidth: 480,
  }));
  const store = new StatisticsColumnLayoutStore({ maximumBytes: 128, maximumEntries: 2 });
  const defaultLayout = createColumnLayout(columns);

  store.set("default-wide-query", defaultLayout, columns);
  assert.equal(store.size, 0);
  assert.equal(store.retainedBytes, 0);

  const customized = defaultLayout.map((column) => ({
    id: column.id,
    maximumWidth: column.maximumWidth,
    minimumWidth: column.minimumWidth,
    visible: column.visible,
    width: 160,
  }));
  store.set("oversized-query", customized, columns);
  assert.equal(store.get("oversized-query", columns), undefined);
  assert.equal(store.size, 0);
});
