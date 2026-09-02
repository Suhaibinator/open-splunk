import assert from "node:assert/strict";
import test from "node:test";

import {
  createColumnLayout,
  reconcileColumnLayout,
  resizeColumn,
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
