import assert from "node:assert/strict";
import test from "node:test";

import {
  calculateVirtualTableWindow,
  maximumVirtualTableScrollTop,
  VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS,
} from "./virtual-table";

test("small tables retain every row without spacer geometry", () => {
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 2,
    rowCount: VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS,
    rowHeight: 42,
    scrollTop: 500,
    viewportHeight: 420,
    overscan: 6,
  }), {
    virtualized: false,
    startIndex: 0,
    endIndex: VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS,
    paddingTop: 0,
    paddingBottom: 0,
  });
});

test("large tables materialize a bounded top window", () => {
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 2,
    rowCount: 1_000,
    rowHeight: 42,
    scrollTop: 0,
    viewportHeight: 420,
    overscan: 6,
  }), {
    virtualized: true,
    startIndex: 0,
    endIndex: 16,
    paddingTop: 0,
    paddingBottom: 41_328,
  });
});

test("large tables retain overscan around a middle window", () => {
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 2,
    rowCount: 1_000,
    rowHeight: 42,
    scrollTop: 21_000,
    viewportHeight: 420,
    overscan: 6,
  }), {
    virtualized: true,
    startIndex: 494,
    endIndex: 516,
    paddingTop: 20_748,
    paddingBottom: 20_328,
  });
});

test("large tables clamp an overscrolled bottom window without losing the final row", () => {
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 2,
    rowCount: 1_000,
    rowHeight: 52,
    scrollTop: Number.POSITIVE_INFINITY,
    viewportHeight: 520,
    overscan: 6,
  }), {
    virtualized: true,
    startIndex: 984,
    endIndex: 1_000,
    paddingTop: 51_168,
    paddingBottom: 0,
  });
});

test("shrinking below the virtualization threshold clears the offset before regrowth", () => {
  const clampedScrollTop = Math.min(
    35_000,
    maximumVirtualTableScrollTop({
      columnCount: 1,
      rowCount: VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS,
      rowHeight: 42,
      viewportHeight: 420,
    }),
  );
  assert.equal(clampedScrollTop, 0);
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 1,
    rowCount: VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS + 1,
    rowHeight: 42,
    scrollTop: clampedScrollTop,
    viewportHeight: 420,
    overscan: 6,
  }), {
    virtualized: true,
    startIndex: 0,
    endIndex: 16,
    paddingTop: 0,
    paddingBottom: 3_570,
  });
});

test("wide tables virtualize before exceeding the unvirtualized cell budget", () => {
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 64,
    rowCount: 32,
    rowHeight: 42,
    scrollTop: 500,
    viewportHeight: 420,
    overscan: 6,
  }), {
    virtualized: false,
    startIndex: 0,
    endIndex: 32,
    paddingTop: 0,
    paddingBottom: 0,
  });
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 64,
    rowCount: 33,
    rowHeight: 42,
    scrollTop: 0,
    viewportHeight: 420,
    overscan: 6,
  }), {
    virtualized: true,
    startIndex: 0,
    endIndex: 16,
    paddingTop: 0,
    paddingBottom: 714,
  });
});

test("window inputs are normalized to finite non-negative geometry", () => {
  assert.deepEqual(calculateVirtualTableWindow({
    columnCount: 2,
    rowCount: 101.9,
    rowHeight: 42,
    scrollTop: -20,
    viewportHeight: Number.NaN,
    overscan: -4,
  }), {
    virtualized: true,
    startIndex: 0,
    endIndex: 1,
    paddingTop: 0,
    paddingBottom: 4_200,
  });
});
