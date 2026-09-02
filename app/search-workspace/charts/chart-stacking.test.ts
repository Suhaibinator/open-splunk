import assert from "node:assert/strict";
import test from "node:test";

import { stackChartRows, stackedChartDomain } from "./chart-stacking";

test("stacking uses independent positive and negative baselines", () => {
  const [row] = stackChartRows([[4, -2, 3, -5]], "stacked");

  assert.deepEqual(row, [
    { end: 4, raw: 4, start: 0 },
    { end: -2, raw: -2, start: 0 },
    { end: 7, raw: 3, start: 4 },
    { end: -7, raw: -5, start: -2 },
  ]);
  assert.deepEqual(stackedChartDomain([row]), [0, 4, 0, -2, 4, 7, -2, -7]);
});

test("100 percent stacking normalizes each sign independently", () => {
  const [row] = stackChartRows([[1, 3, -2, -6]], "stacked100");

  assert.deepEqual(row, [
    { end: 25, raw: 1, start: 0 },
    { end: 100, raw: 3, start: 25 },
    { end: -25, raw: -2, start: 0 },
    { end: -100, raw: -6, start: -25 },
  ]);
});

test("zero and missing totals remain finite", () => {
  const rows = stackChartRows([[0, null, Number.NaN]], "stacked100");

  assert.deepEqual(rows, [[
    { end: 0, raw: 0, start: 0 },
    { end: 0, raw: null, start: 0 },
    { end: 0, raw: null, start: 0 },
  ]]);
});

test("none mode leaves every series on the zero baseline", () => {
  assert.deepEqual(stackChartRows([[2, -3]], "none"), [[
    { end: 2, raw: 2, start: 0 },
    { end: -3, raw: -3, start: 0 },
  ]]);
});
