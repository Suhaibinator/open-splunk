import assert from "node:assert/strict";
import test from "node:test";

import { linearTickScale, projectScaleValue } from "./chart-scale";

test("ordinary chart ticks retain their nice scale", () => {
  assert.deepEqual(linearTickScale([-3, 7]), {
    minimum: -5,
    maximum: 10,
    ticks: [10, 5, 0, -5],
  });
});

test("extreme finite chart domains have bounded finite ordered ticks", () => {
  const cases = [
    [-1e308, 1e308],
    [-Number.MAX_VALUE, Number.MAX_VALUE],
    [Number.MAX_VALUE],
    [-Number.MAX_VALUE],
    [Number.MIN_VALUE],
    [-Number.MIN_VALUE, Number.MIN_VALUE],
    [1e-310, 3e-310],
    [-5e301, 3e-297],
    [-3e-297, 5e301],
    [],
  ];
  for (const values of cases) {
    const scale = linearTickScale(values);
    assert.ok(Number.isFinite(scale.minimum));
    assert.ok(Number.isFinite(scale.maximum));
    assert.ok(scale.maximum > scale.minimum);
    assert.ok(scale.ticks.length >= 2 && scale.ticks.length <= 33);
    for (const [index, tick] of scale.ticks.entries()) {
      assert.ok(Number.isFinite(tick));
      if (index > 0) assert.ok(tick < scale.ticks[index - 1]);
    }
    for (const value of values) {
      assert.ok(value >= scale.minimum && value <= scale.maximum);
      const coordinate = projectScaleValue(value, scale);
      assert.ok(Number.isFinite(coordinate) && coordinate >= 0 && coordinate <= 1);
    }
  }
});

test("opposite finite extrema project proportionally without overflowing", () => {
  const scale = linearTickScale([-Number.MAX_VALUE, Number.MAX_VALUE]);
  assert.equal(projectScaleValue(-Number.MAX_VALUE, scale), 0);
  assert.equal(projectScaleValue(0, scale), 0.5);
  assert.equal(projectScaleValue(Number.MAX_VALUE, scale), 1);
  assert.equal(projectScaleValue(Number.MAX_VALUE / 2, scale), 0.75);
});
