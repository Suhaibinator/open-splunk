import assert from "node:assert/strict";
import test from "node:test";

import { ValueType } from "../../gen/ts/open_splunk/v1/value";
import {
  MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES,
  validFlatMultivalueColumnPresentation,
} from "./result-column-presentation";

function valid(delimiter: string | undefined, multivalue = true, valueType = ValueType.VALUE_TYPE_LIST) {
  return validFlatMultivalueColumnPresentation({
    flatMultivalueDelimiter: delimiter,
    multivalue,
	statsSparkline: false,
    valueType,
  });
}

test("flat multivalue column metadata preserves empty versus absent", () => {
  assert.equal(valid(undefined, false, ValueType.VALUE_TYPE_STRING), true);
  assert.equal(valid(""), true);
  assert.equal(valid(" / "), true);
});

test("flat multivalue column metadata rejects type and byte-bound tampering", () => {
  assert.equal(valid(",", false), false);
  assert.equal(valid(",", true, ValueType.VALUE_TYPE_STRING), false);
  assert.equal(valid("\ud800"), false);
  assert.equal(valid("x".repeat(MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES + 1)), false);
});

test("stats sparkline presentation is exclusive to typed multivalue lists", () => {
  assert.equal(validFlatMultivalueColumnPresentation({
    flatMultivalueDelimiter: undefined,
    multivalue: true,
    statsSparkline: true,
    valueType: ValueType.VALUE_TYPE_LIST,
  }), true);
  assert.equal(validFlatMultivalueColumnPresentation({
    flatMultivalueDelimiter: " ",
    multivalue: true,
    statsSparkline: true,
    valueType: ValueType.VALUE_TYPE_LIST,
  }), false);
  assert.equal(validFlatMultivalueColumnPresentation({
    flatMultivalueDelimiter: undefined,
    multivalue: false,
    statsSparkline: true,
    valueType: ValueType.VALUE_TYPE_STRING,
  }), false);
});
