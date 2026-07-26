import assert from "node:assert/strict";
import test from "node:test";

import {
  MAXIMUM_SAVED_SEARCH_NAME_BYTES,
  duplicateSavedSearchName,
  nextDuplicateSavedSearchName,
  savedSearchNameValidationError,
} from "./saved-search-names";

test("saved-search names enforce the server UTF-8 byte boundary", () => {
  assert.equal(savedSearchNameValidationError("a".repeat(255)), null);
  assert.match(savedSearchNameValidationError("a".repeat(256)) ?? "", /255 UTF-8 bytes/);
  assert.equal(savedSearchNameValidationError("🟢".repeat(63)), null);
  assert.match(savedSearchNameValidationError("🟢".repeat(64)) ?? "", /uses 256/);
});

test("saved-search names reject empty values and embedded controls", () => {
  assert.match(savedSearchNameValidationError(" \t\n ") ?? "", /Enter/);
  assert.match(savedSearchNameValidationError("valid\u0000name") ?? "", /control/);
  assert.equal(savedSearchNameValidationError("  useful name  "), null);
});

test("duplicate suffixes remain within the byte limit and avoid loaded names", () => {
  const original = "🟢".repeat(63);
  const first = duplicateSavedSearchName(original);
  const second = nextDuplicateSavedSearchName(original, [first]);
  assert.ok(new TextEncoder().encode(first).byteLength <= MAXIMUM_SAVED_SEARCH_NAME_BYTES);
  assert.ok(new TextEncoder().encode(second).byteLength <= MAXIMUM_SAVED_SEARCH_NAME_BYTES);
  assert.equal(savedSearchNameValidationError(first), null);
  assert.equal(savedSearchNameValidationError(second), null);
  assert.notEqual(first, second);
});
