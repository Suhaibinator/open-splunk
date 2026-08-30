import assert from "node:assert/strict";
import test from "node:test";

import {
  INDEX_MAX_EVENT_BYTES,
  INDEX_POLICY_FIELDS,
  INDEX_POLICY_KEYS,
  INGESTION_MAX_BYTES_PER_SECOND,
  TOKEN_POLICY_KEYS,
  indexPolicyErrors,
  indexPolicyFieldHint,
  indexPolicyFormFromDefinition,
  indexPolicyFromForm,
  indexPolicyIsValid,
  normalizeTokenPatterns,
  policyFieldInputMode,
  tokenPatternsError,
  tokenPolicyErrors,
  tokenPolicyFormFromToken,
  tokenPolicyFromForm,
  tokenPolicyIsValid,
  type IndexPolicyForm,
} from "./ingestion-policy-form";

const empty = indexPolicyFormFromDefinition();

test("an unset policy is every field blank, and every field blank is acceptable", () => {
  assert.deepEqual(empty, {
    defaultSourcetype: "",
    maxEventBytes: "",
    maxEventsPerSecond: "",
    maxFieldCount: "",
    maxNestingDepth: "",
    maximumEventAgeSeconds: "",
    maximumFutureSkewSeconds: "",
    maxUncompressedBytesPerSecond: "",
  });
  assert.equal(indexPolicyIsValid(empty), true);
  assert.deepEqual(indexPolicyFromForm(empty).limits, {
    maxEventBytes: undefined,
    maxFieldCount: undefined,
    maxNestingDepth: undefined,
    maximumFutureSkew: undefined,
    maximumEventAge: undefined,
  });
});

test("a byte field holds a quantity, so the documented maximum can be typed as it is written", () => {
  // The defect: the hint said "maximum 1 MiB" and the field took a raw byte
  // count, so setting the documented maximum meant typing 1048576 by hand.
  const form: IndexPolicyForm = { ...empty, maxEventBytes: "1 MiB" };
  assert.equal(indexPolicyErrors(form).maxEventBytes, null);
  assert.equal(indexPolicyFromForm(form).limits.maxEventBytes, INDEX_MAX_EVENT_BYTES);
  // And the byte count it always accepted still reads the same.
  assert.equal(indexPolicyFromForm({ ...empty, maxEventBytes: "1048576" }).limits.maxEventBytes, INDEX_MAX_EVENT_BYTES);
});

test("a byte field still reaches values no whole unit can state", () => {
  // This is why these fields take a quantity rather than a count of mebibytes:
  // a ceiling of 1 MiB has to be able to say 512 KiB.
  assert.equal(indexPolicyFromForm({ ...empty, maxEventBytes: "512 KiB" }).limits.maxEventBytes, 524_288n);
  assert.equal(indexPolicyFromForm({ ...empty, maxEventBytes: "1000" }).limits.maxEventBytes, 1_000n);
});

test("every hint states its ceiling by formatting the constant the parser enforces", () => {
  // The old hints were prose beside the constant and free to drift from it.
  assert.equal(
    indexPolicyFieldHint("maxEventBytes", empty),
    "Zero or blank inherits the server limit; maximum 1 MiB.",
  );
  assert.equal(
    indexPolicyFieldHint("maxUncompressedBytesPerSecond", empty),
    "Uncompressed event bytes per second. Zero or blank is unlimited; maximum 1 TiB.",
  );
  assert.equal(
    indexPolicyFieldHint("maxFieldCount", empty),
    "Zero or blank inherits the server limit; maximum 1,024 fields.",
  );
});

test("a byte field echoes the exact count it read when the text is not already that count", () => {
  assert.equal(
    indexPolicyFieldHint("maxEventBytes", { ...empty, maxEventBytes: "512KB" }),
    "512,000 bytes. Zero or blank inherits the server limit; maximum 1 MiB.",
  );
  assert.equal(
    indexPolicyFieldHint("maxEventBytes", { ...empty, maxEventBytes: "512 KiB" }),
    "Zero or blank inherits the server limit; maximum 1 MiB.",
  );
});

test("a value over the ceiling is reported in the notation the field is entered in", () => {
  assert.equal(indexPolicyErrors({ ...empty, maxEventBytes: "2 MiB" }).maxEventBytes, "Enter 1 MiB or less.");
  assert.equal(indexPolicyErrors({ ...empty, maxFieldCount: "2000" }).maxFieldCount, "Enter 1,024 fields or less.");
  assert.equal(
    indexPolicyErrors({ ...empty, maximumFutureSkewSeconds: "301" }).maximumFutureSkewSeconds,
    "Enter 300 seconds or less.",
  );
});

test("an unparseable value names the shape the field takes rather than throwing at submit", () => {
  // These fields had no validation at all until submit, where the parser threw
  // and the reason arrived as a toast that highlighted nothing.
  const errors = indexPolicyErrors({
    ...empty,
    maxEventBytes: "big",
    maxFieldCount: "1.5",
    maximumEventAgeSeconds: "10.00005",
  });
  assert.equal(errors.maxEventBytes, "Enter a size such as 1 MiB, or a plain number of bytes.");
  assert.equal(errors.maxFieldCount, "Enter a whole number, or leave the field blank.");
  assert.equal(errors.maximumEventAgeSeconds, "Enter seconds with at most three decimal places, or leave the field blank.");
});

test("the submit-time throw carries the message the field was already showing", () => {
  // The two cannot report different things, because the throw routes through
  // the validator the field displays.
  assert.throws(
    () => indexPolicyFromForm({ ...empty, maxEventBytes: "2 MiB" }),
    /Maximum event size: Enter 1 MiB or less\./u,
  );
  assert.throws(
    () => tokenPolicyFromForm({
      allowedHostRegexes: "",
      allowedSourceRegexes: "",
      maxEventsPerSecond: "",
      maxUncompressedBytesPerSecond: "2 TiB",
    }),
    /Maximum ingestion rate: Enter 1 TiB or less\./u,
  );
});

test("a label names a unit exactly when the value does not carry one", () => {
  // Both halves matter. A byte label that named a scale would be the second
  // place a scale is stated, which is the shape the original defect had; a
  // seconds field states a plain number, so dropping "(seconds)" from its label
  // would leave the unit nowhere but the hint.
  for (const key of INDEX_POLICY_KEYS) {
    const field = INDEX_POLICY_FIELDS[key];
    if (field.kind === "bytes") assert.doesNotMatch(field.label, /byte|MiB|MB|GiB|TiB/iu, key);
    if (field.kind === "seconds") assert.match(field.label, /\(seconds\)$/u, key);
  }
});

test("blank and zero are the same unset value, in seconds as well as in counts", () => {
  for (const value of ["", "0", " 0 "]) {
    assert.equal(indexPolicyErrors({ ...empty, maxEventBytes: value }).maxEventBytes, null, value);
    assert.equal(indexPolicyFromForm({ ...empty, maxEventBytes: value }).limits.maxEventBytes, undefined, value);
  }
  for (const value of ["", "0", "0.000"]) {
    const form = { ...empty, maximumFutureSkewSeconds: value };
    assert.equal(indexPolicyErrors(form).maximumFutureSkewSeconds, null, value);
    assert.equal(indexPolicyFromForm(form).limits.maximumFutureSkew, undefined, value);
  }
});

test("a duration keeps the milliseconds a Duration message can hold", () => {
  assert.deepEqual(
    indexPolicyFromForm({ ...empty, maximumFutureSkewSeconds: "1.5" }).limits.maximumFutureSkew,
    { seconds: 1n, nanos: 500_000_000 },
  );
});

test("a rate limit round-trips through the form it is displayed in", () => {
  const token = {
    ingestionRateLimits: {
      maxEventsPerSecond: 50_000n,
      maxUncompressedBytesPerSecond: INGESTION_MAX_BYTES_PER_SECOND,
    },
  } as Parameters<typeof tokenPolicyFormFromToken>[0];
  const form = tokenPolicyFormFromToken(token);
  assert.equal(form.maxUncompressedBytesPerSecond, "1 TiB");
  assert.equal(form.maxEventsPerSecond, "50000");
  assert.equal(tokenPolicyIsValid(form), true);
  assert.deepEqual(tokenPolicyFromForm(form).ingestionRateLimits, {
    maxEventsPerSecond: 50_000n,
    maxUncompressedBytesPerSecond: INGESTION_MAX_BYTES_PER_SECOND,
  });
});

test("both rate fields are shared between the index form and the token form", () => {
  // One limit, one label, one ceiling: the two forms used to state them twice.
  assert.deepEqual(TOKEN_POLICY_KEYS, ["maxEventsPerSecond", "maxUncompressedBytesPerSecond"]);
  for (const key of TOKEN_POLICY_KEYS) assert.ok(INDEX_POLICY_KEYS.includes(key), key);
});

test("pattern lists report the bound they exceed", () => {
  assert.equal(tokenPatternsError(""), null);
  assert.equal(tokenPatternsError("api-[0-9]+\nworker-[0-9]+"), null);
  assert.equal(
    tokenPatternsError(Array.from({ length: 17 }, (_, index) => `host-${index}`).join("\n")),
    "Enter at most 16 unique patterns.",
  );
  assert.equal(tokenPatternsError("x".repeat(513)), "Each pattern must be 512 UTF-8 bytes or fewer.");
  assert.equal(tokenPatternsError("bad\0pattern"), "A pattern cannot contain a NUL character.");
  assert.equal(
    // Distinct patterns: identical ones are one pattern, and 300 bytes is well
    // inside every other bound, so only the total can be what rejects these.
    tokenPatternsError(Array.from({ length: 16 }, (_, index) => `${"x".repeat(300)}${index}`).join("\n")),
    "The patterns must be 4,096 UTF-8 bytes or fewer in total.",
  );
});

test("a duplicate pattern is one pattern, in both the text and the array form", () => {
  const errors = tokenPolicyErrors({
    allowedHostRegexes: Array.from({ length: 20 }, () => "worker-[0-9]+").join("\n"),
    allowedSourceRegexes: "",
    maxEventsPerSecond: "",
    maxUncompressedBytesPerSecond: "",
  });
  assert.equal(errors.allowedHostRegexes, null);
  assert.deepEqual(normalizeTokenPatterns(["b", "a", "b"], "Allowed host"), ["a", "b"]);
});

test("the array form rejects an empty pattern a textarea could never produce", () => {
  // It is reached from the persisted token-create guard, whose patterns come
  // back from storage rather than from a field.
  assert.throws(() => normalizeTokenPatterns(["ok", ""], "Allowed host"), /cannot be empty/u);
});

test("a field that accepts a decimal asks for a keyboard that has one", () => {
  // Folding eight hand-written policy inputs into one component collapsed the
  // two `inputMode="decimal"` fields onto the numeric default, and the plain
  // numeric keypad has no decimal separator -- so "1.5" became untypeable on a
  // touch device in a field that still accepts it.
  assert.equal(policyFieldInputMode("seconds"), "decimal");
  assert.equal(policyFieldInputMode("count"), "numeric");
  // A quantity carries letters, so it needs the full keyboard.
  assert.equal(policyFieldInputMode("bytes"), "text");
  for (const key of INDEX_POLICY_KEYS) {
    const field = INDEX_POLICY_FIELDS[key];
    if (field.kind !== "seconds") continue;
    assert.equal(policyFieldInputMode(field.kind), "decimal", key);
    // The kind is only worth checking if the field really does take a fraction.
    assert.equal(indexPolicyErrors({ ...empty, [key]: "1.5" })[key], null, key);
  }
});
