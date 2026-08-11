import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyBufViolations,
  githubAnnotation,
  parseBufViolations,
  waivedViolations,
  waiverBaseCommit,
} from "./check-buf-breaking.mjs";

function structured(violation, overrides = {}) {
  return {
    ...violation,
    start_line: 10,
    start_column: 2,
    end_line: 11,
    end_column: 3,
    ...overrides,
  };
}

test("parses newline-delimited Buf JSON diagnostics", () => {
  const violations = waivedViolations.slice(0, 2).map(structured);
  assert.deepEqual(
    parseBufViolations(`${violations.map((value) => JSON.stringify(value)).join("\n")}\n`),
    violations,
  );
});

test("rejects non-JSON and incomplete Buf diagnostics", () => {
  assert.throws(() => parseBufViolations("not JSON\n"), /non-JSON diagnostic/u);
  assert.throws(
    () => parseBufViolations(`${JSON.stringify({ path: "contract.proto" })}\n`),
    /missing type/u,
  );
});

test("waives only exact violations against the pinned base commit", () => {
  const violations = waivedViolations.map(structured);
  assert.deepEqual(classifyBufViolations(waiverBaseCommit, violations), {
    waived: violations,
    unexpected: [],
  });

  const otherBase = classifyBufViolations("0".repeat(40), violations);
  assert.deepEqual(otherBase.waived, []);
  assert.deepEqual(otherBase.unexpected, violations);

  const changed = structured(waivedViolations[0], { message: "a different breaking change" });
  assert.deepEqual(classifyBufViolations(waiverBaseCommit, [changed]), {
    waived: [],
    unexpected: [changed],
  });
});

test("formats GitHub annotations with locations and escaped content", () => {
  assert.equal(
    githubAnnotation("error", {
      path: "proto/example:file.proto",
      start_line: 7,
      start_column: 4,
      message: "bad, field%\nnext",
    }),
    "::error file=proto/example%3Afile.proto,line=7,col=4::bad, field%25%0Anext",
  );
});
