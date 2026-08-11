import assert from "node:assert/strict";
import test from "node:test";

import {
  applyDiagnosticFix,
  completionContextAt,
  getQueryDiagnostic,
  isCursorInQuotedValue,
  utf16OffsetsForUtf8ByteOffsets,
} from "./spl-editor";

test("editor diagnoses and repairs unclosed single-quoted fields", () => {
  const source = `index=main\n| eval 'HTTP Status=1`;
  const diagnostic = getQueryDiagnostic(source);

  assert.equal(diagnostic?.kind, "unclosed-quote");
  assert.equal(diagnostic?.token, "'");
  assert.equal(diagnostic?.line, 2);
  assert.equal(diagnostic?.column, 8);
  assert.match(diagnostic?.message ?? "", /single quotation/);
  assert.equal(diagnostic === null ? source : applyDiagnosticFix(source, diagnostic), `${source}'`);
});

test("editor keeps value and identifier quote contexts out of completions", () => {
  const fieldSource = `index=main | eval 'HTTP`;
  const valueSource = `index=main | search message="serv`;

  assert.equal(completionContextAt(fieldSource, fieldSource.length), null);
  assert.equal(completionContextAt(valueSource, valueSource.length), null);
  assert.equal(isCursorInQuotedValue(fieldSource, fieldSource.length), true);
  assert.equal(isCursorInQuotedValue(valueSource, valueSource.length), true);
});

test("editor source columns count Unicode code points", () => {
  const source = `index=main | eval emoji="🟢"\n| eval 'open`;
  const diagnostic = getQueryDiagnostic(source);

  assert.equal(diagnostic?.line, 2);
  assert.equal(diagnostic?.column, 8);
});

test("editor batch-converts backend UTF-8 byte ranges to safe UTF-16 offsets", () => {
  const source = "aé🟢漢z";

  assert.deepEqual(
    utf16OffsetsForUtf8ByteOffsets(source, [
      11n,
      2n,
      -1n,
      7n,
      6n,
      3n,
      1n,
      0n,
      10n,
      1_000_000_000_000n,
      2n,
    ]),
    [6, 1, 0, 4, 2, 2, 1, 0, 5, source.length, 1],
  );
});
