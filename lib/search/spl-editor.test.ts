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

test("editor diagnoses an unclosed field inside count eval without splitting its pipe", () => {
  const source = `index=main\n| stats count(eval('HTTP|Status`;
  const diagnostic = getQueryDiagnostic(source);

  assert.equal(diagnostic?.kind, "unclosed-quote");
  assert.equal(diagnostic?.token, "'");
  assert.equal(diagnostic?.line, 2);
  assert.equal(diagnostic?.column, 20);
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

test("editor completion context tells command, term and value fragments apart", () => {
  assert.deepEqual(completionContextAt("index=main | sta", 16), {
    fragmentStart: 13, fragmentEnd: 16, prefix: "sta", followsPipeline: true, stage: "command",
  });
  assert.deepEqual(completionContextAt("index=", 6), {
    fragmentStart: 6, fragmentEnd: 6, prefix: "", followsPipeline: false, stage: "value", fieldName: "index",
  });
  assert.deepEqual(completionContextAt("index=ma", 8), {
    fragmentStart: 6, fragmentEnd: 8, prefix: "ma", followsPipeline: false, stage: "value", fieldName: "index",
  });
  assert.deepEqual(completionContextAt("index=main status!=5", 20), {
    fragmentStart: 19, fragmentEnd: 20, prefix: "5", followsPipeline: false, stage: "value", fieldName: "status",
  });
  assert.deepEqual(completionContextAt("index=main duration_ms >= 25", 28), {
    fragmentStart: 26, fragmentEnd: 28, prefix: "25", followsPipeline: false, stage: "value", fieldName: "duration_ms",
  });
  assert.deepEqual(completionContextAt("index=main | where sta", 22), {
    fragmentStart: 19, fragmentEnd: 22, prefix: "sta", followsPipeline: true, stage: "term",
  });
  assert.deepEqual(completionContextAt("index=main | stats count(du", 27), {
    fragmentStart: 25, fragmentEnd: 27, prefix: "du", followsPipeline: true, stage: "term",
  });
  assert.deepEqual(completionContextAt("index=main | sort -_ti", 22), {
    fragmentStart: 19, fragmentEnd: 22, prefix: "_ti", followsPipeline: true, stage: "term",
  });
  // The implicit search head is command position for insertion, but its bare
  // word is a search term.
  assert.deepEqual(completionContextAt("inde", 4), {
    fragmentStart: 0, fragmentEnd: 4, prefix: "inde", followsPipeline: false, stage: "command",
  });
  // The caret, not the end of the text, decides the fragment.
  assert.deepEqual(completionContextAt("index=main level=ERROR", 17), {
    fragmentStart: 17, fragmentEnd: 17, prefix: "", followsPipeline: false, stage: "value", fieldName: "level",
  });
  // Nothing under the caret to complete.
  assert.equal(completionContextAt("index=main | head 5 ", 20), null);
  assert.equal(completionContextAt("index=main (level=ERROR)", 24), null);
  // A value being spelled inside quotes stays the user's.
  assert.equal(completionContextAt('index=main method="G', 20), null);
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
