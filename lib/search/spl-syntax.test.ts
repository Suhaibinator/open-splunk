import assert from "node:assert/strict";
import test from "node:test";

import {
  firstSplPipelineBoundary,
  isScalarExpressionPipelineCommand,
  isSplOffsetInDoubleQuotedValue,
  isSplOffsetInQuotedValue,
  scanSplStructure,
  splitSplPipeline,
} from "./spl-syntax";

test("SPL structure scanner ignores separators in value and field quotes", () => {
  const source = String.raw`index=main message="a|b" | eval 'request|bytes'=1, note="x\"|y" | where 'request|bytes'>0`;
  const structure = scanSplStructure(source);

  assert.equal(structure.unclosedQuote, null);
  assert.deepEqual(
    structure.pipes.map((offset) => source.slice(offset, offset + 1)),
    ["|", "|"],
  );
  assert.equal(splitSplPipeline(source).length, 3);
  assert.equal(firstSplPipelineBoundary(source), structure.pipes[0]);
  assert.deepEqual(
    structure.scalarStageRanges.map((range) =>
      source.slice(range.startOffset, range.endOffset).trimStart().split(/\s/u)[0]
    ),
    ["eval", "where"],
  );
});

test("scalar-stage classification is case-insensitive and excludes other stages", () => {
  const source =
    "index=main | search status=200 | EVAL ratio=duration_ms+1 | WHERE ratio>1 | table ratio";
  const structure = scanSplStructure(source);

  assert.equal(isScalarExpressionPipelineCommand("EVAL"), true);
  assert.equal(isScalarExpressionPipelineCommand("where"), true);
  assert.equal(isScalarExpressionPipelineCommand("search"), false);
  assert.deepEqual(
    structure.scalarStageRanges.map((range) =>
      source.slice(range.startOffset, range.endOffset).trimStart().split(/\s/u)[0]
    ),
    ["EVAL", "WHERE"],
  );
});

test("SPL structure scanner reports the exact unclosed quote kind", () => {
  assert.deepEqual(scanSplStructure(`index=main | eval x="open`).unclosedQuote, {
    offset: 20,
    quote: '"',
  });
  assert.deepEqual(scanSplStructure(`index=main | eval 'HTTP Status'=1 | where 'open`).unclosedQuote, {
    offset: 42,
    quote: "'",
  });
});

test("quoted-offset helpers distinguish field identifiers from values", () => {
  const source = `index=main | eval 'HTTP Status'="server error"`;
  const fieldOffset = source.indexOf("HTTP") + 2;
  const valueOffset = source.indexOf("server") + 2;

  assert.equal(isSplOffsetInQuotedValue(source, fieldOffset), true);
  assert.equal(isSplOffsetInDoubleQuotedValue(source, fieldOffset), false);
  assert.equal(isSplOffsetInQuotedValue(source, valueOffset), true);
  assert.equal(isSplOffsetInDoubleQuotedValue(source, valueOffset), true);
  assert.equal(isSplOffsetInQuotedValue(source, source.length), false);
});

test("single quotes remain ordinary punctuation outside scalar command stages", () => {
  const source = `index=main O'Reilly | eval label="O'Reilly"`;
  const structure = scanSplStructure(source);

  assert.equal(structure.unclosedQuote, null);
  assert.equal(structure.pipes.length, 1);
  assert.equal(splitSplPipeline(source)[0], `index=main O'Reilly `);
});
