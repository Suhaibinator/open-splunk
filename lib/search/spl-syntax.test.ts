import assert from "node:assert/strict";
import test from "node:test";

import {
  firstSplPipelineBoundary,
  isSupportedSplPipelineCommand,
  isScalarExpressionPipelineCommand,
  isSplOffsetInDoubleQuotedValue,
  isSplOffsetInQuotedValue,
  scanSplStructure,
  splitSplPipeline,
} from "./spl-syntax";

const V03_PIPELINE_COMMANDS = [
  "regex",
  "reverse",
  "accum",
  "strcat",
  "addinfo",
  "fillnull",
  "addtotals",
  "delta",
  "makemv",
  "mvexpand",
] as const;

test("v0.3 commands are browser-supported through the shared catalog", () => {
  for (const command of V03_PIPELINE_COMMANDS) {
    assert.equal(isSupportedSplPipelineCommand(command), true, command);
    assert.equal(isSupportedSplPipelineCommand(command.toUpperCase()), true, command);
    assert.equal(isScalarExpressionPipelineCommand(command), false, command);
  }

  for (const unsupported of ["transaction", "join", "map", "subsearch"]) {
    assert.equal(isSupportedSplPipelineCommand(unsupported), false, unsupported);
  }
});

test("v0.3 quoted Unicode values never mint browser pipeline boundaries", () => {
  const source = String.raw`index=main | regex message="timeout|拒否" | reverse | accum bytes AS running | strcat host "|💥" route endpoint | addinfo | fillnull value="unknown|界" optional | addtotals fieldname=total bytes running | delta running AS step p=2 | makemv delim="💥|界" allowempty=true tags | mvexpand tags limit=2`;
  const stages = splitSplPipeline(source).map((stage) => stage.trim());

  assert.equal(scanSplStructure(source).unclosedQuote, null);
  assert.equal(scanSplStructure(source).pipes.length, V03_PIPELINE_COMMANDS.length);
  assert.equal(stages.length, V03_PIPELINE_COMMANDS.length + 1);
  assert.deepEqual(
    stages.slice(1).map((stage) => stage.split(/\s/u)[0]?.toLowerCase()),
    V03_PIPELINE_COMMANDS,
  );
});

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

test("count eval predicates expose nested scalar ranges in every supported aggregate", () => {
  const source = String.raw`index=main | stats count(eval('HTTP|Status' IN (500, 503))) AS errors | eventstats count(eval('request,bytes'/2>100)) AS large | streamstats count(eval('owner\'s field' NOT IN ("bot"))) AS human`;
  const structure = scanSplStructure(source);

  assert.equal(structure.unclosedQuote, null);
  assert.equal(structure.pipes.length, 3);
  assert.deepEqual(
    structure.scalarStageRanges.map((range) =>
      source.slice(range.startOffset, range.endOffset)
    ),
    [
      `'HTTP|Status' IN (500, 503)`,
      `'request,bytes'/2>100`,
      String.raw`'owner\'s field' NOT IN ("bot")`,
    ],
  );
  assert.deepEqual(
    structure.quotes.filter(({ quote }) => quote === "'").map(({ offset, endOffset }) =>
      source.slice(offset, endOffset)
    ),
    ["'HTTP|Status'", "'request,bytes'", String.raw`'owner\'s field'`],
  );
});

test("count eval scanning handles a long aggregate-stage prefix", () => {
  const padding = " ".repeat(15_000);
  const predicates = [`'HTTP Status' >= 500`, "status == 503"];
  const source = `index=main | stats${padding}CoUnT ( EvAl (${predicates[0]})) AS errors, count(eval(${predicates[1]})) AS unavailable`;
  const structure = scanSplStructure(source);
  const expectedRanges = predicates.map((predicate) => ({
    startOffset: source.indexOf(predicate),
    endOffset: source.indexOf(predicate) + predicate.length,
  }));

  assert.ok(source.length > 15_000);
  assert.ok(source.length < 16 * 1024);
  assert.equal(structure.unclosedQuote, null);
  assert.deepEqual(structure.scalarStageRanges, expectedRanges);
  assert.deepEqual(
    structure.scalarStageRanges.map(({ startOffset, endOffset }) =>
      source.slice(startOffset, endOffset)
    ),
    predicates,
  );
});

test("an unclosed count eval field quote retains inner pipes as scalar text", () => {
  const source = `index=main | stats count(eval('open|field`;
  const structure = scanSplStructure(source);

  assert.equal(structure.pipes.length, 1);
  assert.deepEqual(structure.unclosedQuote, {
    offset: source.indexOf("'open"),
    quote: "'",
  });
  assert.equal(
    source.slice(
      structure.scalarStageRanges[0]?.startOffset,
      structure.scalarStageRanges[0]?.endOffset,
    ),
    `'open|field`,
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
