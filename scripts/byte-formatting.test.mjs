// One module owns how a byte count is read, written and displayed.
//
// This grew back five times before it was folded. `formatBinaryBytes` in the
// search workspace, `formatStorageBytes` in the dataset panel, and a private
// `formatBytes` in each of the admin resource panels, the lookup manager and the
// analytics console -- five implementations of "print a size", which disagreed
// with each other in ways nothing reported: two wrote `2.0 KiB` where the others
// wrote `2 KiB`, the lookup manager's stopped at MiB so two gibibytes read as
// `2048.0 MiB`, and the search workspace's divided by 1024 while labelling the
// result `KB`/`MB`/`GB`. A search job's `resultBytes` was rendered in binary
// units on one page and decimal units on another.
//
// Nothing in the toolchain sees that. Each copy typechecks, lints and renders;
// they are only wrong next to each other, and a reader has to be looking at two
// files at once to notice. So the ratchet is here, and it keys on the spelling
// every one of the five actually used: a top-level function whose name says it
// formats a byte count.
//
// It is a name check, and a determined sixth copy called `renderSize` would slip
// past it. That is the deliberate trade: the alternative is a shape check on
// unit ladders and divisions by 1024, which cannot tell the lookup manager's
// `${maximumDescriptionBytes / 1024} KiB` contract prose from a formatter, and
// would need an allowlist on its first run. A ratchet that fires on the shape
// the defect has actually taken, five times, is worth more than one that has to
// be argued with.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import { listRepositoryFiles, relativePosix } from "./style-inventory.mjs";

const workspace = process.cwd();

/** The module that owns the notation, and its own tests. */
const OWNER = "lib/byte-quantity.ts";

/**
 * The one formatter that lives elsewhere, and why.
 *
 * `formatDecimalBytes` renders a downloadable export artifact's size, and a file
 * is quoted in decimal units by the operating system that shows it next. It is a
 * second *unit system* for a genuinely different thing, not a second
 * implementation of this one, and the reason is written at the function. Its
 * callers are the export dialog and the export job list; a byte count that is
 * not a file uses `summarizeByteQuantity`.
 */
const DOCUMENTED_EXCEPTION = { file: "app/search-workspace/formatters.ts", name: "formatDecimalBytes" };

/**
 * A declaration whose name says it turns a byte count into text.
 *
 * The name has to mention a byte or a size: `formatNonNegativeIntegerQuantity`
 * beside `formatDecimalBytes` formats a row count, and a scan that swept it up
 * would be reporting a function that has nothing to do with this.
 */
const BYTE_FORMATTER =
  /^\s*(?:export\s+)?function\s+((?:format|summarize|describe|render|display)\w*(?:Byte\w*|Size))\s*\(/gmu;

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

test("only one module declares a byte formatter, and the one exception is the documented one", async () => {
  const sources = (await listRepositoryFiles(workspace))
    .map((file) => relativePosix(workspace, file))
    .filter((file) => /^(?:app|lib)\/.*\.tsx?$/u.test(file) && !/\.test\.tsx?$/u.test(file));
  assert.ok(sources.length > 40, `only ${sources.length} source files were discovered; the walk is broken`);

  const declared = (await Promise.all(sources.map(async (file) => {
    const source = await readFile(path.join(workspace, file), "utf8");
    return [...source.matchAll(BYTE_FORMATTER)].map((match) => ({ file, name: match[1] }));
  }))).flat();
  assert.ok(
    declared.some((entry) => entry.file === OWNER),
    `${OWNER} declares no byte formatter, so this scan is reading nothing and would stay green with`
      + " every copy back in the tree",
  );

  const strays = declared
    .filter((entry) => entry.file !== OWNER)
    .filter((entry) => !(entry.file === DOCUMENTED_EXCEPTION.file && entry.name === DOCUMENTED_EXCEPTION.name))
    .map((entry) => `${entry.file} declares ${entry.name}`)
    .toSorted();
  assert.deepEqual(
    strays,
    [],
    `A second way to turn a byte count into text. Five of these existed before ${OWNER}\n`
      + "folded them, and they disagreed about rounding, about where the unit ladder stopped, and\n"
      + "about whether a binary magnitude gets a binary name -- none of which any other gate can\n"
      + `see. Call \`summarizeByteQuantity\` for a displayed size, \`formatByteQuantity\` for a value\n`
      + `a form field holds, and \`describeByteQuantity\` for an exact count in prose:\n${describeList(strays)}`,
  );
});

test("the documented exception is still there, so the allowance is not describing a deleted function", async () => {
  // An exemption nobody can see the effect of is an exemption nobody reviews.
  const source = await readFile(path.join(workspace, DOCUMENTED_EXCEPTION.file), "utf8");
  assert.match(
    source,
    new RegExp(`function ${DOCUMENTED_EXCEPTION.name}\\s*\\(`, "u"),
    `${DOCUMENTED_EXCEPTION.file} no longer declares ${DOCUMENTED_EXCEPTION.name}, so the exception above`
      + " grandfathers nothing and should be deleted with it",
  );
});
