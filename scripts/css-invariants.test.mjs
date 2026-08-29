// Structural invariants for the stylesheet layer.
//
// Phase 0 of the CSS cleanup replaced assertions on stylesheet *text* with
// assertions on rendered behaviour, and deleted rules nothing referenced. These
// tests keep both properties true as the refactor continues: no test may go
// back to reading CSS characters, no `var()` may point at a property nothing
// declares, and no class rule may outlive the markup that used it.
//
// All reading and parsing lives in `scripts/css-inventory.mjs`, so this file --
// which is itself a test file -- never opens a stylesheet, and therefore never
// breaks the first invariant it asserts.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import {
  collectClassEvidence,
  collectCustomPropertyDefinitions,
  collectCustomPropertyUsage,
  collectGlobalStylesheetClasses,
  collectRuntimeCustomProperties,
  collectSourceClassEvidence,
  collectStylesheetClasses,
  cssBlockPreludes,
  findStylesheetTextReads,
  findTestStylesheetReads,
  isClassReachable,
  listStylesheets,
  listTestFiles,
  maskStringLiterals,
  relativePosix,
} from "./css-inventory.mjs";

const workspace = process.cwd();
const allowlistPath = path.join(workspace, "scripts", "css-dynamic-classes.json");

/** Reads the allowlist of classes that only ever exist at runtime. */
async function readDynamicClassAllowlist() {
  const parsed = JSON.parse(await readFile(allowlistPath, "utf8"));
  return { classes: new Set(parsed.classes ?? []), prefixes: new Set(parsed.prefixes ?? []) };
}

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

test("the stylesheet inventory reaches the whole styling layer", async () => {
  const stylesheets = (await listStylesheets(workspace)).map((file) => relativePosix(workspace, file));
  assert.ok(
    stylesheets.includes("app/styles/base.css") && stylesheets.includes("app/styles/primitives/button.css"),
    "the walker no longer reaches the base layer or the primitives, so every CSS invariant below is vacuous",
  );
  assert.ok(
    stylesheets.some((file) => file.endsWith(".module.css")),
    "the walker no longer reaches any CSS module, so module-scoped regressions are invisible",
  );
  const tests = (await listTestFiles(workspace)).map((file) => relativePosix(workspace, file));
  assert.ok(tests.length > 20, `the walker found only ${tests.length} test files; it is missing the suite`);
  assert.ok(
    tests.includes("integration/visual/css-contracts.spec.ts"),
    "the walker misses Playwright specs, so a stylesheet read there would go unnoticed",
  );
});

test("no test file reads stylesheet source text", async () => {
  const offenders = await findTestStylesheetReads(workspace);
  assert.deepEqual(
    offenders,
    [],
    "A test read a stylesheet's characters. Assert on rendered behaviour instead: computed-style\n"
      + "contracts live in integration/visual/css-contracts.spec.ts and appearance is pinned by\n"
      + `integration/visual/*.visual.spec.ts.\n${describeList(offenders)}`,
  );
});

test("every var() reference resolves to a declared custom property", async () => {
  const { declared, references, runtimeDeclared } = await collectCustomPropertyUsage(workspace);
  const dangling = [...references.keys()]
    .filter((name) => !declared.has(name) && !runtimeDeclared.has(name))
    .toSorted()
    .map((name) => `${name} read by ${references.get(name).join(", ")}`);
  assert.deepEqual(
    dangling,
    [],
    `A var() reference has no declaration in any stylesheet and no runtime writer:\n${describeList(dangling)}`,
  );
});

test("every class rule in the global stylesheets is reachable from the application", async () => {
  const classes = await collectGlobalStylesheetClasses(workspace);
  const evidence = await collectClassEvidence(workspace);
  const allowlist = await readDynamicClassAllowlist();
  for (const prefix of allowlist.prefixes) evidence.interpolationPrefixes.add(prefix);
  const unreachable = [...classes]
    .filter((className) => !allowlist.classes.has(className) && !isClassReachable(className, evidence))
    .toSorted();
  assert.deepEqual(
    unreachable,
    [],
    "A global stylesheet styles classes that no literal className, interpolation base, or :global()\n"
      + "selector can produce. Delete the rules, or -- only when a class really is built somewhere\n"
      + "this scan cannot see -- record it in scripts/css-dynamic-classes.json with a comment\n"
      + `naming the code that produces it:\n${describeList(unreachable)}`,
  );
});

test("the dynamic-class allowlist carries no stale entries", async () => {
  const allowlist = await readDynamicClassAllowlist();
  if (allowlist.classes.size === 0 && allowlist.prefixes.size === 0) return;
  const classes = await collectGlobalStylesheetClasses(workspace);
  const evidence = await collectClassEvidence(workspace);
  const stale = [];
  for (const className of [...allowlist.classes].toSorted()) {
    if (!classes.has(className)) stale.push(`${className} is not styled by any global stylesheet`);
    else if (isClassReachable(className, evidence)) {
      stale.push(`${className} is reachable from application code and needs no allowlist entry`);
    }
  }
  for (const prefix of [...allowlist.prefixes].toSorted()) {
    if (![...classes].some((className) => className.startsWith(prefix))) {
      stale.push(`${prefix} matches no class in any global stylesheet`);
    }
  }
  assert.deepEqual(
    stale,
    [],
    `scripts/css-dynamic-classes.json grandfathers entries that are no longer needed:\n${describeList(stale)}`,
  );
});

// The invariants above are only worth their runtime if the parsing underneath
// them can actually see a violation. What follows pins that parsing against the
// shapes that have already fooled a simpler implementation.

test("cssBlockPreludes separates selectors from declaration values", () => {
  const preludes = cssBlockPreludes(`
    @media (max-width: 760px) {
      .card { padding: 0.5rem; background-image: url(a.b.c); }
    }
    /* .commented-out { color: red; } */
    .after {}
  `);
  const selectors = preludes.filter((entry) => !entry.text.startsWith("@")).map((entry) => entry.text);
  assert.deepEqual(selectors, [".card", ".after"]);
});

test("collectStylesheetClasses ignores commented-out rules and value-position dots", () => {
  const classes = collectStylesheetClasses(`
    .live { margin: 1.5rem; }
    /* .retired { color: red; } */
    @media (max-width: 480px) { .narrow { gap: 0.25rem; } }
  `);
  assert.deepEqual([...classes].toSorted(), ["live", "narrow"]);
});

test("collectSourceClassEvidence survives escapes and nested templates", () => {
  const source = [
    String.raw`const quoted = "escaped \" quote";`,
    'const label = `count: ${items.filter((i) => i.tone === "warn").length} shown`;',
    'const outer = `panel panel--${mode === "wide" ? `wide-${size}` : "narrow"} done`;',
    'const plain = "late-literal";',
  ].join("\n");
  const evidence = collectSourceClassEvidence(source);
  // A regex-paired scanner mispairs quotes at the first escape and loses every
  // literal after it; the walker must still see the one closing the file.
  assert.ok(evidence.tokens.has("late-literal"));
  assert.ok(evidence.tokens.has("panel"));
  assert.ok(evidence.tokens.has("narrow"));
  assert.ok(evidence.tokens.has("done"));
  assert.ok(evidence.interpolationPrefixes.has("panel--"));
  assert.ok(evidence.interpolationPrefixes.has("wide-"));
});

test("collectSourceClassEvidence ignores class names inside comments", () => {
  const evidence = collectSourceClassEvidence([
    '// "comment-only-class"',
    '/* "block-comment-class" */',
    'const live = "live-class";',
  ].join("\n"));
  assert.deepEqual([...evidence.tokens].toSorted(), ["live-class"]);
});

test("isClassReachable needs a strictly longer name than the interpolation base", () => {
  const evidence = { interpolationPrefixes: new Set(["status-label--"]), tokens: new Set(["chip"]) };
  assert.equal(isClassReachable("chip", evidence), true);
  assert.equal(isClassReachable("status-label--error", evidence), true);
  assert.equal(isClassReachable("status-label--", evidence), false);
  assert.equal(isClassReachable("status-label", evidence), false);
});

test("maskStringLiterals leaves code callable and makes quoted source inert", () => {
  assert.equal(maskStringLiterals('readFileSync("a.css")'), "readFileSync(‹a.css›)");
  // The call's parentheses and inner quotes are gone, so no read-call pattern
  // can match inside a fixture; only the path characters survive.
  assert.equal(maskStringLiterals(String.raw`'readFileSync("a.css")'`), "‹readFileSync··a.css··›");
});

test("findStylesheetTextReads catches direct, bound, and imported stylesheet reads", () => {
  assert.deepEqual(
    findStylesheetTextReads('const css = readFileSync("app/styles/base.css", "utf8");'),
    ['readFileSync("app/styles/base.css")'],
  );
  assert.deepEqual(
    findStylesheetTextReads([
      'const sheet = path.join(root, "app", "styles", "base.css");',
      'const css = await readFile(sheet, "utf8");',
    ].join("\n")),
    ["readFile(sheet)"],
  );
  assert.deepEqual(findStylesheetTextReads('import "./app/styles/base.css";'), ['import "./app/styles/base.css"']);
});

test("findStylesheetTextReads ignores a read that only appears inside a fixture literal", () => {
  // This file's own parser tests embed stylesheet reads as data. Quoted source
  // is not a read, and treating it as one would make the invariant unfixable.
  const source = 'const fixture = \'const css = readFileSync("app/styles/base.css");\';';
  assert.deepEqual(findStylesheetTextReads(source), []);
});

test("findStylesheetTextReads does not flag handing a stylesheet path to the browser", () => {
  const source = [
    'const globalStylesheet = path.join(__dirname, "..", "..", "app", "styles", "base.css");',
    "await page.addStyleTag({ path: globalStylesheet });",
  ].join("\n");
  assert.deepEqual(findStylesheetTextReads(source), []);
});

test("collectCustomPropertyDefinitions separates declarations from var() fallbacks", () => {
  const declared = collectCustomPropertyDefinitions(`
    :root { --blue: #2878a8; }
    @property --angle { syntax: "<angle>"; }
    .card { color: var(--blue, --never-declared); }
  `);
  assert.deepEqual([...declared].toSorted(), ["--angle", "--blue"]);
});

test("collectRuntimeCustomProperties sees inline style objects and setProperty", () => {
  const runtime = collectRuntimeCustomProperties([
    'element.style.setProperty("--bar-height", `${height}px`);',
    'const style = { "--point-x": x, "--point-y": y };',
  ].join("\n"));
  assert.deepEqual([...runtime].toSorted(), ["--bar-height", "--point-x", "--point-y"]);
});
