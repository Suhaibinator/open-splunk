// The markup-to-CSS direction: does every class the product asks for still exist?
//
// `scripts/css-invariants.test.mjs` asks the opposite question -- does every
// rule still have a caller -- and that direction cannot see a deletion. When a
// rule goes and the markup does not, the class simply stops matching: no build
// error, no runtime error, no lint. The element renders with default styling,
// and a screenshot only notices if a baseline happens to cover that state.
// Phase 3 deleted sixty-six global classes, so this file walks back the other
// way, from every call site to the styling layer.
//
// Reading and parsing live in `scripts/css-inventory.mjs`, so nothing here
// opens a stylesheet and the "no test reads stylesheet text" invariant stays
// true of this file too.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import {
  collectClassAttributeTokens,
  collectImportSpecifiers,
  collectSelectorClassTokens,
  collectSourceClassEvidence,
  collectStyledClasses,
  collectStylesheetClasses,
  listSourceFiles,
  relativePosix,
  stripCssComments,
} from "./css-inventory.mjs";

const workspace = process.cwd();
const retiredPath = path.join(workspace, "scripts", "css-retired-classes.json");
const verticalSpecPath = path.join(workspace, "integration", "browser_vertical.spec.ts");

/**
 * The one module every dialog is expected to come from.
 *
 * Phase 3 moved `Modal` out of `app/search-workspace/` and next to the
 * `modal-surface` helper it installs. A second copy left behind anywhere would
 * still compile and still render, and would quietly stop receiving the focus,
 * inert and scroll-lock behaviour that lives in the moved one.
 */
const MODAL_MODULE = "app/_components/modal";

/** The one shape a component imports a CSS module by: a default binding. */
const MODULE_IMPORT = /import\s+(\w+)\s+from\s+"([^"]*\.module\.css)"/u;

/** This file, which quotes retired names and fake imports as parser fixtures. */
const SELF = "scripts/css-call-sites.test.mjs";

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

/** Every source file bar this one, as `{ directory, relative, source }`. */
async function callSites() {
  const files = (await listSourceFiles(workspace))
    .map((file) => ({ file, relative: relativePosix(workspace, file) }))
    .filter((entry) => entry.relative !== SELF);
  return Promise.all(files.map(async ({ file, relative }) => ({
    directory: path.dirname(file),
    relative,
    source: await readFile(file, "utf8"),
  })));
}

/** Reads the retired-class record: name -> the primitive that replaced it. */
async function readRetiredClasses() {
  const parsed = JSON.parse(await readFile(retiredPath, "utf8"));
  return new Map(Object.entries(parsed.classes ?? {}));
}

test("the record of retired classes and the walk over call sites are both populated", async () => {
  // Every invariant below compares one list against another, and both go quiet
  // when either side is empty. This is the assertion that says they are not.
  const retired = await readRetiredClasses();
  assert.ok(retired.size > 50, `only ${retired.size} retired classes are recorded; the record is not the phase's`);
  for (const [name, replacement] of retired) {
    assert.ok(
      typeof replacement === "string" && replacement.trim().length > 0,
      `${name} is recorded as retired without naming what replaced it`,
    );
  }
  const sites = await callSites();
  assert.ok(sites.length > 100, `the walk found only ${sites.length} source files; it is missing the application`);
  assert.ok(
    sites.some((entry) => entry.relative === "app/_components/product-shell.tsx"),
    "the walk does not reach app/_components, so the chrome's own call sites are invisible",
  );
});

test("nothing writes a retired class into a class attribute", async () => {
  const retired = await readRetiredClasses();
  const offenders = [];
  for (const { relative, source } of await callSites()) {
    const tokens = collectClassAttributeTokens(source);
    for (const name of [...tokens].toSorted()) {
      if (!retired.has(name)) continue;
      offenders.push(`${relative} renders class "${name}"; use ${retired.get(name)}`);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    "Markup asks for a class the consolidation deleted. Nothing in the toolchain reports an\n"
      + "unmatched class, so the element renders with no styling at all until somebody looks at\n"
      + `it:\n${describeList(offenders)}`,
  );
});

test("nothing builds a retired class through a helper or an interpolation", async () => {
  // A class attribute is not the only way a name reaches the DOM: helpers such
  // as `statusClassName` and `buttonClassName` return one from a literal, and
  // `` `status-label--${tone}` `` builds one from a base. Only hyphenated names
  // are checked here, because every class in this codebase is kebab-case with a
  // feature prefix, while the retired bare modifiers (`primary`, `compact`,
  // `cancel`) are also ordinary API values and route names. Those are covered
  // by the class-attribute scan above, which cannot confuse the two.
  const retired = [...(await readRetiredClasses()).keys()].filter((name) => name.includes("-"));
  const offenders = [];
  for (const { relative, source } of await callSites()) {
    // A test quotes retired names on purpose -- the parser fixtures in
    // css-invariants.test.mjs do -- so a fixture is not a call site. The
    // class-attribute scan above still covers fixture *markup*, which is the
    // shape that renders.
    if (/\.(?:test|spec)\.[jt]sx?$|\.test\.mjs$/u.test(relative)) continue;
    const evidence = collectSourceClassEvidence(source);
    for (const name of retired) {
      if (evidence.tokens.has(name)) offenders.push(`${relative} names "${name}"`);
      else if (evidence.interpolationPrefixes.has(`${name}--`)) {
        offenders.push(`${relative} builds "${name}--…" from an interpolation`);
      }
    }
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "A helper or a template literal still produces a class the consolidation deleted. Point it at\n"
      + `the replacement named in scripts/css-retired-classes.json:\n${describeList(offenders.toSorted())}`,
  );
});

test("no retired class has come back to the stylesheets under its old name", async () => {
  const retired = await readRetiredClasses();
  const styled = await collectStyledClasses(workspace);
  const returned = [...retired.keys()].filter((name) => styled.has(name)).toSorted();
  assert.deepEqual(
    returned,
    [],
    "scripts/css-retired-classes.json lists a class the styling layer defines again. Either the\n"
      + "consolidation was undone -- in which case there are two primitives once more -- or the entry\n"
      + `is stale and the record has stopped describing the stylesheets:\n${describeList(returned)}`,
  );
});

test("every class the Go browser harness selects on still exists", async () => {
  // integration/browser_vertical.spec.ts is driven by the Go end-to-end
  // harness against a compiled build, so a class it can no longer find surfaces
  // as a timeout minutes into a run, far from the rename that caused it.
  const styled = await collectStyledClasses(workspace);
  const selected = collectSelectorClassTokens(await readFile(verticalSpecPath, "utf8"));
  assert.ok(
    selected.size > 10,
    `only ${selected.size} class selectors were parsed out of the harness spec; the scan is broken`,
  );
  const missing = [...selected]
    .filter(([name]) => !styled.has(name))
    .map(([name, lines]) => `.${name} at integration/browser_vertical.spec.ts:${lines.join(", :")}`)
    .toSorted();
  assert.deepEqual(
    missing,
    [],
    "The harness spec drives the product through a class the styling layer no longer defines. Do\n"
      + "not loosen the selector: it is pinning a real structural contract. Update it to the class\n"
      + `that replaced the old one:\n${describeList(missing)}`,
  );
});

test("every styles.* read names a class its CSS module defines", async () => {
  // A CSS module resolves to a plain object, so a read of a class the module no
  // longer has yields `undefined` and the string "undefined" lands in the class
  // attribute. Phase 3 deleted module rules -- two `.previewBadge` copies among
  // them -- which is exactly when this goes wrong.
  const consumers = (await callSites()).filter((entry) => MODULE_IMPORT.test(entry.source));
  const offenders = (await Promise.all(consumers.map(async ({ directory, relative, source }) => {
    const [, binding, specifier] = MODULE_IMPORT.exec(source);
    const defined = collectStylesheetClasses(await readFile(path.resolve(directory, specifier), "utf8"));
    const read = new Set();
    for (const match of source.matchAll(new RegExp(String.raw`\b${binding}\.([A-Za-z_]\w*)`, "gu"))) {
      read.add(match[1]);
    }
    for (const match of source.matchAll(new RegExp(String.raw`\b${binding}\[\s*"([^"]*)"`, "gu"))) {
      read.add(match[1]);
    }
    return [...read]
      .filter((name) => !defined.has(name))
      .toSorted()
      .map((name) => `${relative} reads ${binding}.${name}, which ${specifier} does not define`);
  }))).flat();
  assert.deepEqual(
    offenders,
    [],
    "A component reads a class name its CSS module no longer has. The read is not a type error --\n"
      + "a module is typed as a plain record -- so the literal string \"undefined\" is what reaches the\n"
      + `class attribute:\n${describeList(offenders)}`,
  );
});

test("every Modal import resolves to the one component module", async () => {
  const offenders = [];
  for (const { directory, relative, source } of await callSites()) {
    if (!/\bimport\s*\{[^}]*\bModal\b[^}]*\}/u.test(source)) continue;
    for (const specifier of collectImportSpecifiers(source)) {
      if (!/(?:^|\/)modal$/u.test(specifier)) continue;
      const resolved = relativePosix(workspace, path.resolve(directory, specifier));
      if (resolved !== MODAL_MODULE) offenders.push(`${relative} imports Modal from ${specifier} (${resolved})`);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    `Modal lives at ${MODAL_MODULE}, beside the modal-surface helper it installs. A second copy\n`
      + "compiles and renders, and silently does without the focus trap, the inert siblings and the\n"
      + `scroll lock:\n${describeList(offenders)}`,
  );
});

// The invariants above are only as good as the parsing under them. What follows
// pins that parsing against the shapes that make a simpler implementation
// either miss a call site or invent one.

test("collectClassAttributeTokens reads both attribute spellings and expression forms", () => {
  const tokens = collectClassAttributeTokens([
    '<div className="one two" />',
    '<div class="three" />',
    '<div className={wide ? "four" : "five"} />',
    "<div className={`six seven--${tone}`} />",
  ].join("\n"));
  assert.deepEqual([...tokens].toSorted(), ["five", "four", "one", "seven--", "six", "three", "two"]);
});

test("collectClassAttributeTokens ignores words that are API values rather than classes", () => {
  // `primary`, `danger` and `compact` are all retired classes *and* live prop
  // values. A scan that read every literal would report the second as the first
  // and make the invariant unfixable.
  const tokens = collectClassAttributeTokens([
    '<Button variant="danger" size="compact">Cancel</Button>',
    'const route = "cancel";',
    '<div className="button button--primary" />',
  ].join("\n"));
  assert.deepEqual([...tokens].toSorted(), ["button", "button--primary"]);
});

test("collectClassAttributeTokens is not fooled by a name ending in class", () => {
  const tokens = collectClassAttributeTokens('const toneClass = "not-an-attribute";\n<i className="real" />');
  assert.deepEqual([...tokens].toSorted(), ["real"]);
});

test("collectSelectorClassTokens reads selector calls and nothing else", () => {
  const found = collectSelectorClassTokens([
    'const rows = page.locator("tbody tr:not(.spacer)");',
    'element.querySelector(".modal-footer button");',
    "const width = payload.value.length;",
    'const label = element.closest(".card")?.textContent;',
  ].join("\n"));
  assert.deepEqual([...found.keys()].toSorted(), ["card", "modal-footer", "spacer"]);
  assert.deepEqual(found.get("modal-footer"), [2]);
});

test("collectSelectorClassTokens reports every line a class is selected at", () => {
  const found = collectSelectorClassTokens([
    'page.locator(".row");',
    "// nothing here",
    'page.locator(".row .cell");',
  ].join("\n"));
  assert.deepEqual(found.get("row"), [1, 3]);
  assert.deepEqual(found.get("cell"), [3]);
});

test("collectImportSpecifiers separates the module path from the imported names", () => {
  assert.deepEqual(
    collectImportSpecifiers([
      'import { Modal } from "../../_components/modal";',
      'import styles from "./reports.module.css";',
      'const text = \'import { Modal } from "./fake";\';',
    ].join("\n")),
    ["../../_components/modal", "./reports.module.css"],
  );
});

test("stripCssComments and collectStyledClasses agree on what a module contributes", async () => {
  // A module's own class is scoped to a generated identifier and can never
  // collide with a global one; only its `:global(...)` selectors name the
  // global namespace. Getting this backwards would make every global class look
  // defined and the retired-class invariant vacuous.
  const styled = await collectStyledClasses(workspace);
  assert.ok(styled.has("table"), "the global .table primitive was not collected");
  assert.ok(!styled.has("mobileCardTable"), "a module's own class leaked into the global namespace");
  assert.equal(stripCssComments("/* .ghost { color: red; } */ .live {}").includes("ghost"), false);
});
