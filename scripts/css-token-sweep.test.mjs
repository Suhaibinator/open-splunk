// Invariants over the Phase 2 token sweep.
//
// The phase bar is that every colour, spacing, radius, stacking, type and
// elevation literal in the stylesheets and in the two TypeScript palettes now
// names a semantic role instead of a value. Nothing already in the suite can
// see whether that is true. `npm run test:visual` renders the pages and
// compares pixels at Playwright's default per-pixel tolerance, which is loose
// enough in YIQ space to call two different blues equal, so a green run proves
// no layout moved and says almost nothing about colour. `npm run lint:css`
// counts warnings, and a count is not an invariant: it falls when a rule is
// deleted exactly as it falls when a rule is migrated.
//
// These tests read the stylesheets structurally instead, through
// `scripts/css-token-sweep.mjs` -- a library rather than a test file, so this
// file never opens a stylesheet and never breaks the rule
// `scripts/css-invariants.test.mjs` enforces.
//
// Two of them are expected to fail on the tree that introduced them, and are
// written to stay failing. A test that documents a defect is worth more than a
// test that has been shaped around it.
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import {
  acceptedKinds,
  collectColourLiterals,
  collectEncodedColourLiterals,
  collectExactTokenMisses,
  collectPropertyKindMismatches,
  collectScaleLiterals,
  collectSeriesPalette,
  collectSeverityPalette,
  collectTokenValues,
  collectTypeScriptColourLiterals,
  collectTypeScriptTokenReferences,
  hasColourLiteral,
  isScaleLiteral,
  listApplicationStylesheets,
  normaliseHex,
  tokenKind,
  valueComponents,
  withoutImportant,
} from "./css-token-sweep.mjs";
import { relativePosix } from "./css-inventory.mjs";

const workspace = process.cwd();
const ledgerPath = path.join(workspace, "scripts", "css-literal-debt.json");

/**
 * The one colour a stylesheet cannot own.
 *
 * `app/layout.tsx` hands `themeColor` to the browser, which paints the address
 * bar with it before any stylesheet has loaded, so a `var()` there resolves to
 * nothing. The value is the literal behind `--chrome-bar`, and the module says
 * so beside it.
 */
const ALLOWED_TYPESCRIPT_COLOURS = ["app/layout.tsx: #1e252b"];

/**
 * The one colour no `var()` can reach.
 *
 * The select arrow is an SVG inside a `data:` URI, which the browser parses as
 * its own document: a custom property declared on this page is not in scope
 * there. docs/theming.md records it as needing a mask or an inline SVG rather
 * than a token.
 */
const ENCODED_COLOUR_SITE =
  "app/dashboards/operations-dashboard.css | .operations-range-picker select | background: %23526068";

/** The four levels the log data carries, each painted from one token. */
const SEVERITY_TOKENS = {
  debug: "--level-debug",
  error: "--level-error",
  info: "--level-info",
  warn: "--level-warn",
};

const SERIES_COUNT = 12;

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

async function readLedger() {
  return JSON.parse(await readFile(ledgerPath, "utf8"));
}

/**
 * Every stylesheet under `app/` outside the token layer, found by walking the
 * tree rather than by naming them.
 *
 * This used to list CSS modules, because a module was the only stylesheet a
 * feature could own. Phase 4 replaced the modules with plain colocated files,
 * so the question the assertion below asks is the same one -- does the sweep
 * see every stylesheet a feature ships? -- while the shape it walks for is the
 * shape the repository now has. A count would answer a different question: it
 * stays green when one file is added and another deleted in the same change,
 * and it goes red, for no reason a reader can act on, when a feature's rules
 * are folded into a primitive, which is the direction this codebase is
 * deliberately moving in.
 */
async function listApplicationCss() {
  const entries = await readdir(path.join(workspace, "app"), { recursive: true, withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".css"))
    .map((entry) => relativePosix(workspace, path.join(entry.parentPath, entry.name)))
    .filter((file) => !file.startsWith("app/styles/tokens-"))
    .toSorted();
}

test("the sweep reaches every stylesheet outside the token layer", async () => {
  const audited = (await listApplicationStylesheets(workspace))
    .map((file) => relativePosix(workspace, file))
    .toSorted();
  assert.ok(
    audited.includes("app/globals.css"),
    "the sweep no longer reaches app/globals.css, so every invariant below is vacuous",
  );
  const shipped = await listApplicationCss();
  assert.ok(shipped.length > 1, "only one stylesheet was found under app/, so the comparison below is vacuous");
  assert.deepEqual(
    audited.filter((file) => file.startsWith("app/")),
    shipped,
    `the sweep found ${audited.length} stylesheets and must see every one under app/: ${audited.join(", ")}`,
  );
  assert.ok(
    !audited.some((file) => file.startsWith("app/styles/tokens-")),
    "the token layer is inside the audit, which would let a primitive count as a violation",
  );
});

test("no colour literal exists outside the token layer beyond the recorded debt", async () => {
  const ledger = await readLedger();
  assert.deepEqual(
    await collectColourLiterals(workspace),
    ledger.colour,
    "The colour literals in the stylesheets and scripts/css-literal-debt.json disagree.\n"
      + "A literal the ledger does not carry is new debt: give it a semantic token, or -- if\n"
      + "tier 2 names no role for it -- record it in the ledger under the role gap that would\n"
      + "absorb it, which docs/theming.md tabulates. A ledger row with no literal left in the\n"
      + "tree is a migration that has already happened: delete the row.",
  );
});

test("no scale literal exists outside the token layer beyond the recorded debt", async () => {
  const ledger = await readLedger();
  assert.deepEqual(
    await collectScaleLiterals(workspace),
    ledger.scale,
    "The font-size, z-index, border-radius and box-shadow measurements in the stylesheets and\n"
      + "scripts/css-literal-debt.json disagree. Every step the scale layer names is in\n"
      + "app/styles/tokens-scale.css; a measurement it does not name is either an off-grid value\n"
      + "whose nearest step moves a pixel -- record it, docs/theming.md's Replaces tables say\n"
      + "which -- or a local stacking order the ladder deliberately leaves alone.",
  );
});

// This is the sharpest question the sweep can be asked and the only one whose
// answer needs no judgement: a literal that a token already spells exactly can
// be substituted with no rendered difference at all, so leaving it is a miss
// rather than a decision. It fails on the tree that introduced it.
test("no declaration keeps a literal that a token already spells exactly", async () => {
  const misses = (await collectExactTokenMisses(workspace)).map((miss) => (
    `${miss.file} | ${miss.selector} | ${miss.property}: ${miss.value}`
    + `   -- ${miss.wrote} is exactly var(${miss.token})`
  ));
  assert.deepEqual(
    misses,
    [],
    "These declarations write a number that a token resolves to exactly, so substituting the\n"
      + "token changes no pixel and the substitution was simply missed. Two shapes dominate: a\n"
      + "length inside a multi-value shorthand, which the sweep migrated in some rules\n"
      + "(`padding: var(--space-3) 10px`) and not in others (`padding: 8px 10px`); and a\n"
      + "declaration carrying `!important`, where matching the whole value never matched.\n"
      + `z-index is excluded here: docs/theming.md keeps local stacking ladders literal.\n${describeList(misses)}`,
  );
});

test("no new colour is hidden inside a percent-encoded data URI", async () => {
  assert.deepEqual(
    await collectEncodedColourLiterals(workspace),
    [ENCODED_COLOUR_SITE],
    "A colour written as `%23rrggbb` inside a `data:` URI is invisible to every other check\n"
      + "here, to `npm run lint:css`, and to a reader searching for `#`. It is also unreachable\n"
      + "by a token, because the SVG is a separate document: the fix is a mask or an inline SVG\n"
      + "whose fill a rule can set, not another literal. docs/theming.md records the one site\n"
      + "that exists.",
  );
});

test("no application module carries a colour literal outside the browser theme colour", async () => {
  assert.deepEqual(
    await collectTypeScriptColourLiterals(workspace),
    ALLOWED_TYPESCRIPT_COLOURS,
    "A colour written in TypeScript reaches the DOM through an inline style, where it outranks\n"
      + "every rule and no theme can move it. Use a var() string, as the chart palettes do.",
  );
});

test("every token TypeScript writes into the DOM is declared by the token layer", async () => {
  const { declared } = await collectTokenValues(workspace);
  const references = await collectTypeScriptTokenReferences(workspace);
  assert.ok(references.length >= SERIES_COUNT, `only ${references.length} var() strings were found in TypeScript`);
  const dangling = references
    .filter((reference) => !declared.has(reference.name))
    .map((reference) => `${reference.file}: ${reference.name}`)
    .toSorted();
  assert.deepEqual(
    dangling,
    [],
    "An inline style names a custom property the token layer does not declare. Unlike a\n"
      + "stylesheet, this fails silently: the element simply keeps whatever it inherited, and a\n"
      + `chart series renders in the body ink instead of its own hue.\n${describeList(dangling)}`,
  );
});

test("every token a declaration names holds the kind of value that property accepts", async () => {
  // Not a colour check: this is the failure a screenshot cannot show. A
  // `font-size` that names a colour, or a `color` that names a step, renders as
  // no declaration at all rather than as a wrong one.
  assert.equal(acceptedKinds("z-index").join(), "number");
  assert.equal(acceptedKinds("float"), null);
  const mismatches = await collectPropertyKindMismatches(workspace);
  assert.deepEqual(
    mismatches,
    [],
    "A declaration names a token whose value the property cannot use. The browser drops the\n"
      + `whole declaration, so the rule silently stops applying:\n${describeList(mismatches)}`,
  );
});

test("the event severity palette agrees between the stylesheet and the chart", async () => {
  const { stylesheet, typescript } = await collectSeverityPalette(workspace);
  assert.deepEqual(
    Object.fromEntries(Object.entries(stylesheet).map(([level, names]) => [level, names.join(", ")])),
    SEVERITY_TOKENS,
    "A rule paints a severity swatch from something other than that level's own token, or\n"
      + "paints it from two different tokens in two rules. The legend and the row marker are the\n"
      + "same datum and have to be the same colour.",
  );
  assert.deepEqual(
    typescript,
    SEVERITY_TOKENS,
    "categoryColor() in app/search-workspace/panels/visualization-panel.tsx maps the four log\n"
      + "levels onto different tokens from the ones the stylesheet paints the legend with, so a\n"
      + "chart and its own legend disagree. These are --level-* and never --status-*: a status\n"
      + "describes the outcome of a search, a level describes the data inside it.",
  );
});

test("the categorical ramp is the chart-series token family, in order", async () => {
  const { categoryCount, series } = await collectSeriesPalette(workspace);
  const expected = Array.from({ length: SERIES_COUNT }, (_, index) => `var(--chart-series-${index + 1})`);
  assert.deepEqual(
    series,
    expected,
    "TIME_SERIES_COLORS is the one array every chart assigns from. A member that is not its\n"
      + "own --chart-series-N is a hue the token layer cannot move, and an out-of-order member\n"
      + "silently renumbers every series in every chart.",
  );
  assert.equal(
    categoryCount,
    6,
    "CATEGORY_COLORS must stay a slice of TIME_SERIES_COLORS rather than a second array, so\n"
      + "the categorical chart and the time series cannot drift apart.",
  );
  const { declared } = await collectTokenValues(workspace);
  const undeclared = expected
    .map((reference) => /--[\w-]+/u.exec(reference)[0])
    .filter((name) => !declared.has(name));
  assert.deepEqual(undeclared, [], `the palette declares no ${undeclared.join(", ")}`);
  assert.ok(
    !declared.has(`--chart-series-${SERIES_COUNT + 1}`),
    "the palette declares a thirteenth series the ramp never assigns, so one hue is unreachable",
  );
});

// The invariants above are worth their runtime only if the value analysis under
// them can see a violation. What follows pins that analysis against the shapes
// that would quietly make each of them vacuous.

test("valueComponents splits at the top level and keeps a function whole", () => {
  assert.deepEqual(valueComponents("8px 10px"), ["8px", "10px"]);
  assert.deepEqual(valueComponents("clamp(32px, 4vw, 55px)"), ["clamp(32px, 4vw, 55px)"]);
  assert.deepEqual(
    valueComponents("0 3px 9px rgb(18 29 35 / 25%)"),
    ["0", "3px", "9px", "rgb(18 29 35 / 25%)"],
  );
  assert.deepEqual(valueComponents("var(--space-2) 10px"), ["var(--space-2)", "10px"]);
});

test("withoutImportant strips the flag and nothing else", () => {
  assert.equal(withoutImportant("10px !important"), "10px");
  assert.equal(withoutImportant("8px"), "8px");
  // The flag is what defeated a whole-value match during the sweep, so a value
  // that merely mentions the word must survive untouched.
  assert.equal(withoutImportant('"important" 4px'), '"important" 4px');
});

test("hasColourLiteral sees every literal shape and no identifier that merely reads like one", () => {
  assert.equal(hasColourLiteral("#fff"), true);
  assert.equal(hasColourLiteral("rgb(18 29 35 / 25%)"), true);
  assert.equal(hasColourLiteral("oklch(0.7 0.1 200)"), true);
  assert.equal(hasColourLiteral("white"), true);
  assert.equal(hasColourLiteral("var(--fg-text)"), false);
  assert.equal(hasColourLiteral("transparent"), false);
  assert.equal(hasColourLiteral("currentcolor"), false);
  // `white-space: nowrap` and a hue-named primitive are the two shapes a naive
  // search reports, and both would make the ledger unmaintainable.
  assert.equal(hasColourLiteral("nowrap"), false);
  assert.equal(hasColourLiteral("var(--purple-500)"), false);
});

test("tokenKind reads the value rather than the name", () => {
  assert.equal(tokenKind("#2878a8"), "colour");
  assert.equal(tokenKind("12px"), "length");
  assert.equal(tokenKind("200"), "number");
  assert.equal(tokenKind("100ms"), "duration");
  assert.equal(tokenKind("ease-out"), "easing");
  assert.equal(tokenKind("Arial, Helvetica, sans-serif"), "font-stack");
  assert.equal(tokenKind("0 3px 9px rgb(21 35 43 / 24%)"), "shadow");
  // A token renamed out of its family keeps the kind of what it resolves to,
  // which is the whole reason the pairing check survives a rename.
  assert.equal(tokenKind("999px"), "length");
});

test("isScaleLiteral counts measurements and skips migrations, zeroes and inks", () => {
  assert.equal(isScaleLiteral("13px"), true);
  assert.equal(isScaleLiteral("1000"), true);
  assert.equal(isScaleLiteral("clamp(32px, 4vw, 55px)"), true);
  assert.equal(isScaleLiteral("var(--radius-sm)"), false);
  assert.equal(isScaleLiteral("0"), false);
  assert.equal(isScaleLiteral("0px"), false);
  assert.equal(isScaleLiteral("inset"), false);
  assert.equal(isScaleLiteral("rgb(18 29 36 / 24%)"), false);
});

test("normaliseHex makes two spellings of one colour compare equal", () => {
  assert.equal(normaliseHex("#FFF"), "#ffffff");
  assert.equal(normaliseHex("#2878A8"), "#2878a8");
  assert.equal(normaliseHex("#15232b3d"), "#15232b3d");
});
