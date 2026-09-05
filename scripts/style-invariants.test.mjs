// Every structural invariant the CSS cleanup leaves behind, in one suite.
//
// The styling cleanup left a set of assertions that keep its active contracts
// from regressing: reachability, the token layer, the naming grammar, the
// literal sweep, the one-of-each-primitive fold, and the ordered stylesheet
// entry point. They live in this suite and read through one inventory library.
//
// What every assertion here has in common is that nothing else in the toolchain
// can see it. CSS reports no duplicate rule, no unmatched class, no dangling
// `var()` and no missing keyframe block; a screenshot renders one page at one
// width in one theme and is identical whether a rule won the cascade or was
// never needed; and `npm run lint:css` counts violations, which falls when a
// rule is deleted exactly as it falls when a rule is migrated. These are the
// checks that make the cleanup a ratchet rather than a moment.
//
// The reading and the parsing live in `scripts/style-inventory.mjs`, so this
// file -- which is itself a test file -- never opens a stylesheet, and therefore
// never breaks the invariant it asserts about test files. Its last section pins that
// library's parsers against the shapes that have already fooled a simpler
// implementation, because an invariant is worth exactly as much as the parser
// underneath it can see.
//
// The sections run: the reach that stops everything below passing by having
// nothing to look at; the token layer and its grammar; the literal sweep; the
// stylesheet set and its cascade order; where a responsive rule lives; one
// implementation of each primitive; reachability in both directions; and the
// parsers.
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import {
  STYLESHEET_ENTRY_POINT,
  acceptedKinds,
  collectAnimationReferences,
  collectBaseRuleSites,
  collectClassAttributeTokens,
  collectClassEvidence,
  collectColourLiterals,
  collectCustomPropertyDefinitions,
  collectCustomPropertyUsage,
  collectDeclarationBlocks,
  collectDeclarationComments,
  collectEncodedColourLiterals,
  collectEntryPointContent,
  collectExactTokenMisses,
  collectGlobalStylesheetClasses,
  collectImportSpecifiers,
  collectKeyframeSites,
  collectMediaQueryRuns,
  collectModuleLaneReferences,
  collectPrimitiveReferences,
  collectPropertyKindMismatches,
  collectResponsiveOwnership,
  collectRuntimeCustomProperties,
  collectScaleLiterals,
  collectScopeEscapes,
  collectSelectorClassTokens,
  collectSeriesPalette,
  collectSeverityPalette,
  collectSourceClassEvidence,
  collectStyledClasses,
  collectStylesheetClasses,
  collectStylesheetImportSites,
  collectTestStylesheetReads,
  collectTokenBlocks,
  collectTokenComments,
  collectTokenLayer,
  collectTokenValues,
  collectTypeScriptColourLiterals,
  collectTypeScriptTokenReferences,
  cssBlockPreludes,
  cssBlocks,
  cssDeclarations,
  declarationSignature,
  describeRuleSite,
  findComposedStylesheetReads,
  findStylesheetTextReads,
  findTestStylesheetReads,
  hasColourLiteral,
  isClassReachable,
  isDarkThemeContext,
  isScaleLiteral,
  listApplicationStylesheets,
  listHarnessStylesheets,
  listIndexImports,
  listInjectedStylesheets,
  listNonTokenStylesheets,
  listSourceFiles,
  listStylesheets,
  listTestFiles,
  listTokenStylesheets,
  maskStringLiterals,
  mediaQueryRank,
  normaliseHex,
  readHarnessExportExpression,
  relativePosix,
  stripCssComments,
  themeScopeOf,
  tokenBlocksOfSource,
  tokenCascadeOrder,
  tokenKind,
  tokenLayerOfSource,
  universalSpacingSteps,
  valueComponents,
  withoutImportant,
} from "./style-inventory.mjs";

const workspace = process.cwd();

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

/* == The palette axis ========================================================== */

/**
 * The palette that is the base pair itself.
 *
 * `data-palette="classic"` selects nothing: the base light and base dark blocks
 * of `tokens-color.css` and `tokens-scale.css` are what classic renders, so it
 * is the one palette name without a `tokens-palette-<name>.css` file.
 */
const DEFAULT_PALETTE = "classic";

/** The module that owns the palette vocabulary, and the array literal it exports. */
const PALETTE_MODULE = "lib/palettes.ts";
const PALETTES_LITERAL = /export\s+const\s+PALETTES\b[^=]*=\s*\[([^\]]*)\]/u;

/**
 * Reads the palette names from `lib/palettes.ts`'s `PALETTES` array
 * literal, the one place the client spells them.
 *
 * Read by regex rather than imported: this suite runs under `node --test`
 * without a TypeScript loader, and the array is a literal precisely so it can
 * be read this way. A module that no longer exports the literal reads as the
 * default palette alone -- the shape the repository had before palettes
 * existed, and the shape in which no palette file may exist -- rather than as
 * an error, so this suite stays green on a tree with no palettes while still
 * failing the moment a palette file appears without a name behind it.
 */
async function readPaletteNames() {
  return parsePaletteNames(await readFile(path.join(workspace, ...PALETTE_MODULE.split("/")), "utf8"));
}

/** `readPaletteNames` over the module's source text. */
function parsePaletteNames(source) {
  const literal = PALETTES_LITERAL.exec(source);
  if (literal === null) return [DEFAULT_PALETTE];
  return [...literal[1].matchAll(/"([a-z][a-z0-9-]*)"/gu)].map((match) => match[1]);
}

/** The object literal in `lib/palettes.ts` that names the palettes promising more than AA. */
const CONTRAST_FLOOR_LITERAL = /export\s+const\s+PALETTE_CONTRAST_FLOOR\b[^=]*=\s*\{([^}]*)\}/u;

/**
 * Reads the contrast floors from `lib/palettes.ts`'s `PALETTE_CONTRAST_FLOOR`
 * literal, the one place a palette's promise is spelled: the computed-style
 * contracts import the same table, so a palette held to 7:1 on the hex here
 * is held to 7:1 on the page too. Read by regex for the same reason
 * `PALETTES` is; a module without the literal promises AA everywhere.
 */
async function readContrastFloors() {
  return parseContrastFloors(await readFile(path.join(workspace, ...PALETTE_MODULE.split("/")), "utf8"));
}

/** `readContrastFloors` over the module's source text: `{ palette: floor }`, with no prototype. */
function parseContrastFloors(source) {
  const literal = CONTRAST_FLOOR_LITERAL.exec(source);
  const floors = Object.create(null);
  if (literal === null) return floors;
  for (const [, name, floor] of literal[1].matchAll(/(?:^|[\s{,])([a-z][a-z0-9-]*)\s*:\s*(\d+(?:\.\d+)?)/gu)) {
    floors[name] = Number(floor);
  }
  return floors;
}

/** The token file a palette's blocks live in. */
function paletteFile(name) {
  return `app/styles/tokens-palette-${name}.css`;
}

/** The palette a token file belongs to, or `null` for the base pair. */
function paletteOfFile(file) {
  return /^app\/styles\/tokens-palette-([a-z][a-z0-9-]*)\.css$/u.exec(file)?.[1] ?? null;
}

/** A theme scope as a reader would name it: `classic dark`, `ocean light`. */
function scopeLabel(palette, mode) {
  return `${palette ?? DEFAULT_PALETTE} ${mode}`;
}


/* == 1. Reach: nothing below may pass by having nothing to look at ============= */

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

test("the inventory reaches every stylesheet, test file and call site the suite reads", async () => {
  // Every assertion in this file compares one list against another, and both go
  // quiet when either side is empty. This is the one test that says they are
  // not. It is gathered here rather than repeated once per section because a
  // walker that stops seeing the layer makes the whole file vacuous, not one
  // concern of it, and five separate "is the walk populated" tests were five
  // chances to leave one of them behind.
  const stylesheets = (await listStylesheets(workspace)).map((file) => relativePosix(workspace, file));
  assert.ok(
    stylesheets.includes("app/styles/base.css") && stylesheets.includes("app/styles/primitives/button.css"),
    "the walker no longer reaches the base layer or the primitives, so every CSS invariant is vacuous",
  );
  // A feature's rules are plain CSS colocated with the feature. The walker has
  // to reach those files, or every invariant below stops covering the majority
  // of the product's rules.
  const colocated = stylesheets.filter((file) => (
    file.startsWith("app/") && !file.startsWith("app/styles/")
  ));
  assert.ok(
    colocated.length > 1,
    `the walker reaches only ${colocated.length} colocated feature stylesheets, so feature regressions are invisible`,
  );
  const tests = (await listTestFiles(workspace)).map((file) => relativePosix(workspace, file));
  assert.ok(tests.length > 20, `the walker found only ${tests.length} test files; it is missing the suite`);
  assert.ok(
    tests.includes("integration/style-contracts/css-contracts.spec.ts"),
    "the walker misses Playwright specs, so a stylesheet read there would go unnoticed",
  );

  const layer = await collectTokenLayer(workspace);
  const tokenFiles = layer.map((entry) => entry.file);
  assert.ok(
    tokenFiles.includes("app/styles/tokens-scale.css"),
    `the walker no longer reaches the scale tokens, so every token assertion is vacuous: ${tokenFiles.join(", ")}`,
  );
  assert.ok(
    tokenFiles.includes("app/styles/tokens-color.css"),
    `the walker no longer reaches the colour tokens: ${tokenFiles.join(", ")}`,
  );
  const scale = layer.find((entry) => entry.file === "app/styles/tokens-scale.css");
  const scaleTokens = scale.blocks.reduce((total, block) => total + block.tokens.length, 0);
  assert.ok(
    scaleTokens > 20,
    `app/styles/tokens-scale.css declares only ${scaleTokens} tokens; the scales are missing`,
  );
  const colour = layer.find((entry) => entry.file === "app/styles/tokens-color.css");
  assert.deepEqual(
    colour.blocks.map((block) => scopeLabel(block.palette, block.mode)),
    ["classic light", "classic dark"],
    "app/styles/tokens-color.css no longer parses as one base light block followed by one base dark\n"
      + "block, so every per-scope assertion below is reading the wrong blocks or none:\n"
      + describeList(colour.blocks.map((block) => `${block.selector} -> ${scopeLabel(block.palette, block.mode)}`)),
  );

  const audited = (await listNonTokenStylesheets(workspace))
    .map((file) => relativePosix(workspace, file))
    .toSorted();
  const shipped = await listApplicationCss();
  assert.ok(shipped.length > 1, "only one stylesheet was found under app/, so the sweep comparison is vacuous");
  assert.deepEqual(
    audited.filter((file) => file.startsWith("app/")),
    shipped,
    `the sweep found ${audited.length} stylesheets and must see every one under app/: ${audited.join(", ")}`,
  );
  assert.ok(
    !audited.some((file) => file.startsWith("app/styles/tokens-")),
    "the token layer is inside the sweep, which would let a primitive count as a violation",
  );

  const imports = await listIndexImports(workspace);
  assert.ok(
    imports.length > 20,
    `${STYLESHEET_ENTRY_POINT} states only ${imports.length} imports; the walker is reading the wrong file`
      + " and every import-order assertion is vacuous",
  );
  const application = await listApplicationStylesheets(workspace);
  assert.ok(
    application.length > 20,
    `the walker found only ${application.length} application stylesheets; it is missing the layer`,
  );

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

/* == 2. The token layer ======================================================== */

/** A colour written as characters rather than as a reference to a token. */
const COLOUR_LITERAL = /#[0-9a-f]{3,8}\b|\b(?:rgba?|hsla?|oklch|lab)\(/iu;

/**
 * A value that is a colour and nothing else: a hex, or one colour function,
 * in any spelling the token-file stylelint exemption lets through. A shadow
 * carrying an `rgb()` ink is not one -- the scale tier writes those, and a
 * palette may restate a shadow -- which is why this is a whole-value match
 * rather than `COLOUR_LITERAL`.
 */
const WHOLE_COLOUR_LITERAL = /^(?:#[0-9a-f]{3,8}|(?:rgba?|hsla?|oklch|oklab|lab|lch|color)\([^()]*\))$/iu;

/** Names a value reads through `var()`. */
function referencedTokens(value) {
  return [...value.matchAll(/var\(\s*(--[\w-]+)/gu)].map((match) => match[1]);
}

/** Every `{ file, block, token }` triple of a `collectTokenLayer` result, in source order. */
function tokenDeclarationsOf(layer) {
  return layer.flatMap((entry) => entry.blocks.flatMap((block) => (
    block.tokens.map((token) => ({ block, file: entry.file, token }))
  )));
}

/** Every `{ file, block, token }` triple in the token layer, in source order. */
async function tokenDeclarations() {
  return tokenDeclarationsOf(await collectTokenLayer(workspace));
}

/** True for the one block whose declarations introduce a name: the base light block. */
function isBaseLight(block) {
  return block.palette === null && block.mode === "light";
}

/**
 * Every name declared twice in one theme scope, and every colour literal
 * outside the base light block, for a `collectTokenLayer` result.
 *
 * One declaration per name per scope: the base light block introduces a
 * name, and each of the other three block shapes may restate it once. A
 * second declaration in the same scope -- the same block, or the base light
 * block of a second file -- is the shape where file order decides the value.
 * A literal is allowed in the base light block and nowhere else: a palette
 * that wrote a hex, or an `rgb()` the token-file stylelint exemption lets
 * through, would be a tier-1 primitive living outside the ladder the
 * primitives tests read, invisible to every hue check and to the cascade order
 * `tokenCascadeOrder` states.
 */
function declarationSiteProblems(layer) {
  const sites = new Map();
  const literals = [];
  for (const { block, file, token } of tokenDeclarationsOf(layer)) {
    const key = `${scopeLabel(block.palette, block.mode)} ${token.name}`;
    sites.set(key, [...(sites.get(key) ?? []), `${file} (${block.selector})`]);
    if (WHOLE_COLOUR_LITERAL.test(token.value) && !isBaseLight(block)) {
      literals.push(`${file} (${block.selector}): ${token.name}: ${token.value}`);
    }
  }
  const duplicated = [...sites.entries()]
    .filter(([, places]) => places.length > 1)
    .map(([key, places]) => `${key} declared in ${places.join(" and ")}`)
    .toSorted();
  return { duplicated, literals: literals.toSorted() };
}

/** Every name a block other than base light declares without base light declaring it first. */
function inventedNames(layer) {
  const declarations = tokenDeclarationsOf(layer);
  const introduced = new Set(
    declarations.filter(({ block }) => isBaseLight(block)).map(({ token }) => token.name),
  );
  return declarations
    .filter(({ block, token }) => !isBaseLight(block) && !introduced.has(token.name))
    .map(({ block, file, token }) => `${file}: ${token.name} (${block.selector})`)
    .toSorted();
}

test("no token name is declared in more than one place within a scope", async () => {
  const { duplicated, literals } = declarationSiteProblems(await collectTokenLayer(workspace));
  assert.deepEqual(
    duplicated,
    [],
    "A token has two declaration sites in one theme scope, so which value wins depends on file order\n"
      + `rather than on intent. Keep one declaration per name per scope:\n${describeList(duplicated)}`,
  );
  assert.deepEqual(
    literals,
    [],
    "A theme block holds a colour literal. Only the base light block of app/styles/tokens-color.css\n"
      + "declares primitives; a palette that needs a new hue step adds it there, on the 0-950 ladder,\n"
      + `and points its semantic roles at it:\n${describeList(literals)}`,
  );
});

test("every token reference inside the layer resolves within the layer", async () => {
  const declarations = await tokenDeclarations();
  const declared = new Set(declarations.map(({ token }) => token.name));
  const dangling = [];
  for (const { file, token } of declarations) {
    for (const reference of referencedTokens(token.value)) {
      if (!declared.has(reference)) dangling.push(`${file}: ${token.name} reads ${reference}`);
    }
  }
  assert.deepEqual(
    dangling.toSorted(),
    [],
    "A token reads a name the token layer does not declare, so the layer is not self-contained and\n"
      + `the value silently resolves to nothing:\n${describeList(dangling.toSorted())}`,
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

test("a token that names another token carries no colour literal of its own", async () => {
  const mixed = [];
  for (const { file, token } of await tokenDeclarations()) {
    if (referencedTokens(token.value).length === 0) continue;
    if (COLOUR_LITERAL.test(token.value)) mixed.push(`${file}: ${token.name}: ${token.value}`);
  }
  assert.deepEqual(
    mixed.toSorted(),
    [],
    "A semantic token restates a colour instead of naming one. Every colour a tier-2 token uses has\n"
      + "to come from a tier-1 primitive, or retheming stops being a one-file edit:\n"
      + `${describeList(mixed.toSorted())}`,
  );
});

test("every theme block redefines only names the base light block declares", async () => {
  const invented = inventedNames(await collectTokenLayer(workspace));
  assert.deepEqual(
    invented,
    [],
    "A theme block declares a token the base light block does not, so that name is undefined for every\n"
      + "reader on the default theme, and a palette that introduced it would be the one file a retheme\n"
      + `cannot see. Declare the name in the base light block first:\n${describeList(invented)}`,
  );
});

test("nothing outside the token layer reads a primitive", async () => {
  const leaks = (await collectPrimitiveReferences(workspace))
    .map(({ file, name }) => `${file} reads ${name}`)
    .toSorted();
  assert.deepEqual(
    [...new Set(leaks)],
    [],
    "docs/theming.md: nothing outside the token layer may reference a primitive. A rule that names a\n"
      + "step has hard-coded a hue into a component: no theme block or palette file can move it, and\n"
      + "no screenshot can tell it apart from the semantic token beside it. Point it at a tier-2 token,\n"
      + `or add one if no role fits:\n${describeList([...new Set(leaks)])}`,
  );
});

test("no primitive hue family shares its name with a palette", async () => {
  // `--ocean-500` would read as "the ocean palette's step 500" to anyone who
  // knows the palette exists, and a hue named after a palette is a hue no
  // other palette can borrow without the name lying about who owns it. Names
  // are the whole interface between tier 1 and every palette file, so the two
  // vocabularies stay disjoint.
  const palettes = new Set(await readPaletteNames());
  const { primitives } = await readTokenLayer();
  const clashes = [...new Set([...primitives.keys()].map((name) => family(name)))]
    .filter((hue) => palettes.has(hue))
    .map((hue) => `--${hue}-* is a hue family and ${hue} is a palette`)
    .toSorted();
  assert.deepEqual(
    clashes,
    [],
    "A tier-1 hue family carries a palette's name. Hue families are named for the colour, palettes for\n"
      + `the look, and a name on both lists means every reader has to ask which:\n${describeList(clashes)}`,
  );
});

/**
 * Every custom property a stylesheet outside the token layer is allowed to
 * declare, as `file: --name`.
 *
 * A feature sheet declaring a name is not automatically a token escaping the
 * layer: both entries below are a *component interface*, declared on the
 * component's own container to size a child the sheet deliberately does not
 * select into. What the layer forbids is a theme value -- a colour, a space
 * step, a radius -- growing a second declaration site out here, where no
 * `tokens-*.css` edit can reach it. The ledger is the difference: a knob has to
 * be added to this list on purpose, with a reviewer reading the name.
 */
const COMPONENT_KNOBS = [
  "app/dashboards/operations-dashboard.css: --chart-height",
  "app/dashboards/operations-dashboard.css: --chart-plot-min-height",
  "app/dashboards/operations-dashboard.css: --chart-stroke-width",
  "app/dashboards/operations-dashboard.css: --chart-x-axis-height",
  "app/dashboards/operations-dashboard.css: --chart-x-axis-type",
  "app/dashboards/operations-dashboard.css: --chart-y-axis-width",
  "app/search-workspace/search-editor.css: --search-editor-max-height",
  "app/search-workspace/search-job.css: --pulse-ring-core",
];

test("no stylesheet outside the token layer grows a token of its own", async () => {
  const declared = new Set();
  for (const block of await collectDeclarationBlocks(workspace, 1)) {
    if (/^app\/styles\/tokens-[a-z0-9-]+\.css$/u.test(block.file)) continue;
    for (const { property } of block.declarations) {
      if (property.startsWith("--")) declared.add(`${block.file}: ${property}`);
    }
  }
  assert.deepEqual(
    [...declared].toSorted(),
    [...COMPONENT_KNOBS].toSorted(),
    "app/styles/index.css: the token layer owns every theme value, and a feature sheet may declare a\n"
      + "custom property only as a component-scoped knob on its own container -- the ones that exist are\n"
      + "COMPONENT_KNOBS in this file.\n"
      + "A name declared outside app/styles/tokens-*.css is unreachable from a retheme: the one-file\n"
      + "edit the layer promises cannot see it. Move it into the token layer, or -- if it really is a\n"
      + "knob one component reads from its own container -- add it to COMPONENT_KNOBS:\n"
      + describeList([...declared].toSorted()),
  );
});

/**
 * Everything wrong with one block of a token file, as the reader of the
 * failure would want it said: its shape, the file it sits in, and what it
 * declares. `palettes` is the set of names `PALETTES` lists.
 *
 * A `data-palette="classic"` block is refused by name rather than left to the
 * file-ownership check, which a `tokens-palette-classic.css` would satisfy:
 * classic is the base pair, the boot script writes `data-palette="classic"`
 * explicitly, so such a block would apply and classic would stop rendering the
 * base blocks alone -- the one promise every palette's resolution chain rests
 * on.
 */
function tokenBlockProblems(file, block, palettes) {
  const owner = paletteOfFile(file);
  const problems = [];
  const { ancestors, declarations, prelude } = block;
  if (ancestors.length > 0) problems.push(`${file}: ${prelude} is nested inside ${ancestors.join(" > ")}`);
  const scope = themeScopeOf(block);
  if (scope === null) {
    if (ancestors.length === 0) problems.push(`${file}: ${prelude} is a rule, not one of the four theme blocks`);
  } else if (scope.palette === DEFAULT_PALETTE) {
    problems.push(
      `${file}: ${prelude} selects data-palette="${DEFAULT_PALETTE}", which is the base pair and has no block of its own`,
    );
  } else if (scope.palette !== null && !palettes.has(scope.palette)) {
    problems.push(`${file}: ${prelude} selects a palette ${PALETTE_MODULE} does not list in PALETTES`);
  } else if (scope.palette !== owner) {
    problems.push(
      `${file}: ${prelude} is the ${scopeLabel(scope.palette, scope.mode)} block, and this file holds only `
      + `${owner === null ? "base" : owner} blocks`,
    );
  }
  for (const { property } of declarations) {
    // `color-scheme` is the one non-token declaration a theme block owes
    // the browser: without it form controls and scrollbars keep the other
    // theme's colours.
    if (property.startsWith("--") || property === "color-scheme") continue;
    problems.push(`${file}: ${prelude} sets ${property}`);
  }
  return problems;
}

test("the token layer declares tokens and nothing else", async () => {
  const palettes = new Set(await readPaletteNames());
  const files = await collectTokenBlocks(workspace);
  const offenders = files.flatMap(({ blocks, file }) => (
    blocks.flatMap((block) => tokenBlockProblems(file, block, palettes))
  ));
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "A token file grew something that is not a token, or a theme block sits in the wrong file. Rules\n"
      + "belong in an application stylesheet; a palette's two blocks belong in its own\n"
      + "tokens-palette-<name>.css and the base pair in tokens-color.css and tokens-scale.css; classic\n"
      + "is the base pair and no block may select it; and the four preludes are exact, because\n"
      + "`:root[data-palette=\"x\"]` without `:where()` outranks the base dark block and leaks light\n"
      + `grounds into dark mode:\n${describeList(offenders.toSorted())}`,
  );
});

/** The two files of the base pair, the only token files that are not a palette's. */
const BASE_TOKEN_FILES = new Set(["app/styles/tokens-color.css", "app/styles/tokens-scale.css"]);

/**
 * Every way `names` (the `PALETTES` list) and a `collectTokenLayer` result
 * disagree: a name without a file, a file without a name, a palette file whose
 * blocks are not exactly [light, dark], a file for the default palette, and a
 * token file that is neither the base pair nor a palette's.
 *
 * The last is named so a `tokens-extra.css` cannot slip into the layer as a
 * third base file: `paletteOfFile` reads it as base, `tokenCascadeOrder`
 * loads it among the palettes, and nothing else would ask whose it is.
 */
function paletteLedgerProblems(names, layer) {
  const problems = [];
  for (const name of names) {
    if (name === DEFAULT_PALETTE) {
      // Classic is the base pair: `data-palette="classic"` is meant to select
      // nothing, so the one file a palette of that name could have is the one
      // file that must not exist. Named here rather than left to the
      // duplicate-declaration test, which only notices a classic file when
      // its light block collides with a base name and says nothing about a
      // dark-only one.
      if (layer.some((entry) => entry.file === paletteFile(name))) {
        problems.push(`${paletteFile(name)} exists, but ${name} is the base pair and has no palette file`);
      }
      continue;
    }
    const entry = layer.find((candidate) => candidate.file === paletteFile(name));
    if (entry === undefined) {
      problems.push(`${name} is in PALETTES but ${paletteFile(name)} does not exist`);
      continue;
    }
    const shapes = entry.blocks.map((block) => (
      block.mode === null ? `not a theme block (${block.selector})` : scopeLabel(block.palette, block.mode)
    ));
    const expected = [scopeLabel(name, "light"), scopeLabel(name, "dark")];
    if (shapes.join(", ") !== expected.join(", ")) {
      problems.push(`${entry.file} holds [${shapes.join(", ")}] and must hold exactly [${expected.join(", ")}]`);
    }
  }
  for (const entry of layer) {
    const owner = paletteOfFile(entry.file);
    if (owner === null && !BASE_TOKEN_FILES.has(entry.file)) {
      problems.push(`${entry.file} is a token file that is neither the base pair nor a tokens-palette-<name>.css`);
    }
    if (owner !== null && !names.includes(owner)) {
      problems.push(`${entry.file} exists but ${owner} is not in ${PALETTE_MODULE}'s PALETTES`);
    }
  }
  return problems.toSorted();
}

test("every palette in PALETTES has one token file with one light and one dark block", async () => {
  const names = await readPaletteNames();
  assert.ok(
    names.includes(DEFAULT_PALETTE),
    `${PALETTE_MODULE} lists PALETTES without ${DEFAULT_PALETTE}, which is the base pair every palette resolves through`,
  );
  const problems = paletteLedgerProblems(names, await collectTokenLayer(workspace));
  assert.deepEqual(
    problems,
    [],
    `${PALETTE_MODULE}'s PALETTES and the tokens-palette-*.css files disagree. The client offers every name\n`
      + "on that list, so a name without a file renders as classic while claiming not to, and a file\n"
      + "without a name is a look nobody can select. Every palette but classic ships exactly one light\n"
      + "and one dark block so the resolution chain has the same shape for all of them; classic is the\n"
      + `base pair, so a file for it would change what every chain resolves through:\n${describeList(problems)}`,
  );
});

/**
 * Every theme block of a `collectTokenBlocks` result whose `color-scheme`
 * disagrees with what it paints. `primitives` is the set of tier-1 names.
 *
 * `color-scheme` is what the browser paints scrollbars, form controls and the
 * canvas behind the page with, so a block that restates a colour owes the
 * browser the mode it belongs to, and a block that only restates scale --
 * the scale file, or a palette's radii and shadows -- would be making a claim
 * it is not entitled to make. It is an iff rather than an "at least": a
 * `color-scheme` on a colourless block is inert today and misleading the day
 * that block gains a colour of the other mode. A colour is a literal of any
 * spelling or a reference to a primitive.
 */
function colourSchemeProblems(files, primitives) {
  const offenders = [];
  for (const { blocks, file } of files) {
    for (const block of blocks) {
      const scope = themeScopeOf(block);
      if (scope === null) continue;
      const paints = block.declarations.some(({ property, value }) => (
        property.startsWith("--")
        && (WHOLE_COLOUR_LITERAL.test(value) || referencedTokens(value).some((name) => primitives.has(name)))
      ));
      const schemes = block.declarations.filter(({ property }) => property === "color-scheme");
      const site = `${file}: ${block.prelude}`;
      if (paints && schemes.length === 0) offenders.push(`${site} restates a colour and declares no color-scheme`);
      if (!paints && schemes.length > 0) offenders.push(`${site} restates no colour and declares color-scheme`);
      for (const { value } of schemes) {
        if (value !== scope.mode) offenders.push(`${site} is a ${scope.mode} block and declares color-scheme: ${value}`);
      }
    }
  }
  return offenders.toSorted();
}

test("a theme block declares color-scheme exactly when it paints, and names its own mode", async () => {
  const { primitives } = await readTokenLayer();
  const offenders = colourSchemeProblems(await collectTokenBlocks(workspace), primitives);
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "A theme block and its color-scheme disagree. A block that restates a colour without one leaves the\n"
      + "browser painting scrollbars, form controls and the canvas behind the page in the other mode; a\n"
      + "block that declares one without painting is claiming a mode for the scale tier; and a value\n"
      + `other than the block's own mode paints the controls against the page:\n${describeList(offenders.toSorted())}`,
  );
});

test("the fixture harness injects exactly the stylesheets the application loads", async () => {
  // `listHarnessStylesheets` runs the harness's own `importedStylesheets` body
  // rather than restating its parse, so this compares two independent things.
  // Restating it compared app/styles/index.css with itself, and a harness that
  // injected 25 of the 26 shipped sheets kept the whole suite green.
  const injected = await listHarnessStylesheets(workspace);
  const shipped = await listInjectedStylesheets(workspace);
  assert.deepEqual(
    injected,
    shipped,
    "integration/style-contracts/application-stylesheets.ts injects a different set, or a different order,\n"
      + "than app/styles/index.css loads. It cannot inject that file directly -- an @import does not\n"
      + "resolve inside an injected <style> -- so it reads the import list instead, and any filter,\n"
      + "slice or reorder on the way leaves the fixtures rendering var() fallbacks, or missing a\n"
      + `whole primitive while the contracts still pass.\n${describeList(injected)}`,
  );
  const exported = await readHarnessExportExpression(workspace);
  assert.equal(
    exported,
    "importedStylesheets()",
    "APPLICATION_STYLESHEETS is no longer that function's result verbatim. Running the function\n"
      + `proves the derivation; this proves nothing trimmed the result on its way out: ${exported}`,
  );
});

/* == 3. The naming grammar and what the names promise ========================== */

/** Group prefixes the semantic tier is allowed to use, from docs/theming.md. */
const SEMANTIC_GROUPS = ["accent", "bg", "border", "chart", "chrome", "fg", "level", "skeleton", "status", "syntax"];

/** The three interaction tokens the documented grammar leaves ungrouped. */
const INTERACTION_TOKENS = new Set(["--focus-ring", "--highlight", "--selection"]);

/**
 * Name families `app/styles/tokens-scale.css` is allowed to use.
 *
 * `alpha` and `backdrop` are the translucency knobs a palette may turn: the
 * opacity of the chrome bars and of raised surfaces, and the `backdrop-filter`
 * behind a translucent surface. Classic holds them at `100%` and `none`.
 */
const SCALE_FAMILIES = ["alpha", "backdrop", "dur", "ease", "font", "opacity", "radius", "shadow", "space", "type", "z"];

/**
 * The lowest opacity a palette may give an `--alpha-*` knob, as a percentage.
 *
 * Every contrast ratio this suite proves is measured on the opaque hex the
 * tokens resolve to. A surface painted at less than this over an unknown
 * ground can drop below AA in ways no static check can see, so the floor is
 * the point past which the proof stops meaning anything.
 */
const ALPHA_FLOOR = 80;

/** The ladder a primitive step number may sit on: light at 0, darkest at 950. */
const PRIMITIVE_STEPS = new Set([0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 900, 950]);

/**
 * Pairings the tokens' own role comments promise, as `[foreground, background]`.
 *
 * Only the pairings the layer commits to in writing are here: body and heading
 * text on each of the four surfaces, `--fg-inverse`, whose comment reads "Text
 * on --bg-inverse", and `--chrome-fg`, whose comment reads "Text and icons on
 * either bar" and which is a separate role precisely because the inverse
 * surface flips between themes and the two chrome bars do not. Every ground a
 * foreground token's comment names is still checked, in both themes; what
 * changed is which token owns the bars. Secondary and tertiary text
 * are left out on purpose -- `--fg-faint` is placeholder ink, so pinning it
 * here would turn a current implementation detail into an accessibility
 * promise.
 */
const MANDATED_TEXT_PAIRS = [
  ["--fg-text", "--bg-canvas"],
  ["--fg-text", "--bg-surface"],
  ["--fg-text", "--bg-subtle"],
  ["--fg-text", "--bg-raised"],
  ["--fg-strong", "--bg-canvas"],
  ["--fg-strong", "--bg-surface"],
  ["--fg-strong", "--bg-subtle"],
  ["--fg-strong", "--bg-raised"],
  ["--fg-inverse", "--bg-inverse"],
  ["--chrome-fg", "--chrome-bar"],
  ["--chrome-fg", "--chrome-appbar"],
  ["--chrome-fg", "--chrome-hover"],
];

/** WCAG 2.2 AA for text below 18.66px, which is every size this product ships. */
const AA_CONTRAST = 4.5;

/** Role groups whose members must stay visually distinct from one another. */
const ROLE_GROUPS = {
  accent: /^--accent(?:-|$)/u,
  background: /^--bg-/u,
  border: /^--border(?:-|$)/u,
  "chart series": /^--chart-series-\d+$/u,
  chrome: /^--chrome-/u,
  foreground: /^--fg-/u,
  interaction: /^--(?:focus-ring|highlight|selection)$/u,
  "severity level": /^--level-/u,
  status: /^--status-(?!.*-soft$)/u,
  "status wash": /^--status-.*-soft$/u,
  syntax: /^--syntax-/u,
};

/**
 * Reads the layer once and indexes it the way every assertion below needs.
 *
 * `base.light` and `base.dark` are the two blocks of the base pair, merged
 * across `tokens-color.css` and `tokens-scale.css`; `palettes` maps each
 * palette name to its file and its two blocks; and `scopes` lists every
 * palette x mode corner the cascade can land in, each with the `chain` of
 * blocks the browser resolves it through, in cascade order:
 *
 *   classic light   [base.light]
 *   classic dark    [base.light, base.dark]
 *   P light         [base.light, P.light]
 *   P dark          [base.light, P.light, base.dark, P.dark]
 *
 * A scope's `values` is that chain merged, later blocks winning, which is what
 * `resolve` reads; `scopeWithout` below builds the same merge minus one block.
 * `primitives` and `fileOf` are read off the base light block, the one block
 * that introduces a name. A block whose prelude is not one of the four theme
 * shapes is left out here -- "the token layer declares tokens and nothing
 * else" reports it -- so a stray rule cannot be filed under a theme.
 */
async function readTokenLayer() {
  return indexTokenLayer(await collectTokenLayer(workspace), await readContrastFloors());
}

/**
 * `readTokenLayer` over a `collectTokenLayer` result handed in, so a synthetic
 * layer can be indexed; `floors` is the `PALETTE_CONTRAST_FLOOR` table the
 * contrast check reads, AA everywhere when omitted.
 */
function indexTokenLayer(layer, floors = {}) {
  const base = { dark: new Map(), light: new Map() };
  const palettes = new Map();
  const fileOf = new Map();
  for (const entry of layer) {
    for (const block of entry.blocks) {
      if (block.mode === null) continue;
      let target;
      if (block.palette === null) target = base[block.mode];
      else {
        if (!palettes.has(block.palette)) {
          palettes.set(block.palette, { dark: new Map(), file: entry.file, light: new Map() });
        }
        target = palettes.get(block.palette)[block.mode];
      }
      for (const token of block.tokens) {
        target.set(token.name, token.value);
        if (isBaseLight(block)) fileOf.set(token.name, entry.file);
      }
    }
  }
  const primitives = new Map([...base.light].filter(([, value]) => value.startsWith("#")));
  const scopes = [
    { chain: [base.light], mode: "light", palette: null },
    { chain: [base.light, base.dark], mode: "dark", palette: null },
  ];
  for (const [name, palette] of [...palettes].toSorted(([left], [right]) => left.localeCompare(right))) {
    scopes.push({ chain: [base.light, palette.light], mode: "light", palette: name });
    scopes.push({ chain: [base.light, palette.light, base.dark, palette.dark], mode: "dark", palette: name });
  }
  for (const scope of scopes) {
    scope.label = scopeLabel(scope.palette, scope.mode);
    scope.values = mergeChain(scope.chain);
  }
  return { base, fileOf, floors, palettes, primitives, scopes };
}

/** A chain of blocks merged into one lookup, later blocks winning. */
function mergeChain(chain) {
  return new Map(chain.flatMap((block) => [...block]));
}

/**
 * A scope's values as the browser would resolve them if `block` were deleted.
 *
 * This is how "restates nothing it does not change" is asked for a block in
 * the middle of a chain: a palette light restatement is inert if the value
 * with it equals the value without it, and only the chain that contains the
 * block can answer that. `block` is one of the Maps in `scope.chain`.
 */
function scopeWithout(scope, block) {
  return mergeChain(scope.chain.filter((candidate) => candidate !== block));
}

/** The scope whose chain ends in `block`: the corner that block's restatements land in. */
function scopeEndingIn(scopes, block) {
  return scopes.find((scope) => scope.chain.at(-1) === block);
}

/** Every block that restates the base light block, with the scope it lands in. */
function restatingBlocks({ base, palettes, scopes }) {
  const blocks = [{ block: base.dark, scope: scopeEndingIn(scopes, base.dark) }];
  for (const palette of palettes.values()) {
    blocks.push({ block: palette.light, scope: scopeEndingIn(scopes, palette.light) });
    blocks.push({ block: palette.dark, scope: scopeEndingIn(scopes, palette.dark) });
  }
  return blocks;
}

/** Follows a chain of bare `var()` indirections down to the value it lands on. */
function resolve(name, scope) {
  const seen = new Set([name]);
  let value = scope.get(name);
  while (value !== undefined) {
    const reference = /^var\(\s*(--[\w-]+)\s*\)$/u.exec(value);
    if (reference === null) return value;
    if (seen.has(reference[1])) return `cycle through ${reference[1]}`;
    seen.add(reference[1]);
    value = scope.get(reference[1]);
  }
  return undefined;
}

/** sRGB channels of a six-digit hex literal. */
function channels(hex) {
  return [1, 3, 5].map((offset) => Number.parseInt(hex.slice(offset, offset + 2), 16));
}

/** WCAG relative luminance. */
function luminance(hex) {
  const [red, green, blue] = channels(hex).map((channel) => {
    const scaled = channel / 255;
    return scaled <= 0.040_45 ? scaled / 12.92 : ((scaled + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
}

/**
 * CIE L*, the perceptual lightness a reader means by "one step darker".
 *
 * Relative luminance would do for ordering a single hue, but the palette mixes
 * hues of very different saturation and L* is the axis the step numbers claim
 * to walk.
 */
function lightness(hex) {
  const y = luminance(hex);
  return y > 216 / 24_389 ? 116 * Math.cbrt(y) - 16 : (y * 24_389) / 27;
}

function contrastRatio(first, second) {
  const [darker, lighter] = [luminance(first), luminance(second)].toSorted((a, b) => a - b);
  return (lighter + 0.05) / (darker + 0.05);
}

/** The kind of value a token holds, which every name family has to agree on. */
function valueKind(value) {
  if (value === undefined) return "undefined";
  if (/^#[0-9a-f]{3,8}$/iu.test(value) || /^(?:rgba?|hsla?|oklch|color)\(/iu.test(value)) return "colour";
  if (/^-?[\d.]+(?:px|rem|em|%)$/u.test(value)) return "length";
  // A bare `0` is a length: CSS accepts it wherever a `px` goes, stylelint
  // refuses `0px`, and a palette with square corners has to write
  // `--radius-sm: 0` without splitting the --radius-* family in two.
  if (value === "0") return "length";
  // A clamp(), min() or max() over lengths is a length: CSS accepts one
  // wherever it accepts a `px`, and reading it as a keyword would split the
  // --type-* family in two over a fluid step that is a font-size like the rest.
  if (/^(?:clamp|min|max)\((?:\s*-?[\d.]+(?:px|rem|em|%|vw|vh|dvw|dvh)\s*,?)+\)$/u.test(value)) return "length";
  if (/^-?[\d.]+m?s$/u.test(value)) return "duration";
  if (/^-?\d+(?:\.\d+)?$/u.test(value)) return "number";
  if (/\brgba?\(/u.test(value)) return "shadow";
  return "keyword or stack";
}

/** The first `--`-delimited segment of a name, which is its family. */
function family(name) {
  return /^--([a-z0-9]+)/u.exec(name)?.[1] ?? name;
}

test("every token name parses under the documented naming grammar", async () => {
  // The base light block introduces every name; "every theme block redefines
  // only names the base light block declares" is what makes reading it alone
  // complete.
  const { base, primitives } = await readTokenLayer();
  const offenders = [];
  for (const name of base.light.keys()) {
    if (!/^--[a-z0-9]+(?:-[a-z0-9]+)*$/u.test(name)) {
      offenders.push(`${name} is not lowercase kebab-case`);
      continue;
    }
    if (primitives.has(name)) {
      const parsed = /^--[a-z]+-(\d+)$/u.exec(name);
      if (parsed === null) offenders.push(`${name} holds a literal but is not named --<hue>-<step>`);
      else if (!PRIMITIVE_STEPS.has(Number(parsed[1]))) {
        offenders.push(`${name} sits on step ${parsed[1]}, which is not on the 0-950 ladder`);
      }
      continue;
    }
    if (INTERACTION_TOKENS.has(name)) continue;
    if (SEMANTIC_GROUPS.some((group) => name.startsWith(`--${group}-`) || name === `--${group}`)) continue;
    if (SCALE_FAMILIES.some((scale) => name.startsWith(`--${scale}-`) || name === `--${scale}`)) continue;
    offenders.push(`${name} belongs to no documented group; docs/theming.md lists ${SEMANTIC_GROUPS.join(", ")}`);
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "A token name is outside the grammar docs/theming.md documents. Either it needs a documented\n"
      + `group prefix, or the document needs the new group written into it:\n${describeList(offenders.toSorted())}`,
  );
});

test("no semantic or scale token name mentions a hue", async () => {
  const { base, primitives } = await readTokenLayer();
  const hues = new Set([...primitives.keys()].map((name) => family(name)));
  const offenders = [];
  for (const name of base.light.keys()) {
    if (primitives.has(name)) continue;
    const mentioned = name.slice(2).split("-").filter((segment) => hues.has(segment));
    if (mentioned.length > 0) offenders.push(`${name} names the hue ${mentioned.join(", ")}`);
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "docs/theming.md: \"No token name mentions a hue. --status-error, never --status-red.\" A hue in\n"
      + "the name freezes the colour into every call site and a theme can no longer move it:\n"
      + `${describeList(offenders.toSorted())}`,
  );
});

test("every primitive is a distinct colour", async () => {
  const { primitives } = await readTokenLayer();
  const byColour = new Map();
  for (const [name, value] of primitives) {
    const key = value.toLowerCase();
    byColour.set(key, [...(byColour.get(key) ?? []), name]);
  }
  const shared = [...byColour]
    .filter(([, names]) => names.length > 1)
    .map(([value, names]) => `${names.join(" and ")} are both ${value}`);
  assert.deepEqual(
    shared.toSorted(),
    [],
    "Two primitives hold the same literal, so the palette has a step that says nothing. Point the\n"
      + `semantic tokens at one of them and delete the other:\n${describeList(shared.toSorted())}`,
  );
});

test("a primitive family runs light to dark as its step number rises", async () => {
  const { primitives } = await readTokenLayer();
  const families = new Map();
  for (const [name, value] of primitives) {
    const parsed = /^--([a-z]+)-(\d+)$/u.exec(name);
    if (parsed === null) continue;
    families.set(parsed[1], [...(families.get(parsed[1]) ?? []), { hex: value, name, step: Number(parsed[2]) }]);
  }
  const inversions = [];
  for (const members of families.values()) {
    const ordered = members.toSorted((first, second) => first.step - second.step);
    for (let index = 1; index < ordered.length; index += 1) {
      const previous = ordered[index - 1];
      const current = ordered[index];
      if (lightness(current.hex) >= lightness(previous.hex)) {
        inversions.push(
          `${current.name} (${current.hex}, L* ${lightness(current.hex).toFixed(1)}) is not darker than `
          + `${previous.name} (${previous.hex}, L* ${lightness(previous.hex).toFixed(1)})`,
        );
      }
    }
  }
  assert.deepEqual(
    inversions.toSorted(),
    [],
    "docs/theming.md: a primitive is \"a hue family plus a lightness step, where a lower number is\n"
      + "lighter\". Where that is false the ladder lies, and \"one step darker\" during the migration\n"
      + `picks a lighter colour. Renumber the step:\n${describeList(inversions.toSorted())}`,
  );
});

/**
 * Every name family whose members resolve to different kinds of value in some
 * scope of an indexed layer.
 *
 * Per scope rather than per block: a palette restates `--radius-sm: 0` or
 * `--shadow-md` on its own, and the kind that has to agree is the kind a
 * reader of var(--radius-sm) gets under that palette, which only the resolved
 * chain can say.
 */
function familyKindMismatches({ base, fileOf, scopes }) {
  const families = new Map();
  for (const name of base.light.keys()) {
    families.set(family(name), [...(families.get(family(name)) ?? []), name]);
  }
  const mixed = [];
  for (const scope of scopes) {
    for (const [group, names] of families) {
      const kinds = new Map();
      for (const name of names) {
        const kind = valueKind(resolve(name, scope.values));
        kinds.set(kind, [...(kinds.get(kind) ?? []), `${name} (${fileOf.get(name)})`]);
      }
      if (kinds.size === 1) continue;
      mixed.push(
        `${scope.label} --${group}-*: ${[...kinds].map(([kind, members]) => `${kind} = ${members.join(", ")}`).join("; ")}`,
      );
    }
  }
  return mixed.toSorted();
}

test("every token in a name family holds the same kind of value in every scope", async () => {
  const mixed = familyKindMismatches(await readTokenLayer());
  assert.deepEqual(
    mixed,
    [],
    "One name family means two different things, so nothing about var(--family-x) tells a reader\n"
      + "whether it is a colour or a length. Rename the family that has no consumers yet -- the scale\n"
      + `tiers are still unread -- rather than the one with call sites:\n${describeList(mixed)}`,
  );
});

test("a semantic token points at a primitive and a primitive holds a literal", async () => {
  const { base, palettes, primitives } = await readTokenLayer();
  const blocks = [["classic light", base.light], ["classic dark", base.dark]];
  for (const [name, palette] of palettes) {
    blocks.push([scopeLabel(name, "light"), palette.light], [scopeLabel(name, "dark"), palette.dark]);
  }
  const offenders = [];
  for (const [label, declarations] of blocks) {
    for (const [name, value] of declarations) {
      const references = [...value.matchAll(/var\(\s*(--[\w-]+)/gu)].map((match) => match[1]);
      if (primitives.has(name) && declarations === base.light) {
        if (references.length > 0) offenders.push(`${name} is a primitive but reads ${references.join(", ")}`);
        continue;
      }
      for (const reference of references) {
        if (!primitives.has(reference)) {
          offenders.push(`${label} ${name} reads ${reference}, which is not a tier-1 primitive`);
        }
      }
    }
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "docs/theming.md: \"Point each token at a primitive, not at a literal.\" A semantic token that\n"
      + "reads another semantic token inherits a role it was not given, and a theme that moves one\n"
      + `moves both:\n${describeList(offenders.toSorted())}`,
  );
});

/** Every pair of same-group roles that resolve to one colour, in any scope of an indexed layer. */
function roleGroupCollisions({ base, primitives, scopes }) {
  const collisions = [];
  for (const scope of scopes) {
    for (const [group, pattern] of Object.entries(ROLE_GROUPS)) {
      const byColour = new Map();
      for (const name of base.light.keys()) {
        if (primitives.has(name) || !pattern.test(name)) continue;
        const value = resolve(name, scope.values);
        byColour.set(value, [...(byColour.get(value) ?? []), name]);
      }
      for (const [value, names] of byColour) {
        if (names.length > 1) collisions.push(`${scope.label} ${group}: ${names.join(" and ")} are both ${value}`);
      }
    }
  }
  return collisions.toSorted();
}

test("no two tokens in a role group resolve to the same colour in any scope", async () => {
  const collisions = roleGroupCollisions(await readTokenLayer());
  assert.deepEqual(
    collisions,
    [],
    "Two roles in the same group render identically, so the interface cannot tell them apart. Either\n"
      + `they are one role and want one token, or one of them needs its own step:\n${describeList(collisions)}`,
  );
});

/**
 * Every restatement in an indexed layer that changes nothing.
 *
 * For every block after base light, and every name it declares: the value the
 * browser lands on with the block has to differ from the value it lands on
 * without it, in the scope that block's chain ends in. A palette dark
 * restatement that equals what base dark already set, or a palette light one
 * that equals classic light, is inert.
 */
function inertRestatements(layer) {
  const inert = [];
  for (const { block, scope } of restatingBlocks(layer)) {
    const without = scopeWithout(scope, block);
    for (const [name, value] of block) {
      const after = resolve(name, scope.values);
      if (after === resolve(name, without)) {
        inert.push(`${scope.label}: ${name}: ${value} resolves to ${after} with or without the block`);
      }
    }
  }
  return inert.toSorted();
}

test("no theme block restates a token it does not change", async () => {
  const inert = inertRestatements(await readTokenLayer());
  assert.deepEqual(
    inert,
    [],
    "docs/theming.md, Adding a theme, step 4: \"Restate only what changes.\" A restatement that changes\n"
      + "nothing is dead weight a future theme has to keep in step with the blocks before it, and it\n"
      + `hides whether the equality was decided or accidental. Delete it:\n${describeList(inert)}`,
  );
});

test("the dark theme restates every themeable semantic token", async () => {
  // Base dark only: a palette restates what differs from the chain before it
  // and inherits the rest, so completeness is a promise the base pair makes.
  const { base, primitives } = await readTokenLayer();
  const missing = [];
  for (const name of base.light.keys()) {
    if (primitives.has(name) || base.dark.has(name)) continue;
    // The categorical ramp is documented as theme-independent: those twelve
    // hues separate from each other, not from the background.
    if (/^--chart-series-\d+$/u.test(name)) continue;
    if (!base.light.get(name).startsWith("var(")) continue;
    missing.push(name);
  }
  assert.deepEqual(
    missing.toSorted(),
    [],
    "A semantic colour token has no dark value, so it keeps its light primitive on a dark ground.\n"
      + "Either restate it in the dark block, or document it beside the chart ramp as deliberately\n"
      + `theme-independent:\n${describeList(missing.toSorted())}`,
  );
});

/**
 * The contrast floor a scope is held to: the palette's entry in `floors`
 * (the `PALETTE_CONTRAST_FLOOR` table), else AA. Read with `Object.hasOwn`
 * rather than by index: a palette is named by a string the file system
 * accepts, and `constructor` or `toString` would otherwise read a function
 * off `Object.prototype` and turn every `ratio < floor` into a comparison
 * with NaN that never fails.
 */
function contrastFloorOf(palette, floors) {
  const name = palette ?? DEFAULT_PALETTE;
  return Object.hasOwn(floors, name) ? floors[name] : AA_CONTRAST;
}

/**
 * Every mandated text pair that misses its scope's contrast floor, in any
 * scope of an indexed layer; a pair whose ink or ground does not resolve to a
 * six-digit hex is reported rather than measured.
 */
function contrastFailures({ floors, scopes }) {
  const failures = [];
  for (const scope of scopes) {
    const floor = contrastFloorOf(scope.palette, floors);
    for (const [foreground, background] of MANDATED_TEXT_PAIRS) {
      const ink = resolve(foreground, scope.values);
      const ground = resolve(background, scope.values);
      const unresolved = [[foreground, ink], [background, ground]]
        .filter(([, value]) => !/^#[0-9a-f]{6}$/iu.test(value ?? ""));
      if (unresolved.length > 0) {
        for (const [name, value] of unresolved) failures.push(`${scope.label}: ${name} is not a six-digit hex: ${value}`);
        continue;
      }
      const ratio = contrastRatio(ink, ground);
      if (ratio < floor) {
        failures.push(
          `${scope.label}: ${foreground} (${ink}) on ${background} (${ground}) is ${ratio.toFixed(2)}:1, below ${floor}:1`,
        );
      }
    }
  }
  return failures.toSorted();
}

test("text keeps its contrast floor against every ground its role comment promises, in every scope", async () => {
  const failures = contrastFailures(await readTokenLayer());
  assert.deepEqual(
    failures,
    [],
    `Text falls below its palette's contrast floor (WCAG AA, ${AA_CONTRAST}:1, or the higher floor\n`
      + `PALETTE_CONTRAST_FLOOR in ${PALETTE_MODULE} pins for a palette) on a ground its own role comment names. A theme that ships\n`
      + "this renders those labels unreadable, and a ratio near 1 renders them invisible:\n"
      + `${describeList(failures)}`,
  );
});

/** The four block shapes of an indexed layer as `[label, Map]` pairs, base pair first. */
function labelledBlocks({ base, palettes }) {
  const blocks = [["classic light", base.light], ["classic dark", base.dark]];
  for (const [name, palette] of palettes) {
    blocks.push([scopeLabel(name, "light"), palette.light], [scopeLabel(name, "dark"), palette.dark]);
  }
  return blocks;
}

/**
 * Every `--alpha-*` declaration in an indexed layer that is not a percentage
 * at or above `ALPHA_FLOOR`.
 *
 * Every contrast ratio above is proved on the opaque hex a token resolves to.
 * A translucent chrome bar or raised surface composites that hex over whatever
 * scrolls beneath it, and past a point no static check can say what the eye
 * meets. `ALPHA_FLOOR` is that point; the base pair holds every `--alpha-*` at
 * 100%, so only a palette can approach it.
 */
function alphaKnobProblems(layer) {
  const offenders = [];
  for (const [label, declarations] of labelledBlocks(layer)) {
    for (const [name, value] of declarations) {
      if (family(name) !== "alpha") continue;
      const percent = /^(\d+(?:\.\d+)?)%$/u.exec(value);
      if (percent === null) offenders.push(`${label}: ${name}: ${value} is not a percentage`);
      else if (Number(percent[1]) < ALPHA_FLOOR) offenders.push(`${label}: ${name}: ${value} is below ${ALPHA_FLOOR}%`);
    }
  }
  return offenders.toSorted();
}

test("a palette alpha knob never drops below 80%", async () => {
  const offenders = alphaKnobProblems(await readTokenLayer());
  assert.deepEqual(
    offenders,
    [],
    `An --alpha-* knob is below ${ALPHA_FLOOR}%, or is not a percentage. AA contrast is proved on the opaque\n`
      + "hex each token resolves to, and a surface more translucent than this composites that hex over\n"
      + `an unknown ground where no static check can follow it:\n${describeList(offenders)}`,
  );
});

test("every token outside the scale file states its role in one line", async () => {
  const files = await collectTokenComments(workspace);
  const undocumented = [];
  for (const { declarations, file } of files) {
    for (const { comment, name } of declarations) {
      // The scale file states each family's rationale in a banner above it,
      // which is the same promise made once for eight steps rather than eight
      // times; the colour tier and every palette file carry a comment per
      // token, because each restatement is a decision about a role.
      if (file === "app/styles/tokens-scale.css") continue;
      if (comment === null || comment.length === 0) undocumented.push(`${file}: ${name}`);
    }
  }
  assert.deepEqual(
    undocumented.toSorted(),
    [],
    "docs/theming.md: \"Every token carries a one-line comment naming its role. A token whose role\n"
      + "cannot be stated in one line is usually two tokens.\" These have no comment on their\n"
      + `declaration line:\n${describeList(undocumented.toSorted())}`,
  );
});

/* == 4. The literal sweep: a value written out is a value no theme can move ==== */

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

async function readLedger() {
  return JSON.parse(await readFile(ledgerPath, "utf8"));
}

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

/* == 5. The stylesheet set: one entry point, one lane, one order =============== */

/** The file whose cross-cutting rules are deliberately not colocated. */
const CROSS_CUTTING_STYLESHEET = "app/styles/interaction.css";

/** The load order `app/styles/index.css` documents as its contract. */
const LOAD_ORDER = Object.freeze(["tokens", "base", "primitives", "features", "interaction"]);

/** Which band of the load order an import specifier belongs to. */
function loadBand(specifier) {
  if (/^\.\/tokens-[a-z0-9-]+\.css$/u.test(specifier)) return "tokens";
  if (specifier === "./base.css") return "base";
  if (specifier.startsWith("./primitives/")) return "primitives";
  if (specifier === "./interaction.css") return "interaction";
  return "features";
}

test("app/styles/index.css imports every application stylesheet exactly once", async () => {
  const imports = await listIndexImports(workspace);
  const counts = new Map();
  for (const entry of imports) counts.set(entry.file, (counts.get(entry.file) ?? 0) + 1);
  const repeated = [...counts.entries()]
    .filter(([, count]) => count > 1)
    .map(([file, count]) => `${file} is imported ${count} times`)
    .toSorted();
  assert.deepEqual(
    repeated,
    [],
    "A stylesheet is imported twice. Its rules are then stated twice, and the second copy wins ties the\n"
      + `first one used to lose, which no baseline can show:\n${describeList(repeated)}`,
  );
  const application = await listApplicationStylesheets(workspace);
  const present = new Set(application);
  const missing = imports
    .filter((entry) => !present.has(entry.file))
    .map((entry) => `${entry.specifier} resolves to ${entry.file}`);
  assert.deepEqual(
    missing.toSorted(),
    [],
    `${STYLESHEET_ENTRY_POINT} imports files that do not exist. The bundler drops an unresolved @import,`
      + ` so the rules are simply absent:\n${describeList(missing.toSorted())}`,
  );
  const orphans = application
    .filter((file) => file !== STYLESHEET_ENTRY_POINT && !counts.has(file))
    .toSorted();
  assert.deepEqual(
    orphans,
    [],
    `app/layout.tsx imports ${STYLESHEET_ENTRY_POINT} and nothing else, so a stylesheet that file does not`
      + ` import ships to nobody:\n${describeList(orphans)}`,
  );
});

test("app/styles/index.css states imports the scanner reads, and nothing else", async () => {
  const imports = await listIndexImports(workspace);
  const { rules, statements } = await collectEntryPointContent(workspace);
  assert.equal(
    statements,
    imports.length,
    `${STYLESHEET_ENTRY_POINT} states ${statements} @import rules and the scan reads ${imports.length} of them.`
      + " The browser loads all of them; every check built on the import list -- the reachability walk, the"
      + " fixture injection, and import-order checks -- covers only the ones it can parse, and says"
      + " nothing at all about the rest. Write the missed one as @import url(\"…\"), or teach the scanner"
      + " the spelling.",
  );
  assert.deepEqual(
    rules,
    [],
    `${STYLESHEET_ENTRY_POINT} declares a rule of its own. The entry point is the one stylesheet the layer`
      + " derives its file set from rather than being a member of it, so a rule written here is styled by"
      + " nothing any reachability, retired-class or one-primitive check can see. Put it in the file whose"
      + ` feature it belongs to:\n${describeList(rules)}`,
  );
});

test("app/layout.tsx is the only file that pulls a stylesheet in", async () => {
  const sites = await collectStylesheetImportSites(workspace);
  assert.deepEqual(
    sites,
    [`app/layout.tsx imports ./${STYLESHEET_ENTRY_POINT.split("/").slice(1).join("/")}`],
    "The stylesheet layer has one entry point and one @import list, which is what makes the cascade order\n"
      + "a thing you can read. A second import site puts part of the order back into JavaScript module\n"
      + `order or into the bundler's chunking, where nothing states it:\n${describeList(sites)}`,
  );
});

test("no CSS module and no styles object survives", async () => {
  const references = await collectModuleLaneReferences(workspace);
  assert.deepEqual(
    references,
    [],
    "The module lane is back. A module's classes are scoped to a generated hash, so no reachability,\n"
      + "retired-class or one-primitive check can see them, and a `styles.x` read of a class the module no\n"
      + "longer has yields undefined and puts the string \"undefined\" in a class attribute. Rename the\n"
      + `classes with the feature's prefix and import the plain stylesheet instead:\n${describeList(references)}`,
  );
});

test("no stylesheet reaches for a module scoping escape", async () => {
  const escapes = await collectScopeEscapes(workspace);
  assert.deepEqual(
    escapes,
    [],
    ":global(), :local() and `composes` are CSS-module syntax. A browser does not implement any of them,\n"
      + "so in a plain stylesheet each is a silent no-op rather than an error and the rule simply never\n"
      + `applies. With one namespace, a descendant selector says the same thing:\n${describeList(escapes)}`,
  );
});

test("no test file reads a stylesheet's characters, however the path is composed", async () => {
  // Two scans, because each sees a shape the other cannot. The first reads a
  // call's first argument, which catches `readFileSync("app/styles/base.css")`,
  // a path bound to a variable first, and a bare `import "./…css"`. The second
  // follows a path composed inside the call -- `readFileSync(path.join(root,
  // "app", "styles", "base.css"), "utf8")` -- which hands the first nothing that
  // looks like a stylesheet, and is how a read of
  // app/styles/primitives/table.css sat in app/activity/backend-audit-data.test.ts
  // with the invariant green.
  const offenders = [
    ...await findTestStylesheetReads(workspace),
    ...await collectTestStylesheetReads(workspace),
  ].toSorted();
  assert.deepEqual(
    offenders,
    [],
    "A test asserts on stylesheet text. A rule moves between files routinely now, so an assertion\n"
      + "naming the file a rule lives in fails on a move that changed no pixel, and passes on a rule\n"
      + "the cascade overrides everywhere. Assert on rendered behaviour instead: computed-style\n"
      + `contracts live in integration/style-contracts/css-contracts.spec.ts.\n${describeList(offenders)}`,
  );
});

test("app/styles/index.css loads tokens, base, the primitives, the features, then interaction", async () => {
  // docs/theming.md's "Where a rule lives" table, as a contract rather than as
  // prose. Every claim here is one the cascade decides and no screenshot can: a
  // token file stated after a rule that reads it paints the fallback; a feature
  // above a primitive hands `.table-wrap` the win over `.reports-table-wrap` at
  // the same specificity; and interaction.css anywhere but last lets a feature's
  // own min-height or font-size beat the coarse-pointer tap target and the 16px
  // input floor, neither of which any 1440px or 760px screenshot renders.
  const imports = await listIndexImports(workspace);
  const loaded = imports.map((entry) => entry.file);
  // The head is pinned name by name because the band check below leaves the
  // sequence inside the primitive band free, and the later primitives are
  // written to lean on the foundational ones above them. The token files come
  // in `tokenCascadeOrder`: colour, scale, then every palette, because a
  // palette's light block beats the base light block by source order alone
  // (its `:where()` keeps it at the same specificity) and restates scale names
  // as well as colour names.
  const tokens = await tokenCascadeOrder(workspace);
  assert.deepEqual(
    tokens.toSorted(),
    (await listTokenStylesheets(workspace)).map((file) => relativePosix(workspace, file)).toSorted(),
    "tokenCascadeOrder no longer names every token file, so a palette file could load anywhere",
  );
  const head = [
    ...tokens,
    "app/styles/base.css",
    "app/styles/primitives/button.css",
    "app/styles/primitives/table.css",
    "app/styles/primitives/form.css",
    "app/styles/primitives/modal.css",
    "app/styles/primitives/status.css",
    "app/styles/primitives/layout.css",
    "app/styles/primitives/chart.css",
    "app/styles/primitives/skeleton.css",
  ];
  assert.deepEqual(
    loaded.slice(0, head.length),
    head,
    "The tokens, the base sheet and the eight primitives are no longer the first imports, in that\n"
      + "order. Tokens first because a rule that reads a name declared after it paints the fallback;\n"
      + "primitives before every feature because `.reports-table-wrap` and `.table-wrap` are both one\n"
      + `class and the feature is the one meant to win.\n${describeList(loaded.slice(0, head.length))}`,
  );
  const bands = imports.map((entry) => loadBand(entry.specifier));
  const outOfBand = [];
  let reached = 0;
  imports.forEach((entry, index) => {
    const position = LOAD_ORDER.indexOf(bands[index]);
    if (position < reached) outOfBand.push(`${entry.specifier} is ${bands[index]} but follows ${LOAD_ORDER[reached]}`);
    else reached = position;
  });
  assert.deepEqual(
    outOfBand,
    [],
    `${STYLESHEET_ENTRY_POINT} states its order as a contract, and each band exists to be overridable by\n`
      + "the ones below it: a feature rule and the primitive it overrides regularly tie on specificity --\n"
      + `.reports-table-wrap against .table-wrap -- and the feature is the one meant to win:\n${describeList(outOfBand)}`,
  );
  for (const band of LOAD_ORDER) {
    assert.ok(bands.includes(band), `no import falls in the ${band} band, so its half of the order is untested`);
  }
  const misplaced = imports
    .filter((entry, index) => (bands[index] === "features") !== entry.specifier.startsWith("../"))
    .map((entry) => entry.specifier)
    .toSorted();
  assert.deepEqual(
    misplaced,
    [],
    "A feature stylesheet lives with the code that renders it, and a shared one lives under app/styles.\n"
      + "A feature file inside the shared layer is a rule nobody finds from the component, and a shared\n"
      + `file outside it is a primitive with a feature's name on it:\n${describeList(misplaced)}`,
  );
  assert.equal(
    loaded.at(-1),
    CROSS_CUTTING_STYLESHEET,
    `${CROSS_CUTTING_STYLESHEET} states the coarse-pointer tap target, the 16px input floor and the`
      + " reduced-motion cut. Each has to outrank every feature rule it applies to, and at equal"
      + " specificity that is decided by being last. Whatever now sits below it wins those instead.",
  );
});

/* == 6. Where a responsive rule lives ========================================== */

/**
 * Responsive rules that override base rules no file of their own declares.
 *
 * A class with no base rule anywhere is not homeless: `.table--cards` and
 * `.mobile-search-menu` exist only below a breakpoint, so the media block is
 * their one home and there is nothing for them to sit beside. The offence is a
 * rule whose every class is based somewhere and based nowhere here -- a
 * responsive block left behind by the move that took its base rules away.
 */
function orphanedResponsiveRules({ files, owners }) {
  const orphans = [];
  for (const entry of files) {
    if (entry.file === CROSS_CUTTING_STYLESHEET) continue;
    for (const rule of entry.responsive) {
      if (rule.classes.length === 0) continue;
      if (rule.classes.some((className) => entry.base.has(className))) continue;
      if (!rule.classes.every((className) => owners.has(className))) continue;
      const elsewhere = rule.classes
        .map((className) => `.${className} is based in ${[...owners.get(className)].join(", ")}`)
        .join("; ");
      orphans.push(`${entry.file} :: ${rule.at} :: ${rule.selector} -- ${elsewhere}`);
    }
  }
  return orphans.toSorted();
}

test("every responsive rule sits in a file that owns one of its base rules", async () => {
  const ownership = await collectResponsiveOwnership(workspace);
  const responsive = ownership.files.reduce((total, entry) => total + entry.responsive.length, 0);
  assert.ok(
    responsive > 100,
    `only ${responsive} responsive rules were found; the scan is not reading the media blocks`,
  );
  const orphans = orphanedResponsiveRules(ownership);
  assert.deepEqual(
    orphans,
    [],
    "A @media rule overrides base rules that live in another file. The two halves of one adaptation are\n"
      + "then two files apart, which is exactly the document-wide responsive appendix this phase dissolved:\n"
      + "editing the base rule shows no sign that a breakpoint restates it. Move the block to the file that\n"
      + `owns the base rules, or move the base rules here:\n${describeList(orphans)}`,
  );
});

test("app/styles/interaction.css still names classes other files base, so its exemption is live", async () => {
  // The exemption `orphanedResponsiveRules` grants this file is the only one in
  // the responsive-ownership check, and an exemption nobody can see the effect
  // of is an exemption nobody reviews. Its load position is pinned by the
  // cascade-order test above; this is the other half, and the two fail for
  // different reasons.
  const ownership = await collectResponsiveOwnership(workspace);
  const crossCutting = ownership.files.find((entry) => entry.file === CROSS_CUTTING_STYLESHEET);
  const reaching = crossCutting.responsive.filter((rule) => (
    rule.classes.length > 0
    && !rule.classes.some((className) => crossCutting.base.has(className))
    && rule.classes.every((className) => ownership.owners.has(className))
  ));
  assert.ok(
    reaching.length > 0,
    `${CROSS_CUTTING_STYLESHEET} no longer names a class any other file bases, so it is not cross-cutting`
      + " any more and the exemption the responsive-ownership check grants it is dead. Either its rules"
      + " have moved to the features they apply to -- in which case delete the exemption -- or the scan"
      + " has stopped seeing them.",
  );
});

test("every run of media queries follows the documented order", async () => {
  const perFile = await collectMediaQueryRuns(workspace);
  const runs = perFile.reduce((total, entry) => total + entry.runs.length, 0);
  assert.ok(runs > 10, `only ${runs} runs of consecutive media queries were found; the scan is not reading the layer`);
  const inverted = [];
  for (const entry of perFile) {
    for (const run of entry.runs) {
      for (let index = 1; index < run.length; index += 1) {
        if (mediaQueryRank(run[index]) < mediaQueryRank(run[index - 1])) {
          inverted.push(`${entry.file}: (${run[index - 1]}) is stated before (${run[index]})`);
        }
      }
    }
  }
  assert.deepEqual(
    inverted.toSorted(),
    [],
    "docs/theming.md: a section's media blocks run largest width first, then (pointer: coarse), then\n"
      + "(prefers-reduced-motion: reduce). Every width query is a max-width, so a narrower one stated first\n"
      + "is overridden by the wider one that follows and its rules never apply; and a tap target set at a\n"
      + `width beats the coarse-pointer minimum that was meant to outrank it:\n${describeList(inverted.toSorted())}`,
  );
});

/* == 7. One implementation of each primitive =================================== */

/**
 * The primitives Phase 3 consolidated, and what each one replaced.
 *
 * The count is the point: every one of these names was, before the phase, two
 * or more rules describing the same thing. Adding a name here is a claim that
 * the product has exactly one of it.
 */
const CONSOLIDATED_PRIMITIVES = {
  badge: "eight chips: .mode-pill, .role-pill, .severity-badge, two .previewBadge module rules, .readOnlyBadge, .liveBadge/.partialBadge, .availableBadge/.unavailableBadge",
  button: "three vocabularies: .button, .suite-button, .icon-button/.close-button",
  drawer: "two mobile drawers: .suite-mobile-drawer and .search-mobile-drawer",
  "drawer-backdrop": "three identical scrims: .suite-mobile-backdrop, .search-mobile-backdrop, .time-picker-mobile-backdrop",
  "modal-card": "the one dialog surface, after Modal moved to app/_components",
  status: "eight families: .status-icon, .status-dot, .status-label, .mini-status, .job-state-icon, .job-card-state, .inspector-state, and reports' .status/.runStatus",
  table: "five tables: .product-table and four card-mode reimplementations",
  "table-wrap": "two scroll containers: .responsive-table-wrap and .product-table's own wrapper",
  wordmark: "three blocks and four markup copies of the product wordmark",
};

/** Animations more than one family used to declare a private copy of. */
const SHARED_ANIMATIONS = {
  "pulse-ring": "folded from .pulse, .status-pulse and .backend-preview-pulse",
  spin: "folded from .app-icon-spin, .spinner and .backend-state-spin",
};

/** Keyframe blocks the fold above deleted; none may come back under its old name. */
const RETIRED_KEYFRAMES = [
  "app-icon-spin",
  "backend-preview-pulse",
  "backend-state-spin",
  "pulse",
  "spinner",
  "status-pulse",
];

/** Two rules stating this many declarations identically is a restatement, not a coincidence. */
const DUPLICATE_DECLARATION_THRESHOLD = 4;

/** Groups rules in the same at-rule context by what they declare. */
async function duplicateDeclarationGroups(root, minimum) {
  const groups = new Map();
  for (const block of await collectDeclarationBlocks(root, minimum)) {
    const declarations = declarationSignature(block.declarations);
    const key = JSON.stringify({ ancestors: block.ancestors, declarations });
    const group = groups.get(key) ?? { declarations, sites: [] };
    group.sites.push(describeRuleSite(block));
    groups.set(key, group);
  }
  return [...groups.values()]
    .filter((group) => group.sites.length > 1)
    .map((group) => ({ declarations: group.declarations, sites: group.sites.toSorted() }))
    .toSorted((left, right) => left.sites[0].localeCompare(right.sites[0]));
}

test("every consolidated primitive has exactly one base rule", async () => {
  const sites = await collectBaseRuleSites(workspace, Object.keys(CONSOLIDATED_PRIMITIVES));
  const problems = [];
  for (const [name, replaced] of Object.entries(CONSOLIDATED_PRIMITIVES).toSorted()) {
    // An at-rule ancestor makes a rule a responsive or theme override of the
    // base, not a second base: it can only apply where the base already does.
    const bases = sites.get(name).filter((site) => site.ancestors.length === 0);
    if (bases.length === 1) continue;
    problems.push(
      bases.length === 0
        ? `.${name} has no unconditional base rule at all, so ${replaced} were folded into nothing`
        : `.${name} has ${bases.length} base rules, which is ${bases.length} implementations of one primitive:\n`
          + bases.map((site) => `      ${site.file} :: ${site.prelude}`).join("\n"),
    );
  }
  assert.deepEqual(
    problems,
    [],
    "A primitive is defined more than once. Whichever rule loses the cascade is dead weight, and\n"
      + "whichever wins depends on file order rather than on intent -- which is the state Phase 3\n"
      + `removed. Fold the extra rules into the one base:\n${describeList(problems)}`,
  );
});

test("every shared animation is defined exactly once and its retired names stay gone", async () => {
  const sites = await collectKeyframeSites(workspace);
  const problems = [];
  for (const [name, folded] of Object.entries(SHARED_ANIMATIONS).toSorted()) {
    const defined = sites.filter((site) => site.name === name);
    if (defined.length === 1) continue;
    problems.push(
      `@keyframes ${name} (${folded}) is defined ${defined.length} times`
      + (defined.length === 0 ? "" : `: ${defined.map((site) => site.file).join(", ")}`),
    );
  }
  for (const name of RETIRED_KEYFRAMES) {
    const defined = sites.filter((site) => site.name === name);
    if (defined.length > 0) {
      problems.push(`@keyframes ${name} is back in ${defined.map((site) => site.file).join(", ")}`);
    }
  }
  assert.deepEqual(
    problems,
    [],
    "Two copies of one animation is the defect this phase removed, and CSS reports it as nothing:\n"
      + `the later block simply wins.\n${describeList(problems)}`,
  );
});

test("every animation a rule plays names a keyframe block that exists", async () => {
  const defined = new Set((await collectKeyframeSites(workspace)).map((site) => site.name));
  const dangling = (await collectAnimationReferences(workspace))
    .filter((reference) => !defined.has(reference.name))
    .map((reference) => `${reference.file} :: ${reference.selector} plays ${reference.name}`)
    .toSorted();
  assert.deepEqual(
    dangling,
    [],
    "A rule asks for a keyframe block nothing declares, which renders as no animation at all and\n"
      + `raises no error anywhere. This is how folding duplicate keyframes goes wrong:\n${describeList(dangling)}`,
  );
});

test("no two rules state the same four or more declarations", async () => {
  const duplicates = (await duplicateDeclarationGroups(workspace, DUPLICATE_DECLARATION_THRESHOLD))
    .map((group) => `${group.declarations.length} declarations restated by:\n`
      + group.sites.map((site) => `      ${site}`).join("\n")
      + `\n      { ${group.declarations.join("; ")} }`);
  assert.deepEqual(
    duplicates,
    [],
    "Two rules describe the same thing in the same words. Either one of them should be using the\n"
      + "other -- a primitive, a modifier or a shared selector list. The detector is strict: there is\n"
      + `no duplicate-block exemption ledger:\n${describeList(duplicates)}`,
  );
});

/* == 8. Reachability, in both directions ======================================= */

const allowlistPath = path.join(workspace, "scripts", "css-dynamic-classes.json");

/** Reads the allowlist of classes that only ever exist at runtime. */
async function readDynamicClassAllowlist() {
  const parsed = JSON.parse(await readFile(allowlistPath, "utf8"));
  return { classes: new Set(parsed.classes ?? []), prefixes: new Set(parsed.prefixes ?? []) };
}

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

/**
 * The namespaces the colocated feature stylesheets own.
 *
 * One prefix per file under `app/` that is not the shared layer. They are
 * listed rather than derived because the list is the claim: another feature
 * stylesheet has to be added here for its classes to be checked at all, and
 * that is a deliberate one-line edit rather than something a glob does quietly.
 */
const FEATURE_PREFIXES = [
  "alerts-",
  "analytics-",
  "operations-",
  "reports-",
  "visualization-",
  "workspace-dialog-",
];

/** This file, which quotes retired names and fake imports as parser fixtures. */
const SELF = "scripts/style-invariants.test.mjs";

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
    // the last section of this file do -- so a fixture is not a call site. The
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

test("every feature class the markup writes is defined by its colocated stylesheet", async () => {
  // The invariant this replaces read `styles.x` against the CSS module the
  // `styles` binding came from: a module resolved to a plain object, so a read
  // of a class the module no longer had yielded `undefined`, and the string
  // "undefined" reached the class attribute. Phase 4 deleted the modules, and
  // with them that failure -- and, if nothing took its place, the coverage too.
  // A feature class is an ordinary string now, and a rename that misses one call
  // site renders an unstyled element with no error anywhere in the toolchain.
  //
  // FEATURE_PREFIXES is exactly the set of namespaces the colocated stylesheets
  // own, so the scan asks one answerable question -- is every `analytics-…`,
  // `operations-…`, `reports-…`, `visualization-…` and `workspace-dialog-…`
  // class the product writes actually defined? -- rather than trying to decide
  // whether an arbitrary word in a class attribute was meant to be a class.
  const styled = await collectStyledClasses(workspace);
  const offenders = [];
  for (const { relative, source } of await callSites()) {
    // Attribute positions only, the same scan the retired-class check uses: a
    // feature's prefix also spells element ids (`reports-rename-name-error`)
    // and module names (`analytics-data`), and neither is a class.
    const written = collectClassAttributeTokens(source);
    const bases = collectSourceClassEvidence(source).interpolationPrefixes;
    for (const name of [...written].toSorted()) {
      if (!FEATURE_PREFIXES.some((prefix) => name.startsWith(prefix))) continue;
      if (styled.has(name)) continue;
      // A template literal also contributes its base -- `analytics-severity--`
      // from `` `analytics-severity--${severity}` `` -- which is not itself a
      // class. The completed names are checked wherever they are written out.
      if (bases.has(name)) continue;
      offenders.push(`${relative} renders "${name}", which no stylesheet defines`);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    "Markup asks for a feature class no rule matches. Nothing in the toolchain reports an\n"
      + "unmatched class, so the element renders with no styling at all until somebody looks at\n"
      + `it:\n${describeList(offenders)}`,
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

/* == 9. The parsers underneath ================================================= */

// Every invariant above is worth exactly as much as the parsing beneath it
// can see, and each of these pins `scripts/style-inventory.mjs` against a shape
// that has already made a simpler implementation either miss a violation or
// invent one. They are gathered at the end rather than trailing each section
// because they test one library, not nine concerns.

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

test("cssBlocks records nesting so a themed block knows its at-rule", () => {
  const blocks = cssBlocks(`
    :root { --a: 1px; }
    @media (prefers-color-scheme: dark) {
      :root:not([data-theme="light"]) { --a: 2px; }
    }
    /* :root { --commented: 3px; } */
  `);
  const roots = blocks.filter((block) => block.prelude.startsWith(":root"));
  assert.deepEqual(roots.map((block) => isDarkThemeContext(block)), [false, true]);
  assert.deepEqual(
    roots.flatMap((block) => cssDeclarations(block.body).map((declaration) => declaration.value)),
    ["1px", "2px"],
  );
});

/** `themeScopeOf` for an unnested block, which is the shape every theme block has. */
function scopeOf(prelude) {
  return themeScopeOf({ ancestors: [], prelude });
}

test("themeScopeOf accepts exactly the four theme preludes", () => {
  assert.deepEqual(scopeOf(":root"), { mode: "light", palette: null });
  assert.deepEqual(scopeOf(':root[data-theme="dark"]'), { mode: "dark", palette: null });
  assert.deepEqual(scopeOf(':root:where([data-palette="ocean"])'), { mode: "light", palette: "ocean" });
  assert.deepEqual(
    scopeOf(':root[data-palette="ocean"][data-theme="dark"]'),
    { mode: "dark", palette: "ocean" },
  );
});

test("themeScopeOf rejects a palette light block without :where(), and a nested block", () => {
  // `:root[data-palette="x"]` is (0,2,0): it ties with the base dark block and,
  // being loaded after it, would paint the palette's light grounds under
  // dark mode. Only the `:where()` spelling keeps the palette light block at
  // the base light block's specificity.
  assert.equal(scopeOf(':root[data-palette="ocean"]'), null);
  // Nested inside an at-rule the block is conditional, and the four shapes
  // are unconditional by definition; the base dark block reached through a
  // `prefers-color-scheme` query is exactly what the boot script exists to
  // avoid.
  assert.equal(
    themeScopeOf({ ancestors: ["@media (prefers-color-scheme: dark)"], prelude: ':root[data-theme="dark"]' }),
    null,
  );
  // Neither a rule nor a spelling that merely contains the attribute.
  assert.equal(scopeOf(".dark-row"), null);
  assert.equal(scopeOf(':root[data-theme="dark"] .card'), null);
  assert.equal(scopeOf(':root:where([data-palette="ocean"])[data-theme="dark"]'), null);
});

/** An unnested token-file block as `collectTokenBlocks` would report it. */
function unnestedBlock(prelude, declarations = [{ property: "--radius-sm", value: "0" }]) {
  return { ancestors: [], declarations, prelude };
}

test("tokenBlockProblems refuses a block that selects the default palette, in either mode", () => {
  // Fixture-free: the rule is stated here so a classic file is refused by
  // name, not left to whichever other invariant its contents happen to trip.
  const palettes = new Set([DEFAULT_PALETTE, "ocean"]);
  const classicFile = paletteFile(DEFAULT_PALETTE);
  const colourFile = "app/styles/tokens-color.css";
  const classicDark = `:root[data-palette="${DEFAULT_PALETTE}"][data-theme="dark"]`;
  const classicLight = `:root:where([data-palette="${DEFAULT_PALETTE}"])`;
  const refused = `selects data-palette="${DEFAULT_PALETTE}", which is the base pair and has no block of its own`;
  assert.deepEqual(
    tokenBlockProblems(classicFile, unnestedBlock(classicDark), palettes),
    [`${classicFile}: ${classicDark} ${refused}`],
  );
  assert.deepEqual(
    tokenBlockProblems(classicFile, unnestedBlock(classicLight), palettes),
    [`${classicFile}: ${classicLight} ${refused}`],
  );
  // The refusal is about the palette, not the file: the same block in the
  // colour file is refused for the same reason, before file ownership is asked.
  assert.deepEqual(
    tokenBlockProblems(colourFile, unnestedBlock(classicLight), palettes),
    [`${colourFile}: ${classicLight} ${refused}`],
  );
  // The four legitimate shapes, each in its own file, raise nothing.
  assert.deepEqual(tokenBlockProblems(colourFile, unnestedBlock(":root"), palettes), []);
  assert.deepEqual(tokenBlockProblems("app/styles/tokens-scale.css", unnestedBlock(':root[data-theme="dark"]'), palettes), []);
  assert.deepEqual(tokenBlockProblems(paletteFile("ocean"), unnestedBlock(':root:where([data-palette="ocean"])'), palettes), []);
  assert.deepEqual(
    tokenBlockProblems(paletteFile("ocean"), unnestedBlock(':root[data-palette="ocean"][data-theme="dark"]'), palettes),
    [],
  );
  // And the checks it sits beside still fire: an unlisted palette, a block in
  // the wrong file, a nested block, and a rule that is not a theme block.
  assert.deepEqual(
    tokenBlockProblems(paletteFile("sepia"), unnestedBlock(':root:where([data-palette="sepia"])'), palettes),
    [`${paletteFile("sepia")}: :root:where([data-palette="sepia"]) selects a palette ${PALETTE_MODULE} does not list in PALETTES`],
  );
  assert.deepEqual(
    tokenBlockProblems(colourFile, unnestedBlock(':root:where([data-palette="ocean"])'), palettes),
    [`${colourFile}: :root:where([data-palette="ocean"]) is the ocean light block, and this file holds only base blocks`],
  );
  assert.deepEqual(
    tokenBlockProblems(
      colourFile,
      { ancestors: ["@media (prefers-color-scheme: dark)"], declarations: [], prelude: ':root[data-theme="dark"]' },
      palettes,
    ),
    [`${colourFile}: :root[data-theme="dark"] is nested inside @media (prefers-color-scheme: dark)`],
  );
  assert.deepEqual(
    tokenBlockProblems(colourFile, unnestedBlock(".card", [{ property: "color", value: "red" }]), palettes),
    [`${colourFile}: .card is a rule, not one of the four theme blocks`, `${colourFile}: .card sets color`],
  );
});

test("cssDeclarations keeps a value that contains its own colons and commas", () => {
  const declarations = cssDeclarations(
    '--font-mono: "SFMono-Regular", Consolas, monospace;\n'
    + "--shadow-lg: 0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%);\n"
    + "--grid: url(https://example.test/a.png)",
  );
  assert.deepEqual(declarations.map((declaration) => declaration.property), ["--font-mono", "--shadow-lg", "--grid"]);
  assert.equal(declarations[2].value, "url(https://example.test/a.png)");
});

test("cssDeclarations ignores the declarations of a nested rule", () => {
  const blocks = cssBlocks(":root { --a: 1px; .nested { --b: 2px; } --c: 3px; }");
  const root = blocks.find((block) => block.prelude === ":root");
  assert.deepEqual(
    cssDeclarations(root.body).map((declaration) => declaration.property),
    ["--a", "--c"],
  );
});

test("collectStylesheetClasses ignores commented-out rules and value-position dots", () => {
  const classes = collectStylesheetClasses(`
    .live { margin: 1.5rem; }
    /* .retired { color: red; } */
    @media (max-width: 480px) { .narrow { gap: 0.25rem; } }
  `);
  assert.deepEqual([...classes].toSorted(), ["live", "narrow"]);
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

test("universal spacing misses exclude semantic component geometry", () => {
  const steps = universalSpacingSteps(new Map([
    ["--space-1", "4px"],
    ["--space-2", "8px"],
    ["--space-button-toolbar-gap", "5px"],
    ["--space-statistics-cell-maximum", "420px"],
  ]));
  assert.deepEqual([...steps], [["4px", "--space-1"], ["8px", "--space-2"]]);
});

test("collectDeclarationComments pairs a comment with the declaration it trails", () => {
  const found = collectDeclarationComments([
    ":root {",
    "  --gray-0: #ffffff; /* Paper white. */",
    "  --bare: #000000;",
    "  --shadow-lg: 0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%); /* Deep lift. */",
    "  /* A banner above the next token, which is not that token's own comment. */",
    "  --after-banner: 1px;",
    "}",
  ].join("\n"));
  assert.deepEqual(found, [
    { comment: "Paper white.", name: "--gray-0" },
    { comment: null, name: "--bare" },
    { comment: "Deep lift.", name: "--shadow-lg" },
    { comment: null, name: "--after-banner" },
  ]);
});

test("collectDeclarationComments reports a name once per declaration site", () => {
  const found = collectDeclarationComments(
    ':root { --focus-ring: var(--blue-450); /* Light. */ }\n'
    + ':root[data-theme="dark"] { --focus-ring: var(--blue-300); /* Dark. */ }',
  );
  assert.deepEqual(found.map(({ comment }) => comment), ["Light.", "Dark."]);
});

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

test("mediaQueryRank sorts widths largest first and puts the pointer queries after them", () => {
  const canon = [
    "(max-width: 1240px)",
    "(max-width: 980px)",
    "(max-width: 760px)",
    "(max-width: 480px)",
    "(max-height: 650px) and (max-width: 760px)",
    "(pointer: coarse)",
    "(pointer: coarse) and (min-width: 761px)",
    "(prefers-reduced-motion: reduce)",
  ];
  const ranks = canon.map((query) => mediaQueryRank(query));
  assert.deepEqual(ranks.toSorted((left, right) => left - right), ranks);
  assert.ok(mediaQueryRank("(max-width: 980px)") < mediaQueryRank("(max-width: 760px)"));
});

test("declarationSignature is order-independent and keeps property and value together", () => {
  assert.deepEqual(
    declarationSignature(cssDeclarations("gap: 4px; color: red;")),
    declarationSignature(cssDeclarations("color: red; gap: 4px;")),
  );
  assert.notDeepEqual(
    declarationSignature(cssDeclarations("gap: 4px; color: red;")),
    declarationSignature(cssDeclarations("gap: red; color: 4px;")),
  );
});

test("collectDeclarationBlocks counts a rule's own declarations, not a nested rule's", async () => {
  const blocks = await collectDeclarationBlocks(workspace, 1);
  assert.ok(blocks.length > 100, `only ${blocks.length} rules were found; the walk is broken`);
  assert.ok(
    blocks.every((block) => !block.prelude.startsWith("@")),
    "an at-rule prelude was counted as a selector, so @keyframes steps would be compared as rules",
  );
  assert.ok(
    blocks.every((block) => block.declarations.every(({ property }) => !property.includes("{"))),
    "a nested rule's selector was parsed as a declaration property",
  );
});

test("describeRuleSite names the file, the at-rules around a rule, and its selector list", () => {
  assert.equal(
    describeRuleSite({
      ancestors: ["@media (max-width: 760px)"],
      file: "app/styles/primitives/table.css",
      prelude: ".table, .other",
    }),
    "app/styles/primitives/table.css :: @media (max-width: 760px) :: .table, .other",
  );
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
  // Composed inside the call. Reading the argument up to the first comma saw
  // only `path.join(process` and reported nothing, which is how a read of
  // app/styles/primitives/table.css sat in app/activity/backend-audit-data.test.ts
  // with this invariant green.
  assert.deepEqual(
    findStylesheetTextReads(
      'const css = readFileSync(path.join(process.cwd(), "app", "styles", "base.css"), "utf8");',
    ),
    ['readFileSync(path.join(process.cwd(), "app", "styles", "base.css"))'],
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

test("findComposedStylesheetReads follows a path built inside the call, and ignores a fixture", () => {
  assert.deepEqual(
    findComposedStylesheetReads('const css = readFileSync(path.join(root, "app", "styles", "base.css"), "utf8");'),
    ['readFileSync(path.join(root, "app", "styles", "base.css"), "utf8")'],
  );
  // This file's own pins embed a read as data. Masking removes the call's
  // parentheses inside a literal, so quoted source can never be read as a call
  // -- which is what keeps the invariant fixable rather than self-triggering.
  assert.deepEqual(
    findComposedStylesheetReads(String.raw`const fixture = 'readFileSync(path.join(a, "b.css"))';`),
    [],
  );
  // Handing a path to the browser is not a read: Playwright loads the file and
  // the assertion lands on computed style, which is the shape this asks for.
  assert.deepEqual(
    findComposedStylesheetReads('await page.addStyleTag({ path: path.join(root, "app", "styles", "base.css") });'),
    [],
  );
});

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
      'import Link from "next/link";',
      // A stylesheet reaches the bundle through a side-effect import, which
      // names no binding and so has no `from` to read a specifier out of.
      'import "./styles/index.css";',
      'const text = \'import { Modal } from "./fake";\';',
    ].join("\n")),
    ["../../_components/modal", "next/link"],
  );
});

test("stripCssComments and collectStyledClasses agree on what the layer defines", async () => {
  // Until Phase 4 this asserted the opposite shape: a module's own class was
  // scoped to a generated identifier, so it had to be kept *out* of the set
  // while its `:global(...)` selectors were kept in. With one styling lane the
  // question is simply whether the collector reaches every stylesheet, because
  // the retired-class and harness-selector invariants in section 9 are only as
  // wide as the set it returns.
  const styled = await collectStyledClasses(workspace);
  assert.ok(styled.has("table"), "the global .table primitive was not collected");
  assert.ok(
    styled.has("analytics-trend-line"),
    "a colocated feature stylesheet was not collected, so every invariant reading this set has\n"
      + "stopped covering the rules Phase 4 moved out of the CSS modules",
  );
  assert.equal(stripCssComments("/* .ghost { color: red; } */ .live {}").includes("ghost"), false);
});

/* == 8. The palette invariants against synthetic files ======================== */
//
// Every check above reads the workspace, which today holds no palette file: a
// generalised invariant can pass by having nothing to refuse. This section
// hands each one a synthetic layer -- a base pair small enough to read, plus a
// palette file written wrong in exactly one way -- and asserts the invariant
// names the fault. The fixtures never touch the disk, so this file stays a
// test file that opens no stylesheet.

/** A colour file: the primitives and the roles the mandated pairs and role groups read. */
const SYNTHETIC_COLOUR = `
:root {
  color-scheme: light;
  --gray-0: #ffffff;
  --gray-50: #f7f7f7;
  --gray-100: #f0f0f0;
  --gray-150: #e8e8e8;
  --gray-300: #bbbbbb;
  --gray-600: #666666;
  --gray-700: #444444;
  --gray-800: #2a2a2a;
  --gray-900: #1a1a1a;
  --gray-950: #0d0d0d;
  --blue-300: #7aa7e0;
  --blue-400: #4a86d8;
  --blue-500: #1a5fb4;
  --blue-700: #124a8f;
  --bg-canvas: var(--gray-50);
  --bg-surface: var(--gray-0);
  --bg-subtle: var(--gray-100);
  --bg-raised: var(--gray-150);
  --bg-inverse: var(--gray-900);
  --fg-text: var(--gray-700);
  --fg-strong: var(--gray-950);
  --fg-inverse: var(--gray-0);
  --chrome-bar: var(--gray-900);
  --chrome-appbar: var(--gray-800);
  --chrome-hover: var(--gray-700);
  --chrome-fg: var(--gray-0);
  --accent: var(--blue-500);
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --bg-canvas: var(--gray-950);
  --bg-surface: var(--gray-900);
  --bg-subtle: var(--gray-800);
  --bg-raised: var(--gray-700);
  --bg-inverse: var(--gray-0);
  --fg-text: var(--gray-100);
  --fg-strong: var(--gray-0);
  --fg-inverse: var(--gray-950);
  --accent: var(--blue-700);
}
`;

/** A scale file: the two knob families and one member each of two scale families. */
const SYNTHETIC_SCALE = `
:root {
  --alpha-chrome: 100%;
  --alpha-surface: 100%;
  --backdrop-surface: none;
  --radius-sm: 4px;
  --shadow-md: 0 3px 9px rgb(21 35 43 / 24%);
}
`;

const COLOUR_FILE = "app/styles/tokens-color.css";
const SCALE_FILE = "app/styles/tokens-scale.css";

/** The names `PALETTES` lists in every fixture below. */
const SYNTHETIC_PALETTES = new Set([DEFAULT_PALETTE, "ocean", "graphite"]);

/** The `PALETTE_CONTRAST_FLOOR` table every fixture below is held to: graphite promises 7:1. */
const SYNTHETIC_FLOORS = { graphite: 7 };

/** A palette file with one light block and one dark block holding the given declarations. */
function paletteSource(name, light, dark) {
  return `:root:where([data-palette="${name}"]) {\n${light}\n}\n`
    + `:root[data-palette="${name}"][data-theme="dark"] {\n${dark}\n}\n`;
}

/** The base pair plus whatever palette files are handed in, as `collectTokenLayer` would report them. */
function syntheticLayer(paletteFiles = {}) {
  return Object.entries({ [COLOUR_FILE]: SYNTHETIC_COLOUR, [SCALE_FILE]: SYNTHETIC_SCALE, ...paletteFiles })
    .map(([file, css]) => tokenLayerOfSource(file, css));
}

/** The same files as `collectTokenBlocks` would report them. */
function syntheticBlocks(paletteFiles = {}) {
  return Object.entries({ [COLOUR_FILE]: SYNTHETIC_COLOUR, [SCALE_FILE]: SYNTHETIC_SCALE, ...paletteFiles })
    .map(([file, css]) => tokenBlocksOfSource(file, css));
}

/** Every problem "the token layer declares tokens and nothing else" would report for these files. */
function syntheticBlockProblems(paletteFiles) {
  return syntheticBlocks(paletteFiles)
    .flatMap(({ blocks, file }) => blocks.flatMap((block) => tokenBlockProblems(file, block, SYNTHETIC_PALETTES)))
    .toSorted();
}

/** A well-formed ocean palette: one colour and one scale restatement per mode, every one a change. */
const OCEAN = paletteFile("ocean");
const OCEAN_LIGHT = "  color-scheme: light;\n  --accent: var(--blue-300);\n  --radius-sm: 6px;";
const OCEAN_DARK = "  color-scheme: dark;\n  --accent: var(--blue-400);";

test("the synthetic base pair and a well-formed palette pass every palette invariant", () => {
  // The control: if the fixture itself tripped a check, every failure below
  // would be proving nothing about the input it claims to refuse.
  const files = { [OCEAN]: paletteSource("ocean", OCEAN_LIGHT, OCEAN_DARK) };
  const layer = syntheticLayer(files);
  const indexed = indexTokenLayer(layer);
  assert.deepEqual(syntheticBlockProblems(files), []);
  assert.deepEqual(declarationSiteProblems(layer), { duplicated: [], literals: [] });
  assert.deepEqual(inventedNames(layer), []);
  assert.deepEqual(paletteLedgerProblems([...SYNTHETIC_PALETTES].filter((name) => name !== "graphite"), layer), []);
  assert.deepEqual(colourSchemeProblems(syntheticBlocks(files), indexed.primitives), []);
  assert.deepEqual(roleGroupCollisions(indexed), []);
  assert.deepEqual(inertRestatements(indexed), []);
  assert.deepEqual(contrastFailures(indexed), []);
  assert.deepEqual(alphaKnobProblems(indexed), []);
  assert.deepEqual(familyKindMismatches(indexed), []);
  assert.deepEqual(indexed.scopes.map((scope) => scope.label), ["classic light", "classic dark", "ocean light", "ocean dark"]);
});

test("the four scopes resolve a name set in all four blocks in cascade order", () => {
  // `--accent` is set in every block: base light blue-500, ocean light
  // blue-300, base dark blue-700, ocean dark blue-400. Each corner has to land
  // on its own block's value, and ocean dark in particular must not see ocean
  // light's value through the base dark block that sits between them.
  const indexed = indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource(
      "ocean",
      "  color-scheme: light;\n  --accent: var(--blue-300);\n  --bg-canvas: var(--gray-100);",
      "  color-scheme: dark;\n  --accent: var(--blue-400);",
    ),
  }));
  const byLabel = new Map(indexed.scopes.map((scope) => [scope.label, scope.values]));
  assert.deepEqual(
    [...byLabel].map(([label, values]) => `${label} ${resolve("--accent", values)}`),
    ["classic light #1a5fb4", "classic dark #124a8f", "ocean light #7aa7e0", "ocean dark #4a86d8"],
  );
  // A palette light restatement that base dark also restates: base dark wins
  // in the palette's dark scope, because the `:where()` block loses to it.
  assert.equal(resolve("--bg-canvas", byLabel.get("ocean light")), "#f0f0f0");
  assert.equal(resolve("--bg-canvas", byLabel.get("ocean dark")), "#0d0d0d");
  // And a palette light restatement base dark leaves alone carries into the dark scope.
  assert.equal(resolve("--radius-sm", byLabel.get("classic dark")), "4px");
  assert.deepEqual(indexed.scopes.map((scope) => scope.chain.length), [1, 2, 2, 4]);
});

test("a palette light block without :where() is refused as not a theme block, and its values never index", () => {
  const files = {
    [OCEAN]: `:root[data-palette="ocean"] {\n${OCEAN_LIGHT}\n}\n:root[data-palette="ocean"][data-theme="dark"] {\n${OCEAN_DARK}\n}\n`,
  };
  assert.deepEqual(syntheticBlockProblems(files), [
    `${OCEAN}: :root[data-palette="ocean"] is a rule, not one of the four theme blocks`,
  ]);
  const layer = syntheticLayer(files);
  assert.deepEqual(paletteLedgerProblems(["classic", "ocean"], layer), [
    `${OCEAN} holds [not a theme block (:root[data-palette="ocean"]), ocean dark] and must hold exactly [ocean light, ocean dark]`,
  ]);
  // The block is not filed as ocean light either: the scope resolves to base.
  const indexed = indexTokenLayer(layer);
  const light = indexed.scopes.find((scope) => scope.label === "ocean light");
  assert.equal(resolve("--accent", light.values), "#1a5fb4");
  assert.equal(resolve("--radius-sm", light.values), "4px");
});

test("a palette block nested in an at-rule is refused and never indexed", () => {
  const files = {
    [OCEAN]: `@media (max-width: 760px) {\n:root:where([data-palette="ocean"]) {\n${OCEAN_LIGHT}\n}\n}\n`
      + `:root[data-palette="ocean"][data-theme="dark"] {\n${OCEAN_DARK}\n}\n`,
  };
  // Both the nested block and the at-rule wrapping it are refused: the wrapper
  // is a block in a token file that is not one of the four shapes.
  assert.deepEqual(syntheticBlockProblems(files), [
    `${OCEAN}: :root:where([data-palette="ocean"]) is nested inside @media (max-width: 760px)`,
    `${OCEAN}: @media (max-width: 760px) is a rule, not one of the four theme blocks`,
  ]);
  const layer = syntheticLayer(files);
  assert.deepEqual(paletteLedgerProblems(["classic", "ocean"], layer), [
    `${OCEAN} holds [not a theme block (:root:where([data-palette="ocean"])), ocean dark] and must hold exactly [ocean light, ocean dark]`,
  ]);
  const light = indexTokenLayer(layer).scopes.find((scope) => scope.label === "ocean light");
  assert.equal(resolve("--radius-sm", light.values), "4px");
});

test("themeScopeOf refuses every near-miss spelling of the four preludes", () => {
  for (const prelude of [
    ":root:where([data-palette=\"Ocean\"])",
    ":root:where([data-palette='ocean'])",
    ":root:where([data-palette=ocean])",
    ":root :where([data-palette=\"ocean\"])",
    ":root:where([data-palette=\"ocean\"]) ",
    ":root:is([data-palette=\"ocean\"])",
    ":root[data-palette=\"ocean\"]:where([data-theme=\"dark\"])",
    ":root[data-theme=\"dark\"][data-palette=\"ocean\"]",
    ":root[data-theme=\"dark\"]:where([data-palette=\"ocean\"])",
    ":root[data-theme=\"light\"]",
    "html",
    ":root, :root[data-theme=\"dark\"]",
  ]) {
    const scope = themeScopeOf({ ancestors: [], prelude });
    // A trailing space is the one difference `themeScopeOf` forgives, because
    // `cssBlocks` trims preludes before it ever sees one.
    if (prelude.endsWith(" ")) assert.deepEqual(scope, { mode: "light", palette: "ocean" });
    else assert.equal(scope, null, `${prelude} was accepted as ${JSON.stringify(scope)}`);
  }
});

test("a palette block that declares a colour literal of any spelling is refused", () => {
  // A hex is the obvious primitive; `rgb()` is the one the token-file stylelint
  // exemption lets through, so a palette could grow a primitive that way and
  // no hue check would ever see it.
  for (const literal of ["#ff0000", "rgb(255 0 0)", "hsl(0 100% 50%)", "oklch(0.6 0.2 30)"]) {
    const layer = syntheticLayer({
      [OCEAN]: paletteSource("ocean", `  color-scheme: light;\n  --accent: ${literal};`, OCEAN_DARK),
    });
    assert.deepEqual(declarationSiteProblems(layer).literals, [
      `${OCEAN} (:root:where([data-palette="ocean"])): --accent: ${literal}`,
    ]);
  }
  // A new primitive in a palette is both a literal and an invented name.
  const layer = syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  color-scheme: light;\n  --teal-500: #008080;\n  --accent: var(--teal-500);", OCEAN_DARK),
  });
  assert.deepEqual(declarationSiteProblems(layer).literals, [
    `${OCEAN} (:root:where([data-palette="ocean"])): --teal-500: #008080`,
  ]);
  assert.deepEqual(inventedNames(layer), [`${OCEAN}: --teal-500 (:root:where([data-palette="ocean"]))`]);
  // The base dark block is held to the same rule as a palette.
  const darkHex = syntheticLayer();
  darkHex[0].blocks[1].tokens.push({ name: "--accent", value: "#123456" });
  assert.deepEqual(declarationSiteProblems(darkHex).literals, [`${COLOUR_FILE} (:root[data-theme="dark"]): --accent: #123456`]);
  // A shadow's ink is not a colour token: the scale tier writes those and a
  // palette may restate a shadow, so a ring with an rgb() ink passes.
  const ring = syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  --shadow-md: 0 0 0 1px rgb(0 0 0 / 40%);", OCEAN_DARK),
  });
  assert.deepEqual(declarationSiteProblems(ring).literals, []);
});

test("a palette block that restates a name the base never declares is refused", () => {
  const layer = syntheticLayer({
    [OCEAN]: paletteSource("ocean", `${OCEAN_LIGHT}\n  --fg-brand: var(--blue-500);`, `${OCEAN_DARK}\n  --radius-pill: 999px;`),
  });
  assert.deepEqual(inventedNames(layer), [
    `${OCEAN}: --fg-brand (:root:where([data-palette="ocean"]))`,
    `${OCEAN}: --radius-pill (:root[data-palette="ocean"][data-theme="dark"])`,
  ]);
});

test("a name declared twice in one palette scope is refused, and the same name in two scopes is not", () => {
  const twice = syntheticLayer({
    [OCEAN]: paletteSource("ocean", `${OCEAN_LIGHT}\n  --accent: var(--blue-400);`, OCEAN_DARK),
  });
  assert.deepEqual(declarationSiteProblems(twice).duplicated, [
    `ocean light --accent declared in ${OCEAN} (:root:where([data-palette="ocean"])) and ${OCEAN} (:root:where([data-palette="ocean"]))`,
  ]);
  // Two light blocks for one palette: the duplicate is per scope, not per block.
  const split = syntheticLayer({
    [OCEAN]: `${paletteSource("ocean", OCEAN_LIGHT, OCEAN_DARK)}:root:where([data-palette="ocean"]) {\n  --accent: var(--blue-400);\n}\n`,
  });
  assert.deepEqual(declarationSiteProblems(split).duplicated, [
    `ocean light --accent declared in ${OCEAN} (:root:where([data-palette="ocean"])) and ${OCEAN} (:root:where([data-palette="ocean"]))`,
  ]);
  assert.deepEqual(paletteLedgerProblems(["classic", "ocean"], split), [
    `${OCEAN} holds [ocean light, ocean dark, ocean light] and must hold exactly [ocean light, ocean dark]`,
  ]);
});

test("a palette dark restatement that base dark already made, or palette light already made, is inert", () => {
  // `--accent` equals base dark's blue-700; `--radius-sm` equals ocean light's
  // 6px, which base dark leaves alone. Both change nothing in ocean dark.
  const inert = indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", OCEAN_LIGHT, "  color-scheme: dark;\n  --accent: var(--blue-700);\n  --radius-sm: 6px;"),
  }));
  assert.deepEqual(inertRestatements(inert), [
    "ocean dark: --accent: var(--blue-700) resolves to #124a8f with or without the block",
    "ocean dark: --radius-sm: 6px resolves to 6px with or without the block",
  ]);
  // The restatement is inert against the chain, not against the light block:
  // ocean dark repeating ocean light's `--bg-canvas` is a real change, because
  // base dark moved it in between. This is the terminal `--chrome-fg` case.
  const live = indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource(
      "ocean",
      "  color-scheme: light;\n  --bg-canvas: var(--gray-100);",
      "  color-scheme: dark;\n  --bg-canvas: var(--gray-100);",
    ),
  }));
  assert.deepEqual(inertRestatements(live), []);
  // A palette light restatement equal to base light is inert too.
  const light = indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  --radius-sm: 4px;", OCEAN_DARK),
  }));
  assert.deepEqual(inertRestatements(light), ["ocean light: --radius-sm: 4px resolves to 4px with or without the block"]);
});

test("a block that restates a colour without color-scheme, or with the other mode's, is refused", () => {
  const primitives = indexTokenLayer(syntheticLayer()).primitives;
  const missing = syntheticBlocks({
    [OCEAN]: paletteSource("ocean", "  --accent: var(--blue-300);", "  color-scheme: dark;\n  --accent: var(--blue-400);"),
  });
  assert.deepEqual(colourSchemeProblems(missing, primitives), [
    `${OCEAN}: :root:where([data-palette="ocean"]) restates a colour and declares no color-scheme`,
  ]);
  const swapped = syntheticBlocks({
    [OCEAN]: paletteSource(
      "ocean",
      "  color-scheme: dark;\n  --accent: var(--blue-300);",
      "  color-scheme: light dark;\n  --accent: var(--blue-400);",
    ),
  });
  assert.deepEqual(colourSchemeProblems(swapped, primitives), [
    `${OCEAN}: :root:where([data-palette="ocean"]) is a light block and declares color-scheme: dark`,
    `${OCEAN}: :root[data-palette="ocean"][data-theme="dark"] is a dark block and declares color-scheme: light dark`,
  ]);
  // A literal counts as painting even though it is refused elsewhere.
  const literal = syntheticBlocks({
    [OCEAN]: paletteSource("ocean", "  --accent: rgb(1 2 3);", OCEAN_DARK),
  });
  assert.deepEqual(colourSchemeProblems(literal, primitives), [
    `${OCEAN}: :root:where([data-palette="ocean"]) restates a colour and declares no color-scheme`,
  ]);
});

test("a scale-only block that declares color-scheme is refused, and one that does not is accepted", () => {
  const primitives = indexTokenLayer(syntheticLayer()).primitives;
  const claimed = syntheticBlocks({
    [OCEAN]: paletteSource("ocean", "  color-scheme: light;\n  --radius-sm: 6px;", "  color-scheme: dark;\n  --shadow-md: 0 0 0 1px rgb(0 0 0 / 40%);"),
  });
  assert.deepEqual(colourSchemeProblems(claimed, primitives), [
    `${OCEAN}: :root:where([data-palette="ocean"]) restates no colour and declares color-scheme`,
    `${OCEAN}: :root[data-palette="ocean"][data-theme="dark"] restates no colour and declares color-scheme`,
  ]);
  const silent = syntheticBlocks({
    [OCEAN]: paletteSource("ocean", "  --radius-sm: 6px;\n  --alpha-surface: 90%;", "  --shadow-md: 0 0 0 1px rgb(0 0 0 / 40%);"),
  });
  assert.deepEqual(colourSchemeProblems(silent, primitives), []);
  // The scale file's own base block is the same shape and passes for the same reason.
  const scaleWithScheme = syntheticBlocks();
  scaleWithScheme[1].blocks[0].declarations.push({ property: "color-scheme", value: "light" });
  assert.deepEqual(colourSchemeProblems(scaleWithScheme, primitives), [
    `${SCALE_FILE}: :root restates no colour and declares color-scheme`,
  ]);
});

test("an alpha knob at 79% is refused, at 80% accepted, and a non-percentage refused", () => {
  const at = (light, dark = "") => alphaKnobProblems(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", light, dark),
  })));
  assert.deepEqual(at("  --alpha-surface: 79%;"), ["ocean light: --alpha-surface: 79% is below 80%"]);
  assert.deepEqual(at("  --alpha-surface: 79.99%;"), ["ocean light: --alpha-surface: 79.99% is below 80%"]);
  assert.deepEqual(at("  --alpha-surface: 80%;"), []);
  assert.deepEqual(at("  --alpha-chrome: 88%;", "  --alpha-surface: 80%;"), []);
  assert.deepEqual(at("", "  --alpha-chrome: 60%;"), ["ocean dark: --alpha-chrome: 60% is below 80%"]);
  assert.deepEqual(at("  --alpha-surface: 0.9;"), ["ocean light: --alpha-surface: 0.9 is not a percentage"]);
  assert.deepEqual(at("  --alpha-surface: var(--alpha-chrome);"), ["ocean light: --alpha-surface: var(--alpha-chrome) is not a percentage"]);
  assert.deepEqual(at("  --alpha-surface: calc(80%);"), ["ocean light: --alpha-surface: calc(80%) is not a percentage"]);
  // The floor is on the knob family, not on the word: `--backdrop-*` is free.
  assert.deepEqual(at("  --backdrop-surface: blur(14px) saturate(140%);"), []);
});

test("a role-group collision that exists only in a palette dark scope is reported there alone", () => {
  // Ocean dark points `--bg-surface` at gray-950, which base dark already gave
  // `--bg-canvas`: the two collide in ocean dark and nowhere else.
  const collisions = roleGroupCollisions(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", OCEAN_LIGHT, "  color-scheme: dark;\n  --bg-surface: var(--gray-950);"),
  })));
  assert.deepEqual(collisions, ["ocean dark background: --bg-canvas and --bg-surface are both #0d0d0d"]);
  // And one a palette light block causes: base dark leaves the chrome roles
  // alone, so the light restatement carries into ocean dark and collides in
  // both ocean scopes, and in neither classic scope.
  const light = roleGroupCollisions(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  color-scheme: light;\n  --chrome-hover: var(--gray-800);", OCEAN_DARK),
  })));
  assert.deepEqual(light, [
    "ocean dark chrome: --chrome-appbar and --chrome-hover are both #2a2a2a",
    "ocean light chrome: --chrome-appbar and --chrome-hover are both #2a2a2a",
  ]);
});

test("a contrast failure that exists only in a palette scope is reported there alone", () => {
  const failures = contrastFailures(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  color-scheme: light;\n  --fg-text: var(--gray-300);", OCEAN_DARK),
  })));
  assert.deepEqual(failures.map((failure) => failure.replace(/ is [\d.]+:1, below /u, " below ")), [
    "ocean light: --fg-text (#bbbbbb) on --bg-canvas (#f7f7f7) below 4.5:1",
    "ocean light: --fg-text (#bbbbbb) on --bg-raised (#e8e8e8) below 4.5:1",
    "ocean light: --fg-text (#bbbbbb) on --bg-subtle (#f0f0f0) below 4.5:1",
    "ocean light: --fg-text (#bbbbbb) on --bg-surface (#ffffff) below 4.5:1",
  ]);
  assert.ok(failures.every((failure) => /is 1\.\d\d:1, below/u.test(failure)), failures.join("\n"));
  // The same restatement in dark mode fails in ocean dark only.
  const dark = contrastFailures(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", OCEAN_LIGHT, "  color-scheme: dark;\n  --chrome-fg: var(--gray-700);"),
  })));
  assert.ok(dark.length > 0 && dark.every((failure) => failure.startsWith("ocean dark: --chrome-fg")), dark.join("\n"));
  // A pair that does not resolve to a hex is reported rather than measured.
  const unresolved = contrastFailures(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  color-scheme: light;\n  --bg-canvas: transparent;", OCEAN_DARK),
  })));
  assert.deepEqual(unresolved, [
    "ocean light: --bg-canvas is not a six-digit hex: transparent",
    "ocean light: --bg-canvas is not a six-digit hex: transparent",
  ]);
});

test("the contrast floor is the palette's own: graphite is held to 7:1 where ocean passes at 4.5:1", () => {
  // gray-600 (#666666) is between 4.6:1 and 5.8:1 on every light ground: AA, but not AAA.
  const restatement = "  color-scheme: light;\n  --fg-text: var(--gray-600);";
  const ocean = contrastFailures(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", restatement, OCEAN_DARK),
  })));
  assert.deepEqual(ocean, []);
  const graphite = contrastFailures(indexTokenLayer(syntheticLayer({
    [paletteFile("graphite")]: paletteSource("graphite", restatement, "  color-scheme: dark;\n  --accent: var(--blue-400);"),
  }), SYNTHETIC_FLOORS));
  // The same palette held to AA alone passes: the floor, not the file, is what fails it.
  assert.deepEqual(contrastFailures(indexTokenLayer(syntheticLayer({
    [paletteFile("graphite")]: paletteSource("graphite", restatement, "  color-scheme: dark;\n  --accent: var(--blue-400);"),
  }))), []);
  assert.deepEqual(graphite.map((failure) => failure.split(" is ")[0]), [
    "graphite light: --fg-text (#666666) on --bg-canvas (#f7f7f7)",
    "graphite light: --fg-text (#666666) on --bg-raised (#e8e8e8)",
    "graphite light: --fg-text (#666666) on --bg-subtle (#f0f0f0)",
    "graphite light: --fg-text (#666666) on --bg-surface (#ffffff)",
  ]);
  assert.ok(graphite.every((failure) => failure.endsWith(", below 7:1")), graphite.join("\n"));
  // A palette named after an Object.prototype member reads AA, not a function.
  for (const name of ["constructor", "__proto__"]) {
    assert.equal(
      contrastFloorOf(name, SYNTHETIC_FLOORS),
      AA_CONTRAST,
      `${name} read a floor of ${String(contrastFloorOf(name, SYNTHETIC_FLOORS))}`,
    );
  }
  const prototype = contrastFailures(indexTokenLayer(syntheticLayer({
    [paletteFile("constructor")]: paletteSource("constructor", "  color-scheme: light;\n  --fg-text: var(--gray-300);", OCEAN_DARK),
  }), SYNTHETIC_FLOORS));
  assert.equal(prototype.length, 4, prototype.join("\n"));
});

test("a family kind mismatch that exists only in a palette scope is reported there alone", () => {
  // `0` keeps the radius family a length; `none` would split it.
  const square = familyKindMismatches(indexTokenLayer(syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  --radius-sm: 0;", OCEAN_DARK),
  })));
  assert.deepEqual(square, []);
  const layer = syntheticLayer({
    [OCEAN]: paletteSource("ocean", "  --radius-sm: none;", OCEAN_DARK),
  });
  layer[1].blocks[0].tokens.push({ name: "--radius-md", value: "8px" });
  const mixed = familyKindMismatches(indexTokenLayer(layer));
  assert.deepEqual(mixed, [
    `ocean dark --radius-*: keyword or stack = --radius-sm (${SCALE_FILE}); length = --radius-md (${SCALE_FILE})`,
    `ocean light --radius-*: keyword or stack = --radius-sm (${SCALE_FILE}); length = --radius-md (${SCALE_FILE})`,
  ]);
});

test("a palette file for a name PALETTES does not list, or a name with no file, is refused", () => {
  const sepia = paletteFile("sepia");
  const files = { [sepia]: paletteSource("sepia", OCEAN_LIGHT, OCEAN_DARK) };
  assert.deepEqual(syntheticBlockProblems(files), [
    `${sepia}: :root:where([data-palette="sepia"]) selects a palette ${PALETTE_MODULE} does not list in PALETTES`,
    `${sepia}: :root[data-palette="sepia"][data-theme="dark"] selects a palette ${PALETTE_MODULE} does not list in PALETTES`,
  ]);
  assert.deepEqual(paletteLedgerProblems(["classic", "ocean"], syntheticLayer(files)), [
    `${sepia} exists but sepia is not in ${PALETTE_MODULE}'s PALETTES`,
    `ocean is in PALETTES but ${OCEAN} does not exist`,
  ]);
  // The ocean blocks in a file named for another palette are refused as
  // sitting in the wrong file, and that file's name is refused as unlisted.
  const misplaced = { [sepia]: paletteSource("ocean", OCEAN_LIGHT, OCEAN_DARK) };
  assert.deepEqual(syntheticBlockProblems(misplaced), [
    `${sepia}: :root:where([data-palette="ocean"]) is the ocean light block, and this file holds only sepia blocks`,
    `${sepia}: :root[data-palette="ocean"][data-theme="dark"] is the ocean dark block, and this file holds only sepia blocks`,
  ]);
  // A classic file is refused by the ledger even when its blocks are refused by name.
  const classic = { [paletteFile(DEFAULT_PALETTE)]: paletteSource(DEFAULT_PALETTE, OCEAN_LIGHT, OCEAN_DARK) };
  assert.deepEqual(paletteLedgerProblems(["classic", "ocean"], syntheticLayer(classic)), [
    `${paletteFile(DEFAULT_PALETTE)} exists, but ${DEFAULT_PALETTE} is the base pair and has no palette file`,
    `ocean is in PALETTES but ${OCEAN} does not exist`,
  ]);
  // A token file that is neither the base pair nor a palette's cannot pose as a third base file.
  const extra = { "app/styles/tokens-extra.css": ":root {\n  --radius-lg: 12px;\n}\n" };
  assert.deepEqual(paletteLedgerProblems(["classic"], syntheticLayer(extra)), [
    "app/styles/tokens-extra.css is a token file that is neither the base pair nor a tokens-palette-<name>.css",
  ]);
  assert.deepEqual(inventedNames(syntheticLayer(extra)), []);
});

test("parsePaletteNames reads the PALETTES literal in the shapes the module may spell it", () => {
  assert.deepEqual(
    parsePaletteNames('export const PALETTES: readonly Palette[] = ["classic", "ocean", "ember"];'),
    ["classic", "ocean", "ember"],
  );
  assert.deepEqual(
    parsePaletteNames('export const PALETTES = [\n  "classic",\n  "ocean",\n] as const;'),
    ["classic", "ocean"],
  );
  assert.deepEqual(
    parsePaletteNames('export const PALETTE_STORAGE_KEY = "open-splunk.palette";\nexport const PALETTES: ReadonlyArray<Palette> = ["classic"];'),
    ["classic"],
  );
  // No literal at all reads as the base pair alone.
  assert.deepEqual(parsePaletteNames("export type Palette = \"classic\" | \"ocean\";"), [DEFAULT_PALETTE]);
  // A name the file system could not hold, or that is not lowercase, is not a palette.
  assert.deepEqual(parsePaletteNames('export const PALETTES = ["classic", "Ocean", "sepia tone", "9lives"];'), ["classic"]);
});

test("parseContrastFloors reads the PALETTE_CONTRAST_FLOOR literal in the shapes the module may spell it", () => {
  assert.deepEqual(
    { ...parseContrastFloors("export const PALETTE_CONTRAST_FLOOR: Readonly<Partial<Record<Palette, number>>> = { graphite: 7 };") },
    { graphite: 7 },
  );
  assert.deepEqual(
    { ...parseContrastFloors("export const PALETTE_CONTRAST_FLOOR = {\n  graphite: 7,\n  terminal: 4.5,\n} as const;") },
    { graphite: 7, terminal: 4.5 },
  );
  // No literal, or an empty one, promises AA everywhere; the table has no prototype to read a floor off.
  assert.deepEqual({ ...parseContrastFloors('export const PALETTES = ["classic"];') }, {});
  assert.deepEqual({ ...parseContrastFloors("export const PALETTE_CONTRAST_FLOOR = {};") }, {});
  assert.equal(Object.getPrototypeOf(parseContrastFloors("export const PALETTE_CONTRAST_FLOOR = { graphite: 7 };")), null);
  assert.equal(contrastFloorOf("graphite", parseContrastFloors("export const PALETTE_CONTRAST_FLOOR = { graphite: 7 };")), 7);
  assert.equal(contrastFloorOf("ocean", parseContrastFloors("export const PALETTE_CONTRAST_FLOOR = { graphite: 7 };")), AA_CONTRAST);
});

test("every palette PALETTE_CONTRAST_FLOOR names is in PALETTES and promises at least AA", async () => {
  const names = new Set(await readPaletteNames());
  const floors = await readContrastFloors();
  assert.ok(Object.hasOwn(floors, "graphite"), "graphite is the accessibility palette and promises 7:1");
  for (const [name, floor] of Object.entries(floors)) {
    assert.ok(names.has(name), `${PALETTE_MODULE}: PALETTE_CONTRAST_FLOOR names ${name}, which PALETTES does not list`);
    assert.ok(floor >= AA_CONTRAST, `${PALETTE_MODULE}: ${name} promises ${floor}:1, below AA`);
  }
});

/* == 9. The shipped palette files, each broken one way at a time ============== */
//
// Section 8 proves each invariant on a synthetic layer small enough to read.
// This proves the same invariants on the five files that ship: a check that
// was generalised correctly but reads a real file wrong -- a comment shape
// the parser skips, a declaration it drops -- would pass the synthetic layer
// and stay silent on the palette that matters. Each probe takes the blocks
// the inventory read (a test file may not open a stylesheet), writes them
// back out as source, injects exactly one fault into one palette's file, and
// asserts the invariant names that palette and no other. The unbroken
// round trip is the control.

/** One token file's blocks written back as source, declaration for declaration. */
function serialiseBlocks(entry) {
  return entry.blocks.map((block) => (
    `${block.prelude} {\n${block.declarations.map(({ property, value }) => `  ${property}: ${value};`).join("\n")}\n}\n`
  )).join("\n");
}

/**
 * The shipped token layer with one palette's source swapped, as
 * `collectTokenLayer` and `collectTokenBlocks` would each report it.
 * `mutate` receives the palette's source text and returns the broken text.
 */
function shippedLayerWith(shipped, file, mutate) {
  const sources = shipped.map((entry) => [entry.file, entry.file === file ? mutate(serialiseBlocks(entry)) : serialiseBlocks(entry)]);
  return {
    blocks: sources.map(([name, css]) => tokenBlocksOfSource(name, css)),
    layer: sources.map(([name, css]) => tokenLayerOfSource(name, css)),
  };
}

/** The shipped palettes as `{ name, file, light, dark }` with their two preludes. */
async function shippedPalettes() {
  const shipped = await collectTokenBlocks(workspace);
  const names = (await readPaletteNames()).filter((name) => name !== DEFAULT_PALETTE);
  return {
    palettes: names.map((name) => ({
      dark: `:root[data-palette="${name}"][data-theme="dark"]`,
      file: paletteFile(name),
      light: `:root:where([data-palette="${name}"])`,
      name,
    })),
    shipped,
  };
}

/** Every problem string that names a palette other than `own`. */
function mentioningOtherPalettes(problems, own, names) {
  return problems.filter((problem) => names.some((other) => other !== own && problem.includes(other)));
}

test("the shipped token layer survives a source round trip with every invariant still green", async () => {
  const { palettes, shipped } = await shippedPalettes();
  assert.ok(palettes.length >= 5, `only ${palettes.length} shipped palettes; the probes below would prove little`);
  const real = await collectTokenLayer(workspace);
  const { blocks, layer } = shippedLayerWith(shipped, null, (css) => css);
  // The control: the written-back source parses to the same layer, block for
  // block and token for token, so a fault the probes inject is the only
  // difference between the real files and what each invariant is handed.
  assert.deepEqual(layer, real);
  const names = new Set(await readPaletteNames());
  assert.deepEqual(blocks.flatMap((entry) => entry.blocks.flatMap((block) => tokenBlockProblems(entry.file, block, names))), []);
  const indexed = indexTokenLayer(layer, await readContrastFloors());
  assert.deepEqual(declarationSiteProblems(layer), { duplicated: [], literals: [] });
  assert.deepEqual(inventedNames(layer), []);
  assert.deepEqual(colourSchemeProblems(blocks, indexed.primitives), []);
  assert.deepEqual(inertRestatements(indexed), []);
  assert.deepEqual(alphaKnobProblems(indexed), []);
  assert.deepEqual(contrastFailures(indexed), []);
  assert.deepEqual(familyKindMismatches(indexed), []);
  assert.deepEqual(roleGroupCollisions(indexed), []);
  assert.deepEqual(paletteLedgerProblems([...names], layer), []);
});

test("each shipped palette's light block is refused the moment :where() is dropped, and only that palette", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  for (const palette of palettes) {
    const bare = `:root[data-palette="${palette.name}"]`;
    const { blocks, layer } = shippedLayerWith(shipped, palette.file, (css) => css.replace(palette.light, bare));
    const problems = blocks.flatMap((entry) => entry.blocks.flatMap((block) => tokenBlockProblems(entry.file, block, new Set([DEFAULT_PALETTE, ...names]))));
    assert.deepEqual(problems, [`${palette.file}: ${bare} is a rule, not one of the four theme blocks`]);
    const ledger = paletteLedgerProblems([DEFAULT_PALETTE, ...names], layer);
    assert.deepEqual(ledger, [
      `${palette.file} holds [not a theme block (${bare}), ${palette.name} dark] and must hold exactly [${palette.name} light, ${palette.name} dark]`,
    ]);
    // The bare block's values never index, so the palette's light scope
    // reads as classic light and every light-only restatement is gone.
    const indexed = indexTokenLayer(layer);
    assert.deepEqual([...indexed.palettes.get(palette.name).light], []);
    assert.deepEqual(mentioningOtherPalettes([...problems, ...ledger], palette.name, names), []);
  }
});

test("a colour literal of any spelling in a shipped palette's dark block is refused for that file alone", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  for (const palette of palettes) {
    for (const literal of ["#123456", "rgb(18 52 86)", "oklch(40% 0.1 250)"]) {
      const { layer } = shippedLayerWith(shipped, palette.file, (css) => (
        css.replace(`${palette.dark} {\n`, `${palette.dark} {\n  --accent: ${literal};\n`)
      ));
      const { duplicated, literals } = declarationSiteProblems(layer);
      assert.deepEqual(literals, [`${palette.file} (${palette.dark}): --accent: ${literal}`]);
      // Every shipped dark block restates --accent already, so the injected
      // declaration is also the one duplicate in that scope, and nowhere else.
      assert.deepEqual(duplicated, [`${palette.name} dark --accent declared in ${palette.file} (${palette.dark}) and ${palette.file} (${palette.dark})`]);
      assert.deepEqual(mentioningOtherPalettes([...literals, ...duplicated], palette.name, names), []);
    }
  }
});

test("a name no base block introduces is refused in whichever shipped palette invents it", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  for (const palette of palettes) {
    const { layer } = shippedLayerWith(shipped, palette.file, (css) => (
      css.replace(`${palette.light} {\n`, `${palette.light} {\n  --accent-glow: var(--blue-100);\n`)
    ));
    const invented = inventedNames(layer);
    assert.deepEqual(invented, [`${palette.file}: --accent-glow (${palette.light})`]);
    assert.deepEqual(mentioningOtherPalettes(invented, palette.name, names), []);
  }
});

test("a dark restatement that base dark already makes is inert in the shipped palette that carries it", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  const real = indexTokenLayer(await collectTokenLayer(workspace));
  for (const palette of palettes) {
    // A name base dark restates that this palette's dark block leaves alone:
    // copying base dark's value into the palette changes nothing the
    // browser lands on, which is exactly what the invariant refuses.
    const own = real.palettes.get(palette.name);
    const [name, value] = [...real.base.dark].find(([candidate]) => !own.dark.has(candidate) && !own.light.has(candidate));
    const { layer } = shippedLayerWith(shipped, palette.file, (css) => (
      css.replace(`${palette.dark} {\n`, `${palette.dark} {\n  ${name}: ${value};\n`)
    ));
    const scope = real.scopes.find((candidate) => candidate.label === scopeLabel(palette.name, "dark"));
    const inert = inertRestatements(indexTokenLayer(layer));
    assert.deepEqual(inert, [`${palette.name} dark: ${name}: ${value} resolves to ${resolve(name, scope.values)} with or without the block`]);
    assert.deepEqual(mentioningOtherPalettes(inert, palette.name, names), []);
  }
});

test("a shipped palette light block that loses its color-scheme is refused, and its mode cannot be swapped", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  const { primitives } = indexTokenLayer(await collectTokenLayer(workspace));
  for (const palette of palettes) {
    const missing = shippedLayerWith(shipped, palette.file, (css) => css.replace(`${palette.light} {\n  color-scheme: light;\n`, `${palette.light} {\n`));
    assert.deepEqual(colourSchemeProblems(missing.blocks, primitives), [
      `${palette.file}: ${palette.light} restates a colour and declares no color-scheme`,
    ]);
    const swapped = shippedLayerWith(shipped, palette.file, (css) => css.replace(`${palette.dark} {\n  color-scheme: dark;\n`, `${palette.dark} {\n  color-scheme: light;\n`));
    const problems = colourSchemeProblems(swapped.blocks, primitives);
    assert.deepEqual(problems, [`${palette.file}: ${palette.dark} is a dark block and declares color-scheme: light`]);
    assert.deepEqual(mentioningOtherPalettes(problems, palette.name, names), []);
  }
});

test("an alpha knob one point under the floor is refused in the shipped palette that turns it", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  for (const palette of palettes) {
    const { layer } = shippedLayerWith(shipped, palette.file, (css) => {
      // Glass already sets --alpha-surface in both blocks; lower its light
      // value in place so the probe injects a fault, not a duplicate.
      const lowered = css.replace(/--alpha-surface: \d+%;/u, "--alpha-surface: 79%;");
      return lowered === css ? css.replace(`${palette.light} {\n`, `${palette.light} {\n  --alpha-surface: 79%;\n`) : lowered;
    });
    const problems = alphaKnobProblems(indexTokenLayer(layer));
    assert.deepEqual(problems, [`${palette.name} light: --alpha-surface: 79% is below ${ALPHA_FLOOR}%`]);
    assert.deepEqual(mentioningOtherPalettes(problems, palette.name, names), []);
  }
});

test("body text painted in the canvas's own primitive fails contrast in that shipped palette's dark scope alone", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  const floors = await readContrastFloors();
  const real = indexTokenLayer(await collectTokenLayer(workspace), floors);
  for (const palette of palettes) {
    const scope = real.scopes.find((candidate) => candidate.label === scopeLabel(palette.name, "dark"));
    const canvas = resolve("--bg-canvas", scope.values);
    const [primitive] = [...real.primitives].find(([, hex]) => hex === canvas);
    const { layer } = shippedLayerWith(shipped, palette.file, (css) => {
      const restated = css.replace(new RegExp(`(${palette.dark.replaceAll(/[[\]]/gu, "\\$&")} \\{[^}]*?)--fg-text: var\\(--[a-z0-9-]+\\);`, "u"), `$1--fg-text: var(${primitive});`);
      return restated === css ? css.replace(`${palette.dark} {\n`, `${palette.dark} {\n  --fg-text: var(${primitive});\n`) : restated;
    });
    const failures = contrastFailures(indexTokenLayer(layer, floors));
    assert.ok(failures.length > 0, `${palette.name}: --fg-text in the canvas colour passed the contrast floor`);
    assert.ok(
      failures.every((failure) => failure.startsWith(`${palette.name} dark: `)),
      `${palette.name}: a failure landed outside the broken scope:\n${describeList(failures)}`,
    );
    assert.ok(
      failures.some((failure) => failure.includes(`--fg-text (${canvas}) on --bg-canvas (${canvas}) is 1.00:1`)),
      `${palette.name}: the injected pair is not among the failures:\n${describeList(failures)}`,
    );
    assert.deepEqual(mentioningOtherPalettes(failures, palette.name, names), []);
  }
});

test("a keyword where the radius family holds lengths splits the family in the shipped palette that wrote it", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  for (const palette of palettes) {
    const { layer } = shippedLayerWith(shipped, palette.file, (css) => {
      const replaced = css.replace(/--radius-sm: [^;]+;/u, "--radius-sm: none;");
      return replaced === css ? css.replace(`${palette.light} {\n`, `${palette.light} {\n  --radius-sm: none;\n`) : replaced;
    });
    const mixed = familyKindMismatches(indexTokenLayer(layer));
    assert.deepEqual(mixed.map((line) => line.split(" --")[0]), [`${palette.name} dark`, `${palette.name} light`]);
    assert.ok(mixed.every((line) => line.includes("keyword or stack = --radius-sm")), describeList(mixed));
    assert.deepEqual(mentioningOtherPalettes(mixed, palette.name, names), []);
  }
});

test("two foreground roles on one primitive collide in the shipped palette that merged them, and nowhere else", async () => {
  const { palettes, shipped } = await shippedPalettes();
  const names = palettes.map(({ name }) => name);
  const real = indexTokenLayer(await collectTokenLayer(workspace));
  for (const palette of palettes) {
    const scope = real.scopes.find((candidate) => candidate.label === scopeLabel(palette.name, "light"));
    const text = resolve("--fg-text", scope.values);
    const [primitive] = [...real.primitives].find(([, hex]) => hex === text);
    const { layer } = shippedLayerWith(shipped, palette.file, (css) => {
      const restated = css.replace(new RegExp(`(${palette.light.replaceAll(/[()[\]]/gu, "\\$&")} \\{[^}]*?)--fg-strong: var\\(--[a-z0-9-]+\\);`, "u"), `$1--fg-strong: var(${primitive});`);
      return restated === css ? css.replace(`${palette.light} {\n`, `${palette.light} {\n  --fg-strong: var(${primitive});\n`) : restated;
    });
    const collisions = roleGroupCollisions(indexTokenLayer(layer));
    assert.ok(
      collisions.some((line) => line.startsWith(`${palette.name} light foreground: `) && line.includes("--fg-strong") && line.includes("--fg-text")),
      `${palette.name}: the merged pair is not reported:\n${describeList(collisions)}`,
    );
    assert.ok(collisions.every((line) => line.startsWith(`${palette.name} `)), describeList(collisions));
    assert.deepEqual(mentioningOtherPalettes(collisions, palette.name, names), []);
  }
});
