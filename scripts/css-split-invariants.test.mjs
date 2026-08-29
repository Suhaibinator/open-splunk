// Structural invariants for the split that replaced `app/globals.css`.
//
// Phase 4 carved one 7,267-line stylesheet into a shared layer under
// `app/styles/` plus one file per feature, colocated with the code that renders
// it, and converted the last CSS modules to plain prefixed CSS. Every claim that
// makes is invisible to a screenshot: a rule can be dropped on the way out of
// the monolith and cost nothing until the one page that used it is opened, a
// responsive block can be left in a file that no longer owns its base rules and
// stay correct until either file is edited, and a second `@import` of the same
// stylesheet changes the cascade without changing a pixel today. So they are
// asserted here instead.
//
// The strongest of them is parity: `scripts/css-phase3-monolith.json` freezes
// the rule set the monolith stated at the commit before the split, and the
// comparison below rebuilds the same inventory from the files
// `app/styles/index.css` imports. A move that lost, duplicated, or quietly
// rewrote a rule fails with that rule's own text.
//
// Like `css-invariants.test.mjs`, this file never opens a stylesheet: the
// reading and parsing live in `scripts/css-split-inventory.mjs`, so the rule
// that no test may read stylesheet text keeps holding.
import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

import {
  MONOLITH_LEDGER,
  RETIRED_MONOLITH,
  STYLESHEET_ENTRY_POINT,
  applySubstitutions,
  collectEntryPointContent,
  collectMediaQueryRuns,
  collectModuleLaneReferences,
  collectMonolithReferences,
  collectRepositoryPaths,
  collectResponsiveOwnership,
  collectRuleSignatures,
  collectScopeEscapes,
  collectSplitRules,
  collectStylesheetImportSites,
  collectTestStylesheetReads,
  collectTieBreakOrder,
  diffRuleSets,
  findComposedStylesheetReads,
  listApplicationStylesheets,
  listIndexImports,
  mediaQueryRank,
  readMonolithLedger,
  ruleSignature,
} from "./css-split-inventory.mjs";

const workspace = process.cwd();

/** The file whose cross-cutting rules are deliberately not colocated. */
const CROSS_CUTTING_STYLESHEET = "app/styles/interaction.css";

/** The load order `app/styles/index.css` documents as its contract. */
const LOAD_ORDER = Object.freeze(["tokens", "base", "primitives", "features", "interaction"]);

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

/** Which band of the load order an import specifier belongs to. */
function loadBand(specifier) {
  if (/^\.\/tokens-[a-z0-9-]+\.css$/u.test(specifier)) return "tokens";
  if (specifier === "./base.css") return "base";
  if (specifier.startsWith("./primitives/")) return "primitives";
  if (specifier === "./interaction.css") return "interaction";
  return "features";
}

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

test("the split inventory reaches the whole stylesheet set", async () => {
  const imports = await listIndexImports(workspace);
  assert.ok(
    imports.length > 20,
    `${STYLESHEET_ENTRY_POINT} states only ${imports.length} imports; the walker is reading the wrong file`
      + " and every parity and ordering assertion below is vacuous",
  );
  const stylesheets = await listApplicationStylesheets(workspace);
  assert.ok(
    stylesheets.length > 20,
    `the walker found only ${stylesheets.length} application stylesheets; it is missing the layer`,
  );
  const ledger = await readMonolithLedger(workspace);
  assert.ok(
    ledger.rules.length > 1500,
    `${MONOLITH_LEDGER} records only ${ledger.rules.length} rules; the frozen monolith is truncated and`
      + " parity would pass by having nothing to compare",
  );
});

test("the monolith is gone and nothing still points at it", async () => {
  assert.equal(
    (await collectRepositoryPaths(workspace)).has(RETIRED_MONOLITH),
    false,
    `${RETIRED_MONOLITH} is back. Phase 4 exists to make a theme change a one-file edit; a second`
      + " home for a rule is the thing it removed.",
  );
  const references = await collectMonolithReferences(workspace);
  assert.deepEqual(
    references,
    [],
    `Code or tool configuration still names ${RETIRED_MONOLITH}. A lint target, an injected path or an`
      + " import that points at a deleted file does not fail -- it silently covers nothing. Prose may"
      + ` still recall the monolith; an instruction may not:\n${describeList(references)}`,
  );
});

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
  const present = await collectRepositoryPaths(workspace);
  const missing = imports
    .filter((entry) => !present.has(entry.file))
    .map((entry) => `${entry.specifier} resolves to ${entry.file}`);
  assert.deepEqual(
    missing.toSorted(),
    [],
    `${STYLESHEET_ENTRY_POINT} imports files that do not exist. The bundler drops an unresolved @import,`
      + ` so the rules are simply absent:\n${describeList(missing.toSorted())}`,
  );
  const orphans = (await listApplicationStylesheets(workspace))
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
      + " fixture injection, the parity comparison below -- covers only the ones it can parse, and says"
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

test("no test reads a stylesheet's characters, however the path is composed", async () => {
  const reads = await collectTestStylesheetReads(workspace);
  assert.deepEqual(
    reads,
    [],
    "A test asserts on stylesheet text. `css-invariants.test.mjs` already forbids that, and misses this\n"
      + "shape: its scan reads a call's first argument up to the first comma, so a path composed inside the\n"
      + "call -- readFileSync(path.join(...), \"utf8\") -- hands it nothing that looks like a stylesheet.\n"
      + "The rule matters more after Phase 4, not less: a rule moves between files routinely now, so an\n"
      + "assertion naming the file a rule lives in fails on a move that changed no pixel, and passes on a\n"
      + "rule the cascade overrides everywhere. Assert on computed style in\n"
      + `integration/visual/css-contracts.spec.ts instead:\n${describeList(reads)}`,
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

test("every rule the monolith stated is stated once by the split set, unchanged", async () => {
  const ledger = await readMonolithLedger(workspace);
  const recorded = applySubstitutions(ledger.rules, ledger.substitutions);
  const live = await collectSplitRules(workspace, ledger.excluded);
  const compared = new Set(live.map((rule) => rule.file));
  assert.ok(
    compared.size > 15,
    `parity is comparing only ${compared.size} of the layer's stylesheets. Every file the ledger excludes`
      + " is a file this test says nothing about, so an exclusion is the one way to make the comparison"
      + " pass by shrinking it.",
  );
  const { extra, missing } = diffRuleSets(recorded, live);
  assert.deepEqual(
    missing,
    [],
    `These rules were in ${RETIRED_MONOLITH} and are in no file ${STYLESHEET_ENTRY_POINT} imports, or arrived\n`
      + "with a declaration changed. The phase claims a move, and a move loses nothing: put the rule back\n"
      + `verbatim, or record the edit in ${MONOLITH_LEDGER} with the reason it is not a\n`
      + `move:\n${describeList(missing)}`,
  );
  assert.deepEqual(
    extra,
    [],
    `These rules are in the split set and were never in ${RETIRED_MONOLITH}. A rule that was copied rather\n`
      + "than moved is now stated twice, and the two copies drift apart with nothing to report it; a rule\n"
      + `that was written fresh belongs to a later phase than this one:\n${describeList(extra)}`,
  );
});

test("the monolith ledger records no edit the split set no longer makes", async () => {
  const ledger = await readMonolithLedger(workspace);
  const stated = new Set(ledger.rules);
  const live = new Set((await collectSplitRules(workspace, ledger.excluded)).map((rule) => rule.signature));
  const stale = [];
  for (const entry of ledger.substitutions) {
    if (!stated.has(entry.before)) stale.push(`no monolith rule reads ${entry.before}`);
    if (!live.has(entry.after)) stale.push(`no rule in the split set reads ${entry.after}`);
  }
  const imported = new Set((await listIndexImports(workspace)).map((entry) => entry.file));
  for (const file of [...ledger.excluded].toSorted()) {
    if (!imported.has(file)) stale.push(`${file} is excluded from parity but nothing imports it`);
  }
  assert.deepEqual(
    stale.toSorted(),
    [],
    `${MONOLITH_LEDGER} grandfathers edits that no longer describe the tree. An exemption nobody can see\n`
      + "the effect of is an exemption nobody reviews, and it hides the next edit made under the same\n"
      + `heading:\n${describeList(stale.toSorted())}`,
  );
});

test("every repeated selector keeps the order that decides its value", async () => {
  const ledger = await readMonolithLedger(workspace);
  const recorded = collectTieBreakOrder(applySubstitutions(ledger.rules, ledger.substitutions));
  const split = await collectSplitRules(workspace, ledger.excluded);
  const live = collectTieBreakOrder(split.map((rule) => rule.signature));
  const contested = [...recorded.entries()].filter(([, values]) => values.length > 1);
  assert.ok(
    contested.length > 10,
    `only ${contested.length} selectors state a property more than once; the tie-break scan is not reading`
      + " the layer and this assertion is vacuous",
  );
  const inverted = contested
    .filter(([key, values]) => JSON.stringify(live.get(key) ?? []) !== JSON.stringify(values))
    .map(([key, values]) => `${key}: was [${values.join(" -> ")}], now [${(live.get(key) ?? []).join(" -> ")}]`)
    .toSorted();
  assert.deepEqual(
    inverted,
    [],
    "Two rules with the same selector under the same at-rules tie on specificity, so the later one wins\n"
      + "and their order is the whole answer to what the element gets. The split moved one of them past\n"
      + `the other, which changes the value with every rule still present:\n${describeList(inverted)}`,
  );
});

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

test("app/styles/interaction.css is the layer's one cross-cutting file, and loads last", async () => {
  const imports = await listIndexImports(workspace);
  assert.equal(
    imports.at(-1).file,
    CROSS_CUTTING_STYLESHEET,
    `${CROSS_CUTTING_STYLESHEET} states the coarse-pointer tap target, the 16px input floor and the`
      + " reduced-motion cut. Each has to outrank every feature rule it applies to, and at equal"
      + " specificity that is decided by being last. Whatever now sits below it wins those instead.",
  );
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
      + " any more and the exemption the previous test grants it is dead. Either its rules have moved to"
      + " the features they apply to -- in which case delete the exemption -- or the scan has stopped"
      + " seeing them.",
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

test("the load order runs tokens, base, primitives, features, then interaction", async () => {
  const imports = await listIndexImports(workspace);
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
  const misplaced = imports
    .filter((entry) => (bands[imports.indexOf(entry)] === "features") !== entry.specifier.startsWith("../"))
    .map((entry) => entry.specifier)
    .toSorted();
  assert.deepEqual(
    misplaced,
    [],
    "A feature stylesheet lives with the code that renders it, and a shared one lives under app/styles.\n"
      + "A feature file inside the shared layer is a rule nobody finds from the component, and a shared\n"
      + `file outside it is a primitive with a feature's name on it:\n${describeList(misplaced)}`,
  );
  for (const band of LOAD_ORDER) {
    assert.ok(bands.includes(band), `no import falls in the ${band} band, so its half of the order is untested`);
  }
});

// The invariants above are only worth their runtime if the parsing underneath
// them can see a violation, so the parser is pinned against the shapes the
// stylesheets actually use.

test("ruleSignature carries a rule's at-rule context and its declarations", () => {
  const signatures = collectRuleSignatures(`
    @media (max-width: 760px) {
      .fixture-card,
      .fixture-panel { gap: 4px; padding: 0.5rem; }
    }
    /* .fixture-retired { color: red; } */
    .fixture-live { color: var(--fg-text); }
  `);
  assert.deepEqual(signatures, [
    "@media (max-width: 760px) || .fixture-card, .fixture-panel || gap: 4px; padding: 0.5rem",
    " || .fixture-live || color: var(--fg-text)",
  ]);
});

test("ruleSignature keeps a value that carries its own commas, colons and parentheses", () => {
  const [signature] = collectRuleSignatures(
    ".fixture-shell { background: url(https://example.test/a.png); box-shadow: 0 1px 2px rgb(0 0 0 / 10%), 0 0 1px #000; }",
  );
  assert.equal(
    signature,
    " || .fixture-shell || background: url(https://example.test/a.png);"
      + " box-shadow: 0 1px 2px rgb(0 0 0 / 10%), 0 0 1px #000",
  );
});

test("ruleSignature normalises only whitespace, so two spellings of one rule compare equal", () => {
  const spread = ruleSignature({
    ancestors: ["@media\n  (max-width: 480px)"],
    body: "\n  color:  red;\n  gap:\t1px;\n",
    prelude: ".fixture-a ,\n.fixture-b",
  });
  const compact = ruleSignature({
    ancestors: ["@media (max-width: 480px)"],
    body: "color: red; gap: 1px;",
    prelude: ".fixture-a, .fixture-b",
  });
  assert.equal(spread, compact);
});

test("diffRuleSets counts rules rather than merely matching them", () => {
  const stated = " || .fixture-a || color: red";
  const once = diffRuleSets([stated], [{ file: "app/one.css", signature: stated }]);
  assert.deepEqual([once.missing, once.extra], [[], []]);
  const twice = diffRuleSets([stated], [
    { file: "app/one.css", signature: stated },
    { file: "app/two.css", signature: stated },
  ]);
  // A rule copied into a second file, rather than moved, is the failure a set
  // comparison cannot see: both sides would still contain it.
  assert.deepEqual(twice.missing, []);
  assert.deepEqual(twice.extra, [`app/two.css :: ${stated}`]);
});

test("applySubstitutions rewrites only the rules the ledger names", () => {
  const rewritten = applySubstitutions(
    [" || .fixture-a || width: 48px", " || .fixture-b || width: 48px"],
    [{ after: " || .fixture-a || width: var(--fixture-width, 48px)", before: " || .fixture-a || width: 48px" }],
  );
  assert.deepEqual(rewritten, [
    " || .fixture-a || width: var(--fixture-width, 48px)",
    " || .fixture-b || width: 48px",
  ]);
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

test("collectTieBreakOrder splits selector lists and keeps declaration order", () => {
  const order = collectTieBreakOrder([
    " || .fixture-a, .fixture-b || color: red",
    "@media (max-width: 480px) || .fixture-b || color: green",
    " || .fixture-b || color: blue",
  ]);
  assert.deepEqual(order.get(" || .fixture-b || color"), ["red", "blue"]);
  assert.deepEqual(order.get(" || .fixture-a || color"), ["red"]);
  assert.deepEqual(order.get("@media (max-width: 480px) || .fixture-b || color"), ["green"]);
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
