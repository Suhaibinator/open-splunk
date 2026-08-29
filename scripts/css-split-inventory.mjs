/**
 * Inventory of the split stylesheet set Phase 4 replaced `app/globals.css` with.
 *
 * `css-inventory.mjs` answers "what does the styling layer contain"; this module
 * answers the questions the split itself raises: is every stylesheet reached
 * from the one entry point, exactly once; did every rule that left the monolith
 * arrive somewhere unchanged; does a `@media` block still sit in the file that
 * owns the rules it overrides; and is the module lane really gone rather than
 * merely unused.
 *
 * It is a library rather than a test file for the same reason `css-inventory`
 * is: `scripts/css-split-invariants.test.mjs` asserts that no test reads
 * stylesheet text, so the reading has to happen somewhere that is not a test.
 * Everything here is pure text analysis over the repository root it is handed --
 * no browser, no build, no network -- so the invariants stay deterministic.
 */
import { readFile } from "node:fs/promises";
import path from "node:path";

import {
  cssBlocks,
  cssDeclarations,
  listRepositoryFiles,
  listStylesheets,
  maskStringLiterals,
  relativePosix,
  stripCssComments,
} from "./css-inventory.mjs";

/** The one file `app/layout.tsx` imports, and the only `@import` list. */
export const STYLESHEET_ENTRY_POINT = "app/styles/index.css";

/** The monolith Phase 4 split, kept as a name so nothing has to spell it twice. */
export const RETIRED_MONOLITH = "app/globals.css";

/** Where the frozen pre-split rule set lives. */
export const MONOLITH_LEDGER = "scripts/css-phase3-monolith.json";

/** Files that name stylesheets for a tool rather than for the browser. */
const TOOL_CONFIGURATION_FILES = Object.freeze([
  ".github/workflows/ci.yml",
  ".stylelintrc.json",
  "Makefile",
  "next.config.ts",
  "package.json",
  "playwright.contracts.config.ts",
  "playwright.visual.config.ts",
]);

/** Sources whose comments hold the refactor's history and must not count. */
const COMMENTED_SOURCE_EXTENSIONS = new Set([".cjs", ".js", ".jsx", ".mjs", ".ts", ".tsx"]);

/**
 * The scanners, which have to spell in code what they are looking for.
 *
 * `RETIRED_MONOLITH` and the module extension are string constants of this file,
 * so a scan that read this file would report itself and nothing else. Excluding
 * the scanners hides nothing: neither one imports a stylesheet or reads a
 * `styles` object, and a module that came back would still be a file, which the
 * file walk sees whatever this list says.
 */
const SCANNER_MODULES = new Set(["scripts/css-inventory.mjs", "scripts/css-split-inventory.mjs"]);

/** The delimiters `maskStringLiterals` puts around a literal, for reporting. */
const MASK_CHARACTERS = /[‹›]/gu;

/** Collapses runs of whitespace so two spellings of one rule compare equal. */
function tidy(text) {
  return text.replaceAll(/\s+/gu, " ").trim();
}

/** Reads one repository-relative path. */
function readRepositoryFile(root, relative) {
  return readFile(path.join(root, ...relative.split("/")), "utf8");
}

/**
 * Blocks that only hold other rules, and so declare nothing of their own.
 *
 * A `@media` block is the context of the rules inside it rather than a rule, and
 * `cssBlocks` already hands each of those rules its ancestor list, so counting
 * the container as well would double-count every responsive rule.
 */
function isContainerAtRule(prelude) {
  return /^@(?:media|supports|keyframes|layer|container)\b/u.test(prelude);
}

/**
 * One rule as a single comparable line.
 *
 * The shape is `<at-rules> || <selector list> || <declarations>`: everything
 * that decides what the rule paints and nothing that decides where it is
 * written. Two rules compare equal exactly when the browser cannot tell them
 * apart, which is what makes "the split moved this rule" a checkable claim.
 */
export function ruleSignature(block) {
  const at = block.ancestors.map((ancestor) => tidy(ancestor)).join(" ");
  const selector = tidy(block.prelude).replaceAll(/\s*,\s*/gu, ", ");
  const declarations = cssDeclarations(block.body)
    .map((declaration) => `${tidy(declaration.property)}: ${tidy(declaration.value)}`)
    .join("; ");
  return `${at} || ${selector} || ${declarations}`;
}

/** Every rule one stylesheet's text states, in source order. */
export function collectRuleSignatures(css) {
  return cssBlocks(css)
    .filter((block) => !isContainerAtRule(block.prelude))
    .map((block) => ruleSignature(block));
}

/** Splits a signature back into its at-rule, selector and declaration parts. */
export function ruleParts(signature) {
  const [at = "", selector = "", declarations = ""] = signature.split(" || ");
  return { at, declarations, selector };
}

/**
 * The `@import` list of `app/styles/index.css`, in the order it states them.
 *
 * The order is the cascade contract -- tokens, base, primitives, features,
 * interaction -- so it is returned rather than sorted, and a caller that wants a
 * set can build one. Paths are repository-relative POSIX so they compare
 * directly against everything `css-inventory.mjs` returns.
 */
export async function listIndexImports(root) {
  const stylesRoot = path.join(root, "app", "styles");
  const index = await readFile(path.join(stylesRoot, STYLESHEET_ENTRY_POINT.split("/").at(-1)), "utf8");
  return [...stripCssComments(index).matchAll(/@import\s+url\("([^"]+)"\)/gu)].map((match) => ({
    file: relativePosix(root, path.join(stylesRoot, match[1])),
    specifier: match[1],
  }));
}

/**
 * What `app/styles/index.css` states besides the imports the scanner recognises.
 *
 * `@import` has spellings the pattern above does not read -- a bare string, a
 * single-quoted url, a layer or media qualifier -- and an unrecognised one is
 * the worst kind of miss: the browser loads the stylesheet and every check
 * derived from the import list silently stops covering it. Counting the
 * `@import` tokens and comparing turns that into a failure.
 *
 * A rule written directly in this file is the same failure from the other side.
 * `listGlobalStylesheets` builds its set out of what the entry point imports,
 * and the entry point is not one of them, so a class rule here is styled by
 * nothing the reachability or one-primitive checks can see.
 */
export async function collectEntryPointContent(root) {
  const stylesRoot = path.join(root, "app", "styles");
  const css = stripCssComments(await readFile(path.join(stylesRoot, STYLESHEET_ENTRY_POINT.split("/").at(-1)), "utf8"));
  return {
    rules: cssBlocks(css).map((block) => tidy(block.prelude)),
    statements: [...css.matchAll(/@import\b/gu)].length,
  };
}

/** Every stylesheet the application ships, repository-relative and sorted. */
export async function listApplicationStylesheets(root) {
  return (await listStylesheets(root))
    .map((file) => relativePosix(root, file))
    .filter((file) => file.startsWith("app/"))
    .toSorted();
}

/**
 * Every path the repository holds, as a set.
 *
 * Callers ask it several questions in a row -- does the monolith still exist,
 * does each of two dozen imports resolve -- and one walk answers all of them.
 */
export async function collectRepositoryPaths(root) {
  return new Set((await listRepositoryFiles(root)).map((file) => relativePosix(root, file)));
}

/** The frozen pre-split rule set, with the exclusions and edits it records. */
export async function readMonolithLedger(root) {
  const parsed = JSON.parse(await readFile(path.join(root, ...MONOLITH_LEDGER.split("/")), "utf8"));
  return {
    excluded: new Set(parsed.excluded.flatMap((entry) => entry.files)),
    rules: parsed.rules,
    substitutions: parsed.substitutions,
  };
}

/**
 * Every rule the split set states, concatenated in `index.css` import order.
 *
 * The stylesheets the ledger excludes are skipped: they were never in the
 * monolith, so including them would report the whole token layer as new.
 */
export async function collectSplitRules(root, excluded) {
  const wanted = (await listIndexImports(root)).filter((entry) => !excluded.has(entry.file));
  const perFile = await Promise.all(wanted.map(async (entry) => (
    collectRuleSignatures(await readRepositoryFile(root, entry.file))
      .map((signature) => ({ file: entry.file, signature }))
  )));
  return perFile.flat();
}

/** Counts how many times each value appears, so two lists diff as multisets. */
function tally(values) {
  const counts = new Map();
  for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1);
  return counts;
}

/**
 * The multiset difference between the frozen monolith and the split set.
 *
 * `missing` is what the monolith stated and the split set does not; `extra` is
 * the reverse. Order is deliberately not compared -- the phase moved every
 * responsive rule to the file owning its base rules, which reorders them on
 * purpose -- so a rule that merely moved shows up in neither list.
 */
export function diffRuleSets(recorded, live) {
  const before = tally(recorded);
  const after = tally(live.map((rule) => rule.signature));
  const sites = new Map();
  for (const rule of live) sites.set(rule.signature, rule.file);
  const missing = [];
  const extra = [];
  for (const [signature, count] of before) {
    for (let index = after.get(signature) ?? 0; index < count; index += 1) missing.push(signature);
  }
  for (const [signature, count] of after) {
    for (let index = before.get(signature) ?? 0; index < count; index += 1) {
      extra.push(`${sites.get(signature)} :: ${signature}`);
    }
  }
  return { extra: extra.toSorted(), missing: missing.toSorted() };
}

/** Applies the ledger's recorded edits so only unrecorded drift survives. */
export function applySubstitutions(rules, substitutions) {
  const replacements = new Map(substitutions.map((entry) => [entry.before, entry.after]));
  return rules.map((rule) => replacements.get(rule) ?? rule);
}

/**
 * The declared order of every property a repeated selector states more than once.
 *
 * Two rules with the same selector under the same at-rules tie on specificity,
 * so the later one wins and their order is the whole answer to "which value does
 * this element get". Keying on the selector text rather than on a guess at which
 * selectors match the same element keeps the check exact: it reports a real
 * inversion and never an imagined one. Selector lists are split, because
 * `.a, .b { color }` and a later `.b { color }` contest `.b` no differently for
 * `.b` having been written beside `.a`.
 */
export function collectTieBreakOrder(signatures) {
  const order = new Map();
  for (const signature of signatures) {
    const { at, declarations, selector } = ruleParts(signature);
    if (declarations === "") continue;
    for (const one of selector.split(", ")) {
      for (const declaration of declarations.split("; ")) {
        const separator = declaration.indexOf(":");
        const key = `${at} || ${one} || ${declaration.slice(0, separator)}`;
        order.set(key, [...(order.get(key) ?? []), declaration.slice(separator + 2)]);
      }
    }
  }
  return order;
}

/** Class names a selector list names, without duplicates. */
function selectorClasses(selector) {
  return [...new Set([...selector.matchAll(/\.(-?[_a-zA-Z][\w-]*)/gu)].map((match) => match[1]))];
}

/**
 * Which file states each class's base rules, and which file overrides it at a width.
 *
 * "Base" means outside every `@media`: the rule a responsive rule exists to
 * override. A class with no base rule anywhere -- `.table--cards`,
 * `.mobile-search-menu` -- is not homeless, it is mobile-only, and the media
 * block is its one and only home.
 */
export async function collectResponsiveOwnership(root) {
  const files = await Promise.all((await listApplicationStylesheets(root)).map(async (file) => {
    const base = new Set();
    const responsive = [];
    for (const block of cssBlocks(await readRepositoryFile(root, file))) {
      if (block.prelude.startsWith("@")) continue;
      const classes = selectorClasses(tidy(block.prelude));
      if (block.ancestors.some((ancestor) => ancestor.trimStart().startsWith("@media"))) {
        responsive.push({
          at: block.ancestors.map((ancestor) => tidy(ancestor)).join(" "),
          classes,
          selector: tidy(block.prelude),
        });
      } else for (const className of classes) base.add(className);
    }
    return { base, file, responsive };
  }));
  const owners = new Map();
  for (const entry of files) {
    for (const className of entry.base) {
      owners.set(className, new Set([...(owners.get(className) ?? []), entry.file]));
    }
  }
  return { files, owners };
}

/**
 * Where the canon puts a media query inside one run of them.
 *
 * docs/theming.md: base rules, then `1240px`, `980px`, `760px`, `480px`, then
 * `(pointer: coarse)`, then `(prefers-reduced-motion: reduce)`. Widths sort
 * largest-first so each overrides the one above it; the pointer query has to
 * follow them all, or a tap target set at a width beats the coarse-pointer
 * minimum. A query that also constrains height is not on the width canon at all
 * -- the two `max-height: 650px` queries are a legacy step the canon lists as
 * untouched -- so it sorts after every width and before the pointer rules,
 * which is where both of them already sit.
 */
export function mediaQueryRank(query) {
  if (/prefers-reduced-motion/u.test(query)) return 400;
  if (/pointer:\s*coarse/u.test(query)) return 300;
  if (/max-height/u.test(query)) return 200;
  const width = /max-width:\s*(\d+)px/u.exec(query);
  if (width !== null) return 100 - Number(width[1]) / 10_000;
  return 0;
}

/**
 * Each stylesheet's top-level blocks, as runs of media queries between rules.
 *
 * A file states the canon once per section rather than once per file, so the
 * sequence restarts wherever a base rule appears. Nesting is tracked with a
 * depth counter rather than a pattern, because a declaration value contains
 * braces and colons no regular expression can tell from structure.
 */
export async function collectMediaQueryRuns(root) {
  return Promise.all((await listApplicationStylesheets(root)).map(async (file) => {
    const css = stripCssComments(await readRepositoryFile(root, file));
    const runs = [];
    let run = [];
    let depth = 0;
    let start = 0;
    for (let index = 0; index < css.length; index += 1) {
      const character = css[index];
      if (character === "{") {
        if (depth === 0) {
          const prelude = tidy(css.slice(start, index));
          if (prelude.startsWith("@media")) run.push(tidy(prelude.slice("@media".length)));
          else {
            if (run.length > 1) runs.push(run);
            run = [];
          }
        }
        depth += 1;
      } else if (character === "}") {
        depth -= 1;
        if (depth === 0) start = index + 1;
      } else if (character === ";" && depth === 0) start = index + 1;
    }
    if (run.length > 1) runs.push(run);
    return { file, runs };
  }));
}

/**
 * Scoping escape hatches a CSS module needed and plain CSS has no use for.
 *
 * `:global()` and `:local()` are module syntax a browser does not implement, and
 * `composes` is a module-only property; each is a silent no-op in a plain
 * stylesheet rather than an error, so nothing but this reports one. Comments are
 * stripped first: the files that used to need them explain so in prose.
 */
export async function collectScopeEscapes(root) {
  const perFile = await Promise.all((await listApplicationStylesheets(root)).map(async (file) => {
    const css = stripCssComments(await readRepositoryFile(root, file));
    return [...css.matchAll(/:global\(|:local\(|\bcomposes\s*:/gu)].map((match) => `${file}: ${match[0].trim()}`);
  }));
  return perFile.flat().toSorted();
}

/**
 * Live references to the module lane: a `.module.css` path, or a `styles` object.
 *
 * The default import of a CSS module was always bound to `styles`, and every
 * class it scoped was read back off that object, so both halves are searched.
 * `maskStringLiterals` drops comments and leaves a literal's path characters
 * intact, which is the split this needs: a `.module.css` an import names is an
 * offender and a `.module.css` a comment recalls is history.
 *
 * A test file is excluded from the mention scan and only from it, the same
 * carve-out `collectCustomPropertyUsage` documents: `css-invariants.test.mjs`
 * asserts that no `.module.css` exists and has to write the extension down to
 * do it. A test cannot hide a module behind that, because a module is a file
 * and the file scan below sees every one of them.
 */
export async function collectModuleLaneReferences(root) {
  const perFile = await Promise.all((await listRepositoryFiles(root)).map(async (file) => {
    const relative = relativePosix(root, file);
    if (relative.endsWith(".module.css")) return [`${relative} is a CSS module`];
    if (!COMMENTED_SOURCE_EXTENSIONS.has(path.extname(file))) return [];
    if (SCANNER_MODULES.has(relative) || /\.(?:test|spec)\.[a-z]+$/u.test(relative)) return [];
    const source = maskStringLiterals(await readFile(file, "utf8"));
    const offenders = [];
    if (/\bmodule\.css\b/u.test(source)) offenders.push(`${relative} names a .module.css file`);
    if (/\bimport\s+styles\b/u.test(source)) offenders.push(`${relative} imports a styles object`);
    for (const match of source.matchAll(/\bstyles(\.[A-Za-z_$][\w$]*|\[)/gu)) {
      offenders.push(`${relative} reads styles${match[1] === "[" ? "[…]" : match[1]}`);
    }
    return offenders;
  }));
  return perFile.flat().toSorted();
}

/**
 * Live references to the deleted monolith, in code and in the tool configuration.
 *
 * Prose keeps saying `app/globals.css`, and should: the comments explaining the
 * split would be unreadable without naming what was split. What must not survive
 * is an instruction -- an import, a lint target, a path handed to a browser --
 * so source comments are masked and Markdown is left alone entirely.
 */
export async function collectMonolithReferences(root) {
  const configured = new Set(TOOL_CONFIGURATION_FILES);
  const perFile = await Promise.all((await listRepositoryFiles(root)).map(async (file) => {
    const relative = relativePosix(root, file);
    if (SCANNER_MODULES.has(relative)) return [];
    const isSource = COMMENTED_SOURCE_EXTENSIONS.has(path.extname(file));
    if (!isSource && !configured.has(relative)) return [];
    const raw = await readFile(file, "utf8");
    const source = isSource ? maskStringLiterals(raw) : raw;
    return source.includes(RETIRED_MONOLITH) ? [relative] : [];
  }));
  return perFile.flat().toSorted();
}

/**
 * The whole argument list of every call to `name`, parentheses balanced.
 *
 * `findStylesheetTextReads` reads a call's first argument by stopping at the
 * first comma or closing parenthesis, which is right for a path literal and
 * wrong for a path composed in place: `readFileSync(path.join(a, "b.css"),
 * "utf8")` hands that scan `path.join(a` and nothing that looks like a
 * stylesheet. Counting depth instead reads the argument the call actually
 * receives, whatever it is built out of.
 */
function callArguments(masked, name) {
  const found = [];
  const opener = new RegExp(String.raw`\b${name}\s*\(`, "gu");
  for (const match of masked.matchAll(opener)) {
    let depth = 1;
    let index = match.index + match[0].length;
    const start = index;
    while (index < masked.length && depth > 0) {
      if (masked[index] === "(") depth += 1;
      else if (masked[index] === ")") depth -= 1;
      index += 1;
    }
    if (depth === 0) found.push(masked.slice(start, index - 1));
  }
  return found;
}

/**
 * Test files that read a stylesheet's characters, however the path is composed.
 *
 * The layer's oldest invariant is that a test asserts on rendered behaviour
 * rather than on stylesheet text: source text says nothing about what the
 * cascade did with it, so an assertion on characters passes a rule that never
 * applies and fails a rewrite that paints identically. Phase 4 makes that
 * sharper, not softer -- a rule now moves between files routinely, and a test
 * that names the file it lives in breaks on a move that changed nothing.
 */
export function findComposedStylesheetReads(source) {
  const masked = maskStringLiterals(source);
  return ["readFileSync", "readFile", "createReadStream"]
    .flatMap((name) => callArguments(masked, name).map((argument) => ({ argument, name })))
    .filter((call) => /\.css\b/u.test(call.argument))
    .map((call) => `${call.name}(${tidy(call.argument).replaceAll(MASK_CHARACTERS, '"')})`);
}

/** Every such read the test suite performs, keyed by repository path. */
export async function collectTestStylesheetReads(root) {
  const tests = (await listRepositoryFiles(root))
    .filter((file) => /\.(?:test|spec)\.(?:ts|tsx|mjs|cjs|js|jsx)$/u.test(file));
  const perFile = await Promise.all(tests.map(async (file) => {
    const relative = relativePosix(root, file);
    return findComposedStylesheetReads(await readFile(file, "utf8")).map((read) => `${relative}: ${read}`);
  }));
  return perFile.flat().toSorted();
}

/** Stylesheets any file other than the entry point pulls in directly. */
export async function collectStylesheetImportSites(root) {
  const perFile = await Promise.all((await listRepositoryFiles(root)).map(async (file) => {
    const relative = relativePosix(root, file);
    const extension = path.extname(file);
    if (extension === ".css") {
      if (relative === STYLESHEET_ENTRY_POINT) return [];
      const css = stripCssComments(await readFile(file, "utf8"));
      return [...css.matchAll(/@import\s+url\("([^"]+)"\)/gu)].map((match) => `${relative} @imports ${match[1]}`);
    }
    if (!COMMENTED_SOURCE_EXTENSIONS.has(extension)) return [];
    const masked = maskStringLiterals(await readFile(file, "utf8"));
    return [...masked.matchAll(/\bimport\s+‹([^›]*\.css)›/gu)].map((match) => `${relative} imports ${match[1]}`);
  }));
  return perFile.flat().toSorted();
}
