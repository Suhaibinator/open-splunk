/**
 * The one static inventory of the styling layer.
 *
 * Every structural assertion in `scripts/style-invariants.test.mjs` reads the
 * stylesheets through this module, and that file reads them through nothing
 * else. The separation is the point: the suite asserts that no test opens a
 * stylesheet and asserts on its characters, so the reading has to live in a
 * library, and a library is also where a parser can be pinned by tests of its
 * own rather than re-derived by each caller.
 *
 * Phase 5 folded three such libraries -- the layer inventory, the split
 * inventory and the token sweep -- into this one. They had grown two exported
 * functions called `listApplicationStylesheets` that returned different sets in
 * different shapes, which is the collision a single module makes impossible:
 * the stylesheets under `app/` are `listApplicationStylesheets` and the ones
 * outside the token layer are `listNonTokenStylesheets`.
 *
 * Everything here is pure text analysis over the repository root it is handed.
 * It never starts a browser, never builds and never looks outside that root, so
 * the invariants built on it stay deterministic and fast.
 *
 * The three sections below keep the questions they answer apart:
 *
 *   1. the layer -- what the stylesheets contain and how code names their
 *      classes, tokens and animations;
 *   2. the split -- whether one entry point still reaches every stylesheet
 *      exactly once, and whether the cascade order it states still holds;
 *   3. the sweep -- which literals survive outside the token layer, and what
 *      kind of value each token and each property carries.
 */
import { readFileSync } from "node:fs";
import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";

/* == 1. The layer ========================================================== */

/** Directories holding dependencies, build output, or other checkouts. */
const IGNORED_DIRECTORY_NAMES = new Set([
  ".cache",
  ".claude",
  ".git",
  ".next",
  "build",
  "coverage",
  "dist",
  "node_modules",
  "out",
  "test-results",
]);

/** Sources that can name a class: application code and test harnesses. */
const SOURCE_EXTENSIONS = new Set([".cjs", ".js", ".jsx", ".mjs", ".ts", ".tsx"]);

/** A prefix shorter than this matches too much to prove a class is reachable. */
const MINIMUM_INTERPOLATION_PREFIX_LENGTH = 3;

/**
 * Delimiters that stand in for a string literal in masked source.
 *
 * Masking replaces every character a literal could contribute to the code
 * grammar, so masked text can never be mistaken for the code around it. Both
 * delimiters are stripped out of literal values during masking, so a value can
 * never close its own mask.
 */
const MASK_OPEN = "‹";
const MASK_CLOSE = "›";

/** Lists every file under `root`, skipping dependency and build directories. */
export async function listRepositoryFiles(root) {
  async function walk(directory) {
    const entries = await readdir(directory, { withFileTypes: true });
    const nested = await Promise.all(entries.map(async (entry) => {
      if (IGNORED_DIRECTORY_NAMES.has(entry.name)) return [];
      const full = path.join(directory, entry.name);
      if (entry.isSymbolicLink()) {
        // A linked dependency tree is not repository source; a linked file is.
        const target = await stat(full).catch(() => undefined);
        return target === undefined || target.isDirectory() ? [] : [full];
      }
      if (entry.isDirectory()) return walk(full);
      return entry.isFile() ? [full] : [];
    }));
    return nested.flat();
  }
  // Sorted once at the end: the walk runs in parallel, and every caller reports
  // paths back to a reader who needs the same order every run.
  return (await walk(root)).toSorted((left, right) => left.localeCompare(right));
}

/** Repository-relative POSIX path, so failure messages read the same everywhere. */
export function relativePosix(root, file) {
  return path.relative(root, file).split(path.sep).join("/");
}

/** Every stylesheet in the repository, sorted for stable reporting. */
export async function listStylesheets(root) {
  return (await listRepositoryFiles(root)).filter((file) => file.endsWith(".css"));
}

/** Every file that `node --test` or Playwright executes as a test. */
export async function listTestFiles(root) {
  return (await listRepositoryFiles(root))
    .filter((file) => /\.(?:test|spec)\.(?:ts|tsx|mjs|cjs|js|jsx)$/u.test(file));
}

/** Removes `/* … *\/` comments so commented-out rules never count as live CSS. */
export function stripCssComments(css) {
  return css.replaceAll(/\/\*[\s\S]*?\*\//gu, " ");
}

/**
 * Splits a stylesheet into the selector preludes that open a block.
 *
 * A regular expression cannot do this: declaration values contain dots
 * (`0.5rem`), and at-rules nest, so the prelude has to be tracked with a depth
 * counter that resets at `;`, `{`, and `}`.
 */
export function cssBlockPreludes(css) {
  const preludes = [];
  let buffer = "";
  let depth = 0;
  for (const character of stripCssComments(css)) {
    if (character === "{") {
      preludes.push({ depth, text: buffer.trim() });
      buffer = "";
      depth += 1;
    } else if (character === "}") {
      depth = Math.max(0, depth - 1);
      buffer = "";
    } else if (character === ";") {
      buffer = "";
    } else {
      buffer += character;
    }
  }
  return preludes;
}

/** Class names a stylesheet writes rules for, excluding at-rule preludes. */
export function collectStylesheetClasses(css) {
  const classes = new Set();
  for (const { text } of cssBlockPreludes(css)) {
    if (text.length === 0 || text.startsWith("@")) continue;
    for (const match of text.matchAll(/\.(-?[_a-zA-Z][\w-]*)/gu)) classes.add(match[1]);
  }
  return classes;
}

/** Custom properties a stylesheet declares, including `@property` at-rules. */
export function collectCustomPropertyDefinitions(css) {
  const declared = new Set();
  const body = stripCssComments(css);
  for (const match of body.matchAll(/(?:^|[;{\s])(--[\w-]+)\s*:/gu)) declared.add(match[1]);
  for (const match of body.matchAll(/@property\s+(--[\w-]+)/gu)) declared.add(match[1]);
  return declared;
}

/** Custom properties a stylesheet reads through `var()`. */
function collectCustomPropertyReferences(css) {
  const referenced = new Set();
  for (const match of stripCssComments(css).matchAll(/var\(\s*(--[\w-]+)/gu)) {
    referenced.add(match[1]);
  }
  return referenced;
}

/**
 * Custom properties application code sets at runtime.
 *
 * A property assigned from an inline style object or through `setProperty` is
 * declared just as truly as one written in a `:root` block, so a `var()` that
 * reads it is live rather than dangling.
 */
export function collectRuntimeCustomProperties(source) {
  const declared = new Set();
  for (const match of source.matchAll(/["'`](--[\w-]+)["'`]\s*:/gu)) declared.add(match[1]);
  for (const match of source.matchAll(/setProperty\(\s*["'`](--[\w-]+)["'`]/gu)) declared.add(match[1]);
  return declared;
}

/**
 * Walks JavaScript or TypeScript source, separating code from literal text.
 *
 * Both callers below need the same distinction and neither can get it from a
 * regular expression: pairing quotes with a pattern goes wrong at the first
 * backslash escape or nested template literal, and one mispairing silently
 * flips the classification of everything after it in the file. `onLiteral`
 * receives one static segment at a time, with `followedByHole` set when a
 * `${…}` interpolation comes straight after it. Comments are dropped.
 */
function walkSource(source, onCode, onLiteral) {
  const length = source.length;

  function readQuoted(start) {
    const quote = source[start];
    let index = start + 1;
    let text = "";
    while (index < length) {
      const character = source[index];
      if (character === "\\") {
        text += " ";
        index += 2;
        continue;
      }
      // A quoted literal cannot span a line, so an unterminated quote was
      // something else -- a apostrophe in a comment, say -- and the caller
      // rewinds to treat it as code.
      if (character === "\n") return -1;
      if (character === quote) {
        onLiteral(text, false);
        return index + 1;
      }
      text += character;
      index += 1;
    }
    return -1;
  }

  function readTemplate(start) {
    let index = start + 1;
    let text = "";
    while (index < length) {
      const character = source[index];
      if (character === "\\") {
        text += " ";
        index += 2;
        continue;
      }
      if (character === "`") {
        onLiteral(text, false);
        return index + 1;
      }
      if (character === "$" && source[index + 1] === "{") {
        onLiteral(text, true);
        text = "";
        const next = readHole(index + 2);
        if (next === -1) return -1;
        index = next;
        continue;
      }
      text += character;
      index += 1;
    }
    return -1;
  }

  function readHole(start) {
    let index = start;
    let depth = 1;
    while (index < length) {
      const character = source[index];
      if (character === '"' || character === "'") {
        const next = readQuoted(index);
        if (next !== -1) {
          index = next;
          continue;
        }
      } else if (character === "`") {
        const next = readTemplate(index);
        if (next === -1) return -1;
        index = next;
        continue;
      } else if (character === "{") {
        depth += 1;
      } else if (character === "}") {
        depth -= 1;
        if (depth === 0) {
          onCode("}");
          return index + 1;
        }
      }
      onCode(character);
      index += 1;
    }
    return -1;
  }

  let index = 0;
  while (index < length) {
    const character = source[index];
    if (character === "/" && source[index + 1] === "/") {
      const end = source.indexOf("\n", index);
      onCode("\n");
      index = end === -1 ? length : end + 1;
      continue;
    }
    if (character === "/" && source[index + 1] === "*") {
      const end = source.indexOf("*/", index + 2);
      onCode(" ");
      index = end === -1 ? length : end + 2;
      continue;
    }
    if (character === '"' || character === "'") {
      const next = readQuoted(index);
      if (next !== -1) {
        index = next;
        continue;
      }
    } else if (character === "`") {
      const next = readTemplate(index);
      if (next !== -1) {
        index = next;
        continue;
      }
    }
    onCode(character);
    index += 1;
  }
}

/**
 * Rewrites source so every literal becomes an inert `‹value›` marker.
 *
 * Only characters a path needs survive inside a marker, so source quoted inside
 * a test fixture can no longer be read as code: `"readFileSync(\"a.css\")"`
 * masks to one marker whose parenthesis is gone, while a real
 * `readFileSync("a.css")` keeps its call shape and gains a marked argument.
 */
export function maskStringLiterals(source) {
  const pieces = [];
  walkSource(
    source,
    (text) => pieces.push(text),
    (text) => {
      pieces.push(MASK_OPEN, text.replaceAll(/[^\w./@-]/gu, "·"), MASK_CLOSE);
    },
  );
  return pieces.join("");
}

/**
 * Identifiers and interpolation prefixes a source file mentions.
 *
 * `tokens` holds every word inside a string or template literal, which covers
 * `className="a b"`, `querySelector(".a")`, and class names a helper returns as
 * a literal. `interpolationPrefixes` holds the static text immediately before a
 * `${…}` hole, which is how the codebase builds modifier classes such as
 * `` `status-label status-label--${tone}` ``.
 */
export function collectSourceClassEvidence(source) {
  const tokens = new Set();
  const interpolationPrefixes = new Set();
  walkSource(
    source,
    () => undefined,
    (text, followedByHole) => {
      for (const token of text.split(/[^\w-]+/u)) {
        if (token.length > 0) tokens.add(token);
      }
      if (!followedByHole) return;
      const prefix = /([\w-]+)$/u.exec(text);
      if (prefix !== null && prefix[1].length >= MINIMUM_INTERPOLATION_PREFIX_LENGTH) {
        interpolationPrefixes.add(prefix[1]);
      }
    },
  );
  return { interpolationPrefixes, tokens };
}

/** Class names a static HTML page carries in `class` attributes. */
function collectMarkupClasses(markup) {
  const tokens = new Set();
  for (const match of markup.matchAll(/class\s*=\s*"([^"]*)"/gu)) {
    for (const token of match[1].split(/[^\w-]+/u)) {
      if (token.length > 0) tokens.add(token);
    }
  }
  return tokens;
}

/**
 * Reads every non-CSS source file and merges the class evidence it carries.
 *
 * A stylesheet contributes nothing here: it is the other side of the question.
 * Until Phase 4 a CSS module was the exception -- its `:global(...)` selectors
 * named global classes while its own classes were reached through a generated
 * `styles` object -- and with the modules gone there is one namespace and one
 * direction to read.
 */
export async function collectClassEvidence(root) {
  const files = await listRepositoryFiles(root);
  const contributions = await Promise.all(files.map(async (file) => {
    if (file.endsWith(".css")) return null;
    if (file.endsWith(".html")) {
      return { interpolationPrefixes: new Set(), tokens: collectMarkupClasses(await readFile(file, "utf8")) };
    }
    if (!SOURCE_EXTENSIONS.has(path.extname(file))) return null;
    return collectSourceClassEvidence(await readFile(file, "utf8"));
  }));
  const tokens = new Set();
  const interpolationPrefixes = new Set();
  for (const evidence of contributions) {
    if (evidence === null) continue;
    for (const token of evidence.tokens) tokens.add(token);
    for (const prefix of evidence.interpolationPrefixes) interpolationPrefixes.add(prefix);
  }
  return { interpolationPrefixes, tokens };
}

/** True when some literal or interpolation base can produce `className`. */
export function isClassReachable(className, evidence) {
  if (evidence.tokens.has(className)) return true;
  for (const prefix of evidence.interpolationPrefixes) {
    if (className.length > prefix.length && className.startsWith(prefix)) return true;
  }
  return false;
}

/**
 * The text between a call's parentheses, given the index of the opening one.
 *
 * Counting delimiters rather than stopping at the first comma is what lets a
 * path composed inside the call -- `readFileSync(path.join(root, "a.css"))` --
 * be read as one argument list. Masking has already turned every character a
 * path does not need into `·`, so a bracket inside a string literal cannot
 * unbalance the count. Returns `null` for an unterminated call.
 */
function callArgumentList(masked, open) {
  let depth = 0;
  for (let index = open; index < masked.length; index += 1) {
    const character = masked[index];
    if (character === "(") depth += 1;
    else if (character === ")") {
      depth -= 1;
      if (depth === 0) return masked.slice(open + 1, index);
    }
  }
  return null;
}

/** The first argument of an argument list, ignoring commas nested in a call. */
function firstArgument(argumentList) {
  let depth = 0;
  for (let index = 0; index < argumentList.length; index += 1) {
    const character = argumentList[index];
    if (character === "(" || character === "[" || character === "{") depth += 1;
    else if (character === ")" || character === "]" || character === "}") depth -= 1;
    else if (character === "," && depth === 0) return argumentList.slice(0, index);
  }
  return argumentList;
}

/**
 * Locates reads of stylesheet text in one source file.
 *
 * Three shapes the repository has used are covered: a path literal handed
 * straight to a read call, a path bound to a constant a read call dereferences
 * later, and a path composed inside the call itself --
 * `readFileSync(path.join(process.cwd(), "app", "styles", "x.css"), "utf8")`.
 * That last one is why the argument is read by counting brackets instead of by
 * stopping at the first comma: the comma-stopping form saw only `path.join(`
 * and reported nothing, so a nested read sat inside the suite this invariant
 * walks while the invariant stayed green. Handing a path to Playwright's
 * `addStyleTag` is not a read -- the browser loads the file and the assertion
 * is on computed style -- so only calls that hand back the characters count.
 */
export function findStylesheetTextReads(source) {
  const masked = maskStringLiterals(source);
  const stylesheetBindings = new Set();
  for (const match of masked.matchAll(/(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=([^;\n]*)/gu)) {
    if (/\.css\b/u.test(match[2])) stylesheetBindings.add(match[1]);
  }
  const reads = [];
  const readCall = /\b(readFileSync|readFile|createReadStream)\s*\(/gu;
  for (const match of masked.matchAll(readCall)) {
    const argumentList = callArgumentList(masked, match.index + match[0].length - 1);
    if (argumentList === null) continue;
    const argument = firstArgument(argumentList).trim().replaceAll(/\s+/gu, " ");
    if (/\.css\b/u.test(argument) || stylesheetBindings.has(argument)) {
      reads.push(`${match[1]}(${argument.replaceAll(MASK_OPEN, '"').replaceAll(MASK_CLOSE, '"')})`);
    }
  }
  const importPattern = new RegExp(
    `(?:import[^;\\n]*from\\s*|import\\s*|require\\s*\\(\\s*)${MASK_OPEN}([^${MASK_CLOSE}]*\\.css)${MASK_CLOSE}`,
    "gu",
  );
  for (const match of masked.matchAll(importPattern)) reads.push(`import "${match[1]}"`);
  return reads;
}

/** Every stylesheet read a test file performs, keyed by repository path. */
export async function findTestStylesheetReads(root) {
  const perFile = await Promise.all((await listTestFiles(root)).map(async (file) => {
    const reads = findStylesheetTextReads(await readFile(file, "utf8"));
    return reads.map((read) => `${relativePosix(root, file)}: ${read}`);
  }));
  return perFile.flat();
}

/**
 * Where every custom property is declared and read across the styling layer.
 *
 * References come from the stylesheets *and* from the application's own source:
 * a chart palette that writes `"var(--chart-series-7)"` into an inline style
 * reads a token exactly as a rule does, and a rename that misses it renders as
 * an unset property rather than as an error. Reading the source with the same
 * `var()` pattern the stylesheets use keeps both halves under one invariant.
 *
 * Test files are excluded from that scan and only from it: a spec quotes token
 * names it is asserting *about*, including partial ones and deliberately
 * retired ones, so counting those as reads would make the invariant fail for
 * naming a token rather than for using it.
 */
export async function collectCustomPropertyUsage(root) {
  const stylesheets = await Promise.all((await listStylesheets(root)).map(async (file) => ({
    css: await readFile(file, "utf8"),
    name: relativePosix(root, file),
  })));
  const declared = new Set();
  const references = new Map();
  for (const { css, name: file } of stylesheets) {
    for (const name of collectCustomPropertyDefinitions(css)) declared.add(name);
    for (const name of collectCustomPropertyReferences(css)) {
      const sources = references.get(name) ?? [];
      sources.push(file);
      references.set(name, sources);
    }
  }

  const sources = (await listRepositoryFiles(root)).filter((file) => SOURCE_EXTENSIONS.has(path.extname(file)));
  const tests = new Set(await listTestFiles(root));
  const perSource = await Promise.all(sources.map(async (file) => {
    const source = await readFile(file, "utf8");
    return {
      name: relativePosix(root, file),
      read: tests.has(file) ? new Set() : collectCustomPropertyReferences(source),
      written: collectRuntimeCustomProperties(source),
    };
  }));
  const runtimeDeclared = new Set();
  for (const { name: file, read, written } of perSource) {
    for (const name of written) runtimeDeclared.add(name);
    for (const name of read) {
      const seen = references.get(name) ?? [];
      seen.push(file);
      references.set(name, seen);
    }
  }
  return { declared, references, runtimeDeclared };
}

/**
 * Every stylesheet whose class names are global, sorted for stable reporting.
 *
 * That used to be one file. It is now the set `app/styles/index.css` imports,
 * minus the two that write no class name of their own: the token files declare
 * only custom properties. Deriving the set from the import list rather than
 * naming files means splitting a sheet, or adding one, cannot quietly take its
 * rules out of the invariants built on this.
 */
async function listGlobalStylesheets(root) {
  const stylesRoot = path.join(root, "app", "styles");
  const index = await readFile(path.join(stylesRoot, "index.css"), "utf8");
  return [...index.matchAll(/@import\s+url\("([^"]+)"\)/gu)]
    .map((match) => path.join(stylesRoot, match[1]))
    .filter((file) => !/tokens-[a-z0-9-]+\.css$/u.test(file))
    .toSorted((left, right) => left.localeCompare(right));
}

/** Class names the global stylesheets write rules for. */
export async function collectGlobalStylesheetClasses(root) {
  const files = await listGlobalStylesheets(root);
  const perFile = await Promise.all(files.map(async (file) => collectStylesheetClasses(await readFile(file, "utf8"))));
  const classes = new Set();
  for (const found of perFile) for (const className of found) classes.add(className);
  return classes;
}

/**
 * Splits a stylesheet into blocks, each with its prelude, body, and ancestors.
 *
 * `cssBlockPreludes` above answers "which selectors open a block"; the token
 * layer needs the declarations inside one, and needs to know whether the block
 * sits inside a dark-theme at-rule. Nesting is tracked with an explicit stack
 * for the same reason the prelude scanner uses a depth counter: declaration
 * values contain punctuation no regular expression can tell from structure.
 */
export function cssBlocks(source) {
  const css = stripCssComments(source);
  const blocks = [];
  const open = [];
  let start = 0;
  for (let index = 0; index < css.length; index += 1) {
    const character = css[index];
    if (character === "{") {
      open.push({
        ancestors: open.map((entry) => entry.prelude),
        bodyStart: index + 1,
        prelude: css.slice(start, index).trim(),
      });
      start = index + 1;
    } else if (character === "}") {
      const entry = open.pop();
      if (entry !== undefined) {
        blocks.push({
          ancestors: entry.ancestors,
          body: css.slice(entry.bodyStart, index),
          prelude: entry.prelude,
        });
      }
      start = index + 1;
    } else if (character === ";") {
      start = index + 1;
    }
  }
  return blocks;
}

/**
 * The `property: value` pairs a block body declares at its own level.
 *
 * Declarations that belong to a nested rule are skipped: a `:root` block has
 * none, but the parser has to survive a stylesheet that does.
 */
export function cssDeclarations(body) {
  const declarations = [];
  let depth = 0;
  let buffer = "";
  function flush() {
    const text = buffer.trim();
    buffer = "";
    if (depth > 0 || text.length === 0) return;
    const separator = text.indexOf(":");
    if (separator === -1) return;
    declarations.push({
      property: text.slice(0, separator).trim(),
      value: text.slice(separator + 1).trim().replaceAll(/\s+/gu, " "),
    });
  }
  for (const character of body) {
    if (character === "{") {
      depth += 1;
      buffer = "";
    } else if (character === "}") {
      depth = Math.max(0, depth - 1);
      buffer = "";
    } else if (character === ";") {
      flush();
    } else {
      buffer += character;
    }
  }
  flush();
  return declarations;
}

/**
 * The two shapes a dark theme is selected by, and only those: the root
 * attribute the product sets, and the user's own preference. A bare search for
 * the word "dark" would also claim a class named `.dark-row`, and every token
 * inside it would be filed as a restatement -- which is how the
 * one-declaration-per-name invariant built on this could pass while a name
 * really was declared twice.
 */
const DARK_THEME_CONTEXT = /\[data-theme\s*=\s*["']?dark["']?\]|prefers-color-scheme\s*:\s*dark/u;

/** True when a block, or any at-rule around it, targets the dark theme. */
export function isDarkThemeContext(block) {
  return [block.prelude, ...block.ancestors].some((prelude) => DARK_THEME_CONTEXT.test(prelude));
}

/** The token files of the two-tier layer, sorted so reports read the same way. */
export async function listTokenStylesheets(root) {
  const layer = /^app\/styles\/tokens-[a-z0-9-]+\.css$/u;
  return (await listStylesheets(root)).filter((file) => layer.test(relativePosix(root, file)));
}

/**
 * Every custom property the token layer declares, split by theme.
 *
 * Declarations come back as lists rather than maps so a name declared twice in
 * one file stays visible to the caller instead of being silently overwritten.
 */
export async function collectTokenLayer(root) {
  const files = await listTokenStylesheets(root);
  return Promise.all(files.map(async (file) => {
    const css = await readFile(file, "utf8");
    const dark = [];
    const light = [];
    for (const block of cssBlocks(css)) {
      if (block.prelude.startsWith("@")) continue;
      const target = isDarkThemeContext(block) ? dark : light;
      for (const { property, value } of cssDeclarations(block.body)) {
        if (!property.startsWith("--")) continue;
        target.push({ name: property, selector: block.prelude, value });
      }
    }
    return { dark, file: relativePosix(root, file), light };
  }));
}

/**
 * Pairs every custom-property declaration with the comment that trails it.
 *
 * `cssBlocks` strips comments before parsing, which is right for structure and
 * useless for the rule that every token states its role in one line. This scan
 * therefore runs over the raw characters: it finds a declaration, walks to the
 * `;` that ends it, and reports whatever comment sits between that `;` and the
 * end of the line. Declarations come back in source order, and a name declared
 * once per theme appears once per declaration, so a caller can require the
 * comment on every site rather than on whichever site it happened to see last.
 */
export function collectDeclarationComments(css) {
  const found = [];
  for (const match of css.matchAll(/(?:^|[;{\s])(--[\w-]+)\s*:/gu)) {
    const valueStart = match.index + match[0].length;
    const end = css.indexOf(";", valueStart);
    if (end === -1) continue;
    const lineEnd = css.indexOf("\n", end);
    const trailer = css.slice(end + 1, lineEnd === -1 ? css.length : lineEnd);
    const comment = /\/\*(.*?)\*\//su.exec(trailer);
    found.push({ comment: comment === null ? null : comment[1].trim(), name: match[1] });
  }
  return found;
}

/** Every declaration comment in the token layer, keyed by file, in source order. */
export async function collectTokenComments(root) {
  const files = await listTokenStylesheets(root);
  return Promise.all(files.map(async (file) => ({
    declarations: collectDeclarationComments(await readFile(file, "utf8")),
    file: relativePosix(root, file),
  })));
}

/**
 * The blocks the token layer opens, with every declaration they carry.
 *
 * `collectTokenLayer` drops anything that is not a custom property, which is
 * exactly what hides a token file quietly growing real rules. This keeps the
 * selector and the whole declaration list so a caller can insist the layer
 * declares tokens and nothing else.
 */
export async function collectTokenBlocks(root) {
  const files = await listTokenStylesheets(root);
  return Promise.all(files.map(async (file) => {
    const css = await readFile(file, "utf8");
    return {
      blocks: cssBlocks(css).map((block) => ({
        ancestors: block.ancestors,
        declarations: cssDeclarations(block.body),
        prelude: block.prelude,
      })),
      file: relativePosix(root, file),
    };
  }));
}

/**
 * Every reference to a tier-1 primitive from outside the file that declares it.
 *
 * "Nothing outside `app/styles/tokens-color.css` may reference a primitive" is
 * the rule the whole two-tier split rests on, and it is the one rule a
 * screenshot can never see: a rule that reads `--green-700` through a `var()`
 * renders exactly like the semantic token pointing at the same step, right up
 * to the day a theme tries to move it. The primitives are read out of the
 * palette file itself -- a name is a primitive because it holds a literal
 * there, not because of how it is spelled -- so adding a hue family extends the
 * check for free.
 *
 * Test and spec files are skipped: they quote token names inside fixture
 * strings on purpose, and a fixture is not a call site.
 */
export async function collectPrimitiveReferences(root) {
  const palette = (await listTokenStylesheets(root))
    .find((file) => relativePosix(root, file) === "app/styles/tokens-color.css");
  const primitives = new Set();
  if (palette !== undefined) {
    const css = stripCssComments(await readFile(palette, "utf8"));
    for (const match of css.matchAll(/(--[a-z]+-\d+)\s*:\s*#[0-9a-f]{3,8}/giu)) primitives.add(match[1]);
  }
  const tests = new Set(await listTestFiles(root));
  const candidates = (await listRepositoryFiles(root)).filter((file) => (
    file !== palette
    && !tests.has(file)
    && (file.endsWith(".css") || SOURCE_EXTENSIONS.has(path.extname(file)))
  ));
  const perFile = await Promise.all(candidates.map(async (file) => {
    const text = await readFile(file, "utf8");
    const source = file.endsWith(".css") ? stripCssComments(text) : text;
    return [...source.matchAll(/var\(\s*(--[\w-]+)/gu)]
      .filter((match) => primitives.has(match[1]))
      .map((match) => ({ file: relativePosix(root, file), name: match[1] }));
  }));
  return perFile.flat();
}

/**
 * The stylesheets `app/styles/index.css` imports, in cascade order.
 *
 * This is what the application loads: `app/layout.tsx` imports that file and
 * nothing else. It says nothing about the fixture harness -- comparing the two
 * is `listHarnessStylesheets`'s job -- so a caller that wants to know whether
 * the harness agrees has to ask for both lists and compare them.
 *
 * It is the file list of `listIndexImports` and not a second parse of the same
 * file: two parses drifted apart silently, because a spelling one of them
 * missed simply shrank the set it returned rather than failing anywhere.
 */
export async function listInjectedStylesheets(root) {
  return (await listIndexImports(root)).map((entry) => entry.file);
}

/** Where the fixture harness lives, and the name of the function under test. */
const HARNESS_PATH = ["integration", "visual", "application-stylesheets.ts"];
const HARNESS_FUNCTION = "importedStylesheets";

/**
 * The statement text of one top-level `const` or `function` in a source file.
 *
 * Braces and parentheses are counted rather than matched by a regex so a
 * function body that contains either still comes back whole. Throws when the
 * declaration is missing: the caller is asserting *about* this code, and a
 * silent `undefined` would let the assertion pass on a file that no longer
 * contains what it claims to test.
 */
function declarationSource(source, keyword, name) {
  const start = source.search(new RegExp(String.raw`^\s*(?:export\s+)?${keyword}\s+${name}\b`, "mu"));
  if (start < 0) throw new Error(`${HARNESS_PATH.join("/")} no longer declares ${keyword} ${name}`);
  let depth = 0;
  for (let index = start; index < source.length; index += 1) {
    const character = source[index];
    if (character === "{" || character === "(") depth += 1;
    else if (character === "}" || character === ")") {
      depth -= 1;
      if (depth === 0 && character === "}") return source.slice(start, index + 1);
    } else if (character === ";" && depth === 0) return source.slice(start, index + 1);
  }
  throw new Error(`${HARNESS_PATH.join("/")}: ${keyword} ${name} is unterminated`);
}

/**
 * The stylesheets `integration/visual/application-stylesheets.ts` really injects.
 *
 * The harness cannot inject `app/styles/index.css` itself -- an `@import` does
 * not resolve inside an injected `<style>` -- so it reads that file's import
 * list and injects the files one at a time. Restating the parse here would only
 * prove that this module can read `index.css`, which is the shape the invariant
 * had rotted into: it compared `index.css` with `index.css` and stayed green
 * while the harness injected a different set. So the harness's own
 * `importedStylesheets` body is *executed* instead, with its own `stylesRoot`
 * expression evaluated against the harness's directory, and a filter, a slice
 * or a reordered `map` inside it changes this result.
 *
 * Anything that stops the body from evaluating -- a syntax the evaluator cannot
 * run, a renamed function, a moved file -- throws. That is deliberate: a caught
 * error returning an empty list is exactly the silent fallback this function
 * exists to remove.
 */
export async function listHarnessStylesheets(root) {
  const harnessPath = path.join(root, ...HARNESS_PATH);
  const source = await readFile(harnessPath, "utf8");
  const rootExpression = /const\s+stylesRoot\s*=\s*([^;]+);/u.exec(source)?.[1];
  if (rootExpression === undefined) throw new Error(`${HARNESS_PATH.join("/")} no longer binds stylesRoot`);
  const body = declarationSource(source, "function", HARNESS_FUNCTION)
    .replace(/^\s*(?:export\s+)?function\s+\w+\s*\([^)]*\)\s*(?::[^{]+)?\{/u, "")
    .replace(/\}\s*$/u, "");
  const evaluate = new Function("readFileSync", "path", "__dirname", `
    const stylesRoot = ${rootExpression};
    ${body}
  `);
  const injected = evaluate(readFileSync, path, path.dirname(harnessPath));
  return [...injected].map((file) => relativePosix(root, file));
}

/**
 * How the harness turns `importedStylesheets()` into what it exports.
 *
 * Executing the function proves the derivation; this proves nothing was done to
 * the result on its way out, so `importedStylesheets().slice(1)` cannot pass by
 * satisfying the executed half.
 */
export async function readHarnessExportExpression(root) {
  const source = await readFile(path.join(root, ...HARNESS_PATH), "utf8");
  return declarationSource(source, "const", "APPLICATION_STYLESHEETS")
    .replace(/^[^=]*=\s*/u, "")
    .replace(/;$/u, "")
    .trim();
}

/**
 * Consolidation inventory.
 *
 * Phase 3 folded parallel families -- eight status chips, eight badges, three
 * button vocabularies, five tables, six keyframe blocks -- into one primitive
 * each. The collectors below answer the two questions that keeps true: is each
 * primitive still defined exactly once, and does anything still ask for a name
 * the fold retired? Both are text questions about structure, which is why they
 * live here rather than in a test file.
 */

/** A declaration list rendered as a stable, order-independent signature. */
export function declarationSignature(declarations) {
  return declarations.map(({ property, value }) => `${property}: ${value}`).toSorted();
}

/**
 * Every rule in the styling layer that declares at least `minimum` properties.
 *
 * At-rule preludes (`@media`, `@keyframes`, `@property`) are dropped: only
 * selector blocks can restate one another, and a keyframe step is a position on
 * a timeline rather than a rule a second selector could be sharing.
 */
export async function collectDeclarationBlocks(root, minimum) {
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => {
    const css = await readFile(file, "utf8");
    return cssBlocks(css)
      .filter((block) => !block.prelude.startsWith("@"))
      .map((block) => ({
        ancestors: block.ancestors.map((prelude) => prelude.replaceAll(/\s+/gu, " ")),
        declarations: cssDeclarations(block.body),
        file: relativePosix(root, file),
        prelude: block.prelude.replaceAll(/\s+/gu, " "),
      }))
      .filter((block) => block.declarations.length >= minimum);
  }));
  return perFile.flat();
}

/** Human-readable address of a rule: file, enclosing at-rules, selector list. */
export function describeRuleSite(block) {
  return [block.file, ...block.ancestors, block.prelude].join(" :: ");
}

/**
 * Rules whose selector list contains a primitive's bare class selector.
 *
 * "Bare" means the selector is exactly `.name` -- not `.name--modifier`, not
 * `.name:hover`, not `.other .name`. That is the shape that *defines* a
 * primitive, and counting it is how "one implementation of each primitive"
 * becomes checkable: a second bare rule is a second base, wherever it lives.
 * Every stylesheet is read the same way now that Phase 4 left one namespace;
 * a CSS module used to count only through `:global(.name)`, since its own
 * `.name` was scoped to a generated identifier and could not collide.
 */
export async function collectBaseRuleSites(root, classNames) {
  const wanted = new Set(classNames);
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => {
    const css = await readFile(file, "utf8");
    const found = [];
    for (const block of cssBlocks(css)) {
      if (block.prelude.startsWith("@")) continue;
      for (const selector of block.prelude.split(",")) {
        const text = selector.trim().replaceAll(/\s+/gu, " ");
        const name = /^\.([-\w]+)$/u.exec(text)?.[1];
        if (name === undefined || !wanted.has(name)) continue;
        found.push([name, {
          ancestors: block.ancestors.map((prelude) => prelude.replaceAll(/\s+/gu, " ")),
          file: relativePosix(root, file),
          prelude: block.prelude.replaceAll(/\s+/gu, " "),
        }]);
      }
    }
    return found;
  }));
  const sites = new Map([...wanted].map((name) => [name, []]));
  for (const [name, site] of perFile.flat()) sites.get(name).push(site);
  return sites;
}

/** Every `@keyframes` block in the styling layer, as `{ file, name }`. */
export async function collectKeyframeSites(root) {
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => {
    const css = stripCssComments(await readFile(file, "utf8"));
    return [...css.matchAll(/@keyframes\s+([-\w]+)/gu)]
      .map((match) => ({ file: relativePosix(root, file), name: match[1] }));
  }));
  return perFile.flat();
}

/** Longhand and shorthand animation keywords a name can never be confused with. */
const ANIMATION_KEYWORDS = new Set([
  "alternate", "alternate-reverse", "backwards", "both", "ease", "ease-in", "ease-in-out",
  "ease-out", "forwards", "infinite", "inherit", "initial", "linear", "none", "normal",
  "paused", "reverse", "running", "step-end", "step-start", "unset",
]);

/**
 * Every animation name the styling layer asks a rule to play.
 *
 * `animation-name` carries the name alone; the `animation` shorthand carries it
 * among durations, easings and counts, so the reader takes the first token that
 * is neither a keyword nor a quantity. An animation naming a keyframe set that
 * no longer exists renders as no animation at all, in silence, which is exactly
 * what folding six identical keyframe blocks into two risks.
 */
export async function collectAnimationReferences(root) {
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => {
    const css = await readFile(file, "utf8");
    const found = [];
    for (const block of cssBlocks(css)) {
      if (block.prelude.startsWith("@keyframes")) continue;
      for (const { property, value } of cssDeclarations(block.body)) {
        if (property !== "animation" && property !== "animation-name") continue;
        const name = value.split(/[\s,]+/u).find((token) => (
          /^[-A-Za-z_][-\w]*$/u.test(token) && !ANIMATION_KEYWORDS.has(token)
        ));
        if (name === undefined) continue;
        found.push({
          file: relativePosix(root, file),
          name,
          selector: block.prelude.replaceAll(/\s+/gu, " "),
        });
      }
    }
    return found;
  }));
  return perFile.flat();
}

/**
 * Class names a source file writes into a `className` or `class` attribute.
 *
 * `scripts/style-invariants.test.mjs` asks the CSS-to-markup question: does every
 * rule still have a caller? This is the other direction, the one a deletion
 * gets wrong -- does every class the markup asks for still have a rule? Only
 * attribute positions count, so a word that happens to be an API value
 * (`variant="danger"`, a route named `cancel`) is never mistaken for a class.
 *
 * Both attribute spellings are read, and an expression form
 * (`className={cond ? "a" : "b"}`) contributes every literal inside it, since
 * any of them can reach the DOM.
 */
export function collectClassAttributeTokens(source) {
  const tokens = new Set();
  for (const match of source.matchAll(/(?:^|[^-\w.])(?:className|class)\s*=\s*/gu)) {
    let index = match.index + match[0].length;
    let expression = "";
    if (source[index] === "{") {
      let depth = 0;
      const start = index;
      for (; index < source.length; index += 1) {
        if (source[index] === "{") depth += 1;
        else if (source[index] === "}") {
          depth -= 1;
          if (depth === 0) break;
        }
      }
      expression = source.slice(start, index + 1);
    } else if (source[index] === '"' || source[index] === "'" || source[index] === "`") {
      const end = source.indexOf(source[index], index + 1);
      expression = end === -1 ? "" : source.slice(index, end + 1);
    }
    walkSource(expression, () => undefined, (text) => {
      for (const token of text.split(/[^\w-]+/u)) {
        if (token.length > 0) tokens.add(token);
      }
    });
  }
  return tokens;
}

/** Calls that hand a CSS selector to the DOM or to Playwright. */
const SELECTOR_CALL = /\b(?:locator|querySelector|querySelectorAll|waitForSelector|closest|matches)\s*\(\s*/gu;

/**
 * Class names a source file selects on, with the lines each one sits at.
 *
 * A spec that drives the product through `.knowledge-manager__detail` is
 * coupled to the stylesheet's vocabulary just as tightly as a rule is, and a
 * rename that misses it fails as a timeout somewhere far from the cause. Only
 * the first argument of a selector call is read, so an identifier such as
 * `payload.value` can never be mistaken for a class.
 */
export function collectSelectorClassTokens(source) {
  const found = new Map();
  const lineStarts = [0];
  for (let index = 0; index < source.length; index += 1) {
    if (source[index] === "\n") lineStarts.push(index + 1);
  }
  function lineAt(offset) {
    let low = 0;
    let high = lineStarts.length - 1;
    while (low < high) {
      const middle = Math.ceil((low + high) / 2);
      if (lineStarts[middle] <= offset) low = middle;
      else high = middle - 1;
    }
    return low + 1;
  }
  for (const match of source.matchAll(SELECTOR_CALL)) {
    const index = match.index + match[0].length;
    const quote = source[index];
    if (quote !== '"' && quote !== "'" && quote !== "`") continue;
    const end = source.indexOf(quote, index + 1);
    if (end === -1) continue;
    const line = lineAt(index);
    for (const name of source.slice(index + 1, end).matchAll(/\.(-?[_a-zA-Z][\w-]*)/gu)) {
      const lines = found.get(name[1]) ?? new Set();
      lines.add(line);
      found.set(name[1], lines);
    }
  }
  return new Map([...found].map(([name, lines]) => [name, [...lines].toSorted((left, right) => left - right)]));
}

/** Every class the styling layer can match, across every stylesheet it ships. */
export async function collectStyledClasses(root) {
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => (
    Array.from(collectStylesheetClasses(await readFile(file, "utf8")))
  )));
  return new Set(perFile.flat());
}

/** Application and harness sources: everything a class name can be written in. */
export async function listSourceFiles(root) {
  return (await listRepositoryFiles(root)).filter((file) => SOURCE_EXTENSIONS.has(path.extname(file)));
}

/** Module specifiers a source file imports from, in source order. */
export function collectImportSpecifiers(source) {
  const pattern = new RegExp(
    `\\bimport\\b[^;\\n]*?from\\s*${MASK_OPEN}([^${MASK_CLOSE}]*)${MASK_CLOSE}`,
    "gu",
  );
  return [...maskStringLiterals(source).matchAll(pattern)].map((match) => match[1]);
}

/* == 2. The split ========================================================== */

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
 * the scanner hides nothing: it imports no stylesheet and reads no `styles`
 * object, and a module that came back would still be a file, which the file
 * walk sees whatever this list says.
 */
const SCANNER_MODULES = new Set(["scripts/style-inventory.mjs"]);

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
function ruleParts(signature) {
  const [at = "", selector = "", declarations = ""] = signature.split(" || ");
  return { at, declarations, selector };
}

/**
 * The `@import` list of `app/styles/index.css`, in the order it states them.
 *
 * The order is the cascade contract -- tokens, base, primitives, features,
 * interaction -- so it is returned rather than sorted, and a caller that wants a
 * set can build one. Paths are repository-relative POSIX so they compare
 * directly against everything the layer section returns.
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
 * carve-out `collectCustomPropertyUsage` documents: `style-invariants.test.mjs`
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

/* == 3. The sweep ========================================================== */

/** Sources that can carry a colour into the DOM through an inline style. */
const TYPESCRIPT_EXTENSIONS = new Set([".ts", ".tsx"]);

/**
 * A colour written as a literal rather than as a role.
 *
 * Hex covers `#rgb` through `#rrggbbaa`; the function list covers every colour
 * function CSS has, because "we only ever write `rgb()`" is a convention and
 * this is meant to be a rule. The lookbehind keeps `--purple-500` and
 * `translate(...)` out: a colour function is a bare word, never the tail of an
 * identifier.
 */
const COLOUR_LITERAL = /#[0-9a-fA-F]{3,8}\b|(?<![\w-])(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\(/u;

/**
 * CSS's named colours.
 *
 * A named colour is a literal exactly as much as a hex is, and it is the one
 * shape a search for `#` or `rgb(` misses entirely. `transparent` and
 * `currentcolor` are deliberately absent: neither names a colour, they name
 * "no paint" and "whatever the ink already is", and both survive a retheme.
 */
const NAMED_COLOURS = new Set([
  "aliceblue", "antiquewhite", "aqua", "aquamarine", "azure", "beige", "bisque", "black",
  "blanchedalmond", "blue", "blueviolet", "brown", "burlywood", "cadetblue", "chartreuse",
  "chocolate", "coral", "cornflowerblue", "cornsilk", "crimson", "cyan", "darkblue", "darkcyan",
  "darkgoldenrod", "darkgray", "darkgreen", "darkgrey", "darkkhaki", "darkmagenta", "darkolivegreen",
  "darkorange", "darkorchid", "darkred", "darksalmon", "darkseagreen", "darkslateblue",
  "darkslategray", "darkslategrey", "darkturquoise", "darkviolet", "deeppink", "deepskyblue",
  "dimgray", "dimgrey", "dodgerblue", "firebrick", "floralwhite", "forestgreen", "fuchsia",
  "gainsboro", "ghostwhite", "gold", "goldenrod", "gray", "green", "greenyellow", "grey",
  "honeydew", "hotpink", "indianred", "indigo", "ivory", "khaki", "lavender", "lavenderblush",
  "lawngreen", "lemonchiffon", "lightblue", "lightcoral", "lightcyan", "lightgoldenrodyellow",
  "lightgray", "lightgreen", "lightgrey", "lightpink", "lightsalmon", "lightseagreen",
  "lightskyblue", "lightslategray", "lightslategrey", "lightsteelblue", "lightyellow", "lime",
  "limegreen", "linen", "magenta", "maroon", "mediumaquamarine", "mediumblue", "mediumorchid",
  "mediumpurple", "mediumseagreen", "mediumslateblue", "mediumspringgreen", "mediumturquoise",
  "mediumvioletred", "midnightblue", "mintcream", "mistyrose", "moccasin", "navajowhite", "navy",
  "oldlace", "olive", "olivedrab", "orange", "orangered", "orchid", "palegoldenrod", "palegreen",
  "paleturquoise", "palevioletred", "papayawhip", "peachpuff", "peru", "pink", "plum", "powderblue",
  "purple", "rebeccapurple", "red", "rosybrown", "royalblue", "saddlebrown", "salmon", "sandybrown",
  "seagreen", "seashell", "sienna", "silver", "skyblue", "slateblue", "slategray", "slategrey",
  "snow", "springgreen", "steelblue", "tan", "teal", "thistle", "tomato", "turquoise", "violet",
  "wheat", "white", "whitesmoke", "yellow", "yellowgreen",
]);

/** Properties that measure a gap and are therefore on the spacing scale. */
const SPACING_PROPERTY =
  /^(?:padding|margin|gap|row-gap|column-gap)(?:-(?:top|bottom|left|right|block|inline|block-start|block-end|inline-start|inline-end))?$/u;

/** The four scale families the phase bar names explicitly, beside spacing. */
const SCALE_PROPERTY = /^(?:font-size|z-index|border-radius|box-shadow)$/u;

/**
 * Which token families a property is allowed to name.
 *
 * The key is the property; the value is the set of *kinds* -- derived from what
 * a token resolves to, not from how it is spelled -- that the property accepts.
 * Longhands are listed rather than shorthands wherever the shorthand mixes
 * kinds, because `border: 1px solid var(--x)` legitimately carries a length and
 * a colour and can prove nothing on its own. `background` is the exception that
 * has to be listed anyway: it is the only way to paint a gradient, and a
 * gradient's stops are inks drawn as a ground.
 */
const PROPERTY_KINDS = new Map(Object.entries({
  "accent-color": ["colour"],
  "animation": ["duration", "easing"],
  "animation-duration": ["duration"],
  "animation-timing-function": ["easing"],
  "background": ["colour", "length"],
  "background-color": ["colour"],
  "border": ["colour", "length"],
  "border-block": ["colour", "length"],
  "border-block-end": ["colour", "length"],
  "border-block-start": ["colour", "length"],
  "border-bottom": ["colour", "length"],
  "border-bottom-color": ["colour"],
  "border-bottom-left-radius": ["length"],
  "border-bottom-right-radius": ["length"],
  "border-color": ["colour"],
  "border-inline": ["colour", "length"],
  "border-inline-end": ["colour", "length"],
  "border-inline-start": ["colour", "length"],
  "border-left": ["colour", "length"],
  "border-left-color": ["colour"],
  "border-radius": ["length"],
  "border-right": ["colour", "length"],
  "border-right-color": ["colour"],
  "border-top": ["colour", "length"],
  "border-top-color": ["colour"],
  "border-top-left-radius": ["length"],
  "border-top-right-radius": ["length"],
  "box-shadow": ["colour", "length", "shadow"],
  "caret-color": ["colour"],
  "color": ["colour"],
  "column-gap": ["length"],
  "column-rule-color": ["colour"],
  "fill": ["colour"],
  "font-family": ["font-stack"],
  "font-size": ["length"],
  "gap": ["length"],
  "margin": ["length"],
  "margin-bottom": ["length"],
  "margin-left": ["length"],
  "margin-right": ["length"],
  "margin-top": ["length"],
  "outline": ["colour", "length"],
  "outline-color": ["colour"],
  "padding": ["length"],
  "padding-block": ["length"],
  "padding-bottom": ["length"],
  "padding-inline": ["length"],
  "padding-left": ["length"],
  "padding-right": ["length"],
  "padding-top": ["length"],
  "row-gap": ["length"],
  "scrollbar-color": ["colour"],
  "stroke": ["colour"],
  "text-decoration": ["colour", "length"],
  "text-decoration-color": ["colour"],
  "transition": ["duration", "easing"],
  "transition-duration": ["duration"],
  "transition-timing-function": ["easing"],
  "z-index": ["number"],
}));

/** Repository-relative paths of every stylesheet that is not the token layer. */
export async function listNonTokenStylesheets(root) {
  const layer = new Set((await listTokenStylesheets(root)).map((file) => relativePosix(root, file)));
  return (await listStylesheets(root))
    .filter((file) => !layer.has(relativePosix(root, file)));
}

/**
 * Every declaration the application stylesheets make, with its context.
 *
 * One pass, reused by every collector below, so a stylesheet is read once and
 * every invariant sees exactly the same declarations. The selector is
 * whitespace-collapsed because a selector list spans lines in the source and a
 * failure message has to fit on one.
 */
async function collectApplicationDeclarations(root) {
  const files = await listNonTokenStylesheets(root);
  const perFile = await Promise.all(files.map(async (file) => {
    const css = await readFile(file, "utf8");
    return cssBlocks(css).flatMap((block) => cssDeclarations(block.body).map((declaration) => ({
      ancestors: block.ancestors,
      file: relativePosix(root, file),
      property: declaration.property,
      selector: block.prelude.replaceAll(/\s+/gu, " "),
      value: declaration.value,
    })));
  }));
  return perFile.flat();
}

/** Splits a declaration value into its top-level, comma- and space-free parts. */
export function valueComponents(value) {
  const parts = [];
  let buffer = "";
  let depth = 0;
  for (const character of value) {
    if (character === "(") depth += 1;
    if (character === ")") depth = Math.max(0, depth - 1);
    if (depth === 0 && /[\s,]/u.test(character)) {
      if (buffer.length > 0) parts.push(buffer);
      buffer = "";
      continue;
    }
    buffer += character;
  }
  if (buffer.length > 0) parts.push(buffer);
  return parts;
}

/** A declaration value with any `!important` flag removed. */
export function withoutImportant(value) {
  return value.replace(/\s*!\s*important\s*$/iu, "").trim();
}

/**
 * True when a value paints a colour the token layer did not name.
 *
 * A named colour only counts in a component of its own: `white-space: nowrap`
 * and `linear-gradient(…)` both contain colour words inside longer
 * identifiers, and neither paints anything.
 */
export function hasColourLiteral(value) {
  if (COLOUR_LITERAL.test(value)) return true;
  return valueComponents(value).some((part) => NAMED_COLOURS.has(part.toLowerCase()));
}

/**
 * The light-theme value of every custom property the token layer declares.
 *
 * Dark blocks are skipped: a theme restates tier 2, and the question every
 * caller here asks -- "does this literal already have a name?" -- is asked
 * against the theme the product actually ships. `var()` chains are followed to
 * the primitive, so a tier-2 token and the literal it replaced are directly
 * comparable.
 */
export async function collectTokenValues(root) {
  const files = await listTokenStylesheets(root);
  const sources = await Promise.all(files.map((file) => readFile(file, "utf8")));
  const declared = new Map();
  for (const css of sources) {
    for (const block of cssBlocks(css)) {
      if (block.prelude.startsWith("@") || isDarkThemeContext(block)) continue;
      for (const { property, value } of cssDeclarations(block.body)) {
        if (property.startsWith("--")) declared.set(property, value);
      }
    }
  }
  const resolved = new Map();
  function resolve(name, seen) {
    if (resolved.has(name)) return resolved.get(name);
    if (seen.has(name)) return declared.get(name);
    seen.add(name);
    const value = (declared.get(name) ?? "").replaceAll(
      /var\(\s*(--[\w-]+)\s*\)/gu,
      (whole, reference) => (declared.has(reference) ? resolve(reference, seen) : whole),
    );
    resolved.set(name, value);
    return value;
  }
  for (const name of declared.keys()) resolve(name, new Set());
  return { declared, resolved };
}

/** `#abc` and `#AABBCC` written the one way, so two spellings compare equal. */
export function normaliseHex(hex) {
  const body = hex.slice(1).toLowerCase();
  if (body.length === 3) return `#${body[0]}${body[0]}${body[1]}${body[1]}${body[2]}${body[2]}`;
  if (body.length === 4) {
    return `#${body[0]}${body[0]}${body[1]}${body[1]}${body[2]}${body[2]}${body[3]}${body[3]}`;
  }
  return `#${body}`;
}

/**
 * What kind of value a token holds, read off the value rather than the name.
 *
 * Deriving the kind from the value is what makes the property/token pairing
 * check survive a rename: `--type-md` is a length because it resolves to
 * `12px`, not because it is spelled `--type-`. The shadow case is the one that
 * needs structure as well as shape -- an elevation is several lengths and a
 * colour -- so it is recognised last, as "more than one component, at least one
 * of which paints".
 */
export function tokenKind(value) {
  const text = value.trim();
  if (/^#[0-9a-fA-F]{3,8}$/u.test(text)) return "colour";
  if (/^(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\([^()]*\)$/u.test(text)) return "colour";
  if (NAMED_COLOURS.has(text.toLowerCase())) return "colour";
  if (/^-?\d+(?:\.\d+)?(?:px|rem|em|ch|vh|vw|%)$/u.test(text)) return "length";
  if (/^-?\d+(?:\.\d+)?$/u.test(text)) return "number";
  if (/^\d+(?:\.\d+)?m?s$/u.test(text)) return "duration";
  if (/^(?:linear|ease|ease-in|ease-out|ease-in-out|steps\(.*\)|cubic-bezier\(.*\))$/u.test(text)) {
    return "easing";
  }
  if (/(?:sans-serif|serif|monospace|system-ui|cursive|fantasy)$/u.test(text)) return "font-stack";
  if (valueComponents(text).length > 1 && COLOUR_LITERAL.test(text)) return "shadow";
  return "unknown";
}

/** Kinds a property accepts, or `null` when the sweep makes no claim about it. */
export function acceptedKinds(property) {
  return PROPERTY_KINDS.get(property) ?? null;
}

/**
 * Declarations outside the token layer that still paint a literal colour.
 *
 * Reported as `property: value` per file rather than per rule: two rules that
 * share a literal share one entry, because what the ledger records is a colour
 * the palette has no name for, not a line number that moves whenever anything
 * above it is edited.
 */
export async function collectColourLiterals(root) {
  const grouped = new Map();
  for (const declaration of await collectApplicationDeclarations(root)) {
    if (!hasColourLiteral(declaration.value)) continue;
    const perFile = grouped.get(declaration.file) ?? new Map();
    const perProperty = perFile.get(declaration.property) ?? new Set();
    perProperty.add(declaration.value);
    perFile.set(declaration.property, perProperty);
    grouped.set(declaration.file, perFile);
  }
  return groupedToLedger(grouped);
}

/**
 * Colours hidden inside a percent-encoded `data:` URI.
 *
 * `fill='%23526068'` is a hex that no search for `#` will ever find and that no
 * `var()` can reach, because the SVG is a separate document the browser parses
 * out of a URL. It is the one shape that can grow without any of the checks
 * above noticing, so it is collected on its own and pinned to the sites that
 * are known and written down.
 */
export async function collectEncodedColourLiterals(root) {
  const found = [];
  for (const declaration of await collectApplicationDeclarations(root)) {
    for (const match of declaration.value.matchAll(/%23[0-9a-fA-F]{3,8}\b/gu)) {
      found.push(`${declaration.file} | ${declaration.selector} | ${declaration.property}: ${match[0]}`);
    }
  }
  return found.toSorted();
}

/**
 * True when a value component is a measurement the scale layer could name.
 *
 * Components, not whole values, because a `box-shadow` is several lengths and
 * an ink and a `border-radius` can be four corners: recording the whole string
 * would file `0 0 0 1px var(--border-focus)` -- a ring whose only colour is
 * already a token -- as unmigrated, and the ledger would stop meaning anything.
 * Zero is excluded because it measures nothing and is the same length on every
 * scale; a `var()` is excluded because it is the migrated case; a colour is
 * excluded because the colour ledger already carries it with its full context.
 */
export function isScaleLiteral(component) {
  if (component.startsWith("var(")) return false;
  if (hasColourLiteral(component)) return false;
  if (/^0(?:px|rem|em|%)?$/u.test(component)) return false;
  return /\d/u.test(component);
}

/**
 * The measurements outside the token layer that are still written as numbers.
 *
 * Recorded per file and property as the distinct components, so
 * `font-size: 13px` in twelve rules is one row and the ledger reads as "these
 * numbers are still hard-coded here" rather than as a transcript.
 */
export async function collectScaleLiterals(root) {
  const grouped = new Map();
  for (const declaration of await collectApplicationDeclarations(root)) {
    if (!SCALE_PROPERTY.test(declaration.property)) continue;
    const literals = valueComponents(withoutImportant(declaration.value)).filter(isScaleLiteral);
    if (literals.length === 0) continue;
    const perFile = grouped.get(declaration.file) ?? new Map();
    const perProperty = perFile.get(declaration.property) ?? new Set();
    for (const literal of literals) perProperty.add(literal);
    perFile.set(declaration.property, perProperty);
    grouped.set(declaration.file, perFile);
  }
  return groupedToLedger(grouped);
}

/** Turns the nested Maps the collectors build into sorted, comparable JSON. */
function groupedToLedger(grouped) {
  const ledger = {};
  for (const [file, perFile] of [...grouped].toSorted()) {
    ledger[file] = Object.fromEntries(
      [...perFile].toSorted().map(([property, values]) => [property, [...values].toSorted()]),
    );
  }
  return ledger;
}

/**
 * Literals a token already spells exactly, which are therefore pure misses.
 *
 * This is the sharpest thing the sweep can be asked, and the only one whose
 * answer needs no judgement: if a declaration writes `8px` where `--space-2`
 * resolves to `8px`, substituting the token moves nothing at all. Anything
 * further away is a decision about hue or density and belongs in the ledger.
 *
 * Two scopes, because the families differ in how a value is built. Spacing is
 * checked component by component -- `padding: 3px 8px` is two lengths and the
 * second one has a name -- while the rest are checked whole, because a length
 * inside `box-shadow` or `border-radius: 1px 1px 0 0` measures a geometry the
 * spacing scale makes no claim about.
 *
 * `z-index` is excluded by name. docs/theming.md keeps twenty-three of them
 * literal on purpose: they order an element inside a stacking context its
 * parent already opened, so `--z-base` would promise an escape they cannot
 * make, and five of those happen to be `1`.
 */
export async function collectExactTokenMisses(root) {
  const { resolved } = await collectTokenValues(root);
  /** The family a property measures on, so `width: 14px` never meets `--type-lg`. */
  const families = new Map([
    ["border-bottom-left-radius", "--radius-"],
    ["border-bottom-right-radius", "--radius-"],
    ["border-radius", "--radius-"],
    ["border-top-left-radius", "--radius-"],
    ["border-top-right-radius", "--radius-"],
    ["box-shadow", "--shadow-"],
    ["font-size", "--type-"],
  ]);
  const steps = new Map();
  for (const [name, value] of resolved) {
    const prefix = /^(--[a-z]+-)/u.exec(name)?.[1];
    if (prefix === undefined) continue;
    const perFamily = steps.get(prefix) ?? new Map();
    perFamily.set(value.trim(), name);
    steps.set(prefix, perFamily);
  }
  const spacingSteps = steps.get("--space-") ?? new Map();
  const misses = [];
  for (const declaration of await collectApplicationDeclarations(root)) {
    const { file, property, selector, value } = declaration;
    const bare = withoutImportant(value);
    if (SPACING_PROPERTY.test(property)) {
      // One entry per length, not per position: `padding: 8px 10px 8px 13px`
      // is one substitution to make, written twice.
      for (const component of new Set(valueComponents(bare))) {
        const token = spacingSteps.get(component);
        if (token !== undefined) {
          misses.push({ file, property, selector, token, value, wrote: component });
        }
      }
      continue;
    }
    const family = families.get(property);
    if (family === undefined) continue;
    const token = (steps.get(family) ?? new Map()).get(bare);
    if (token !== undefined) misses.push({ file, property, selector, token, value, wrote: bare });
  }
  return misses.toSorted((left, right) => (
    `${left.file}${left.selector}${left.property}`.localeCompare(`${right.file}${right.selector}${right.property}`)
  ));
}

/**
 * Declarations that name a token of a kind the property cannot carry.
 *
 * The sweep's stated method was to substitute per property role -- a border
 * literal may only become a border token, a background only a ground. Nothing
 * enforced the weaker half of that claim: that a `font-size` never reads a
 * colour and a `color` never reads a step. A mistake of that shape does not
 * render as a wrong colour, it renders as no rule at all, which no screenshot
 * of a page full of text reliably shows.
 */
export async function collectPropertyKindMismatches(root) {
  const { resolved } = await collectTokenValues(root);
  const mismatches = [];
  for (const declaration of await collectApplicationDeclarations(root)) {
    const accepted = acceptedKinds(declaration.property);
    if (accepted === null) continue;
    for (const match of declaration.value.matchAll(/var\(\s*(--[\w-]+)/gu)) {
      const value = resolved.get(match[1]);
      if (value === undefined) continue;
      const kind = tokenKind(value);
      if (kind === "unknown" || accepted.includes(kind)) continue;
      mismatches.push(
        `${declaration.file} | ${declaration.selector} | ${declaration.property}: ${match[1]} `
        + `is a ${kind} (${value.trim()}), and ${declaration.property} accepts ${accepted.join(" or ")}`,
      );
    }
  }
  return mismatches.toSorted();
}

/** Every TypeScript module the application ships, tests excluded. */
async function listApplicationTypeScript(root) {
  return (await listRepositoryFiles(root)).filter((file) => (
    TYPESCRIPT_EXTENSIONS.has(path.extname(file)) && !/\.(?:test|spec)\.tsx?$/u.test(file)
  ));
}

/**
 * Colour literals TypeScript carries, with the comments stripped.
 *
 * A hex in a component is worse than a hex in a stylesheet: it reaches the DOM
 * through an inline `style`, where it outranks every rule and no theme can
 * touch it. Comments are removed first so prose about a colour -- which the
 * visual harness has, explaining why two blues compare equal -- is not read as
 * one.
 */
export async function collectTypeScriptColourLiterals(root) {
  const files = await listApplicationTypeScript(root);
  const perFile = await Promise.all(files.map(async (file) => {
    const source = (await readFile(file, "utf8"))
      .replaceAll(/\/\*[\s\S]*?\*\//gu, " ")
      .replaceAll(/(^|[^:])\/\/[^\n]*/gu, "$1");
    const found = new Set();
    for (const match of source.matchAll(/#[0-9a-fA-F]{3,8}\b/gu)) found.add(match[0]);
    for (const match of source.matchAll(/(?<![\w-])(?:rgba?|hsla?)\([^)]*\)/gu)) found.add(match[0]);
    return [...found].toSorted().map((literal) => `${relativePosix(root, file)}: ${literal}`);
  }));
  return perFile.flat().toSorted();
}

/** Every `var(--token)` TypeScript reads, with the file that reads it. */
export async function collectTypeScriptTokenReferences(root) {
  const files = await listApplicationTypeScript(root);
  const perFile = await Promise.all(files.map(async (file) => {
    const source = await readFile(file, "utf8");
    return [...source.matchAll(/var\(\s*(--[\w-]+)\s*\)/gu)]
      .map((match) => ({ file: relativePosix(root, file), name: match[1] }));
  }));
  return perFile.flat();
}

/**
 * The token each event severity is painted with, read from both languages.
 *
 * The legend swatch is a CSS class and the chart swatch is an inline style
 * built in TypeScript, so the same four levels are spelled twice in two
 * different files. Nothing but this comparison makes them agree: a legend that
 * disagrees with its own chart is a wrong colour, not a missing one, and the
 * page still renders.
 */
export async function collectSeverityPalette(root) {
  const stylesheet = {};
  for (const declaration of await collectApplicationDeclarations(root)) {
    if (declaration.property !== "background" && declaration.property !== "background-color") continue;
    const token = /^var\(\s*(--[\w-]+)\s*\)$/u.exec(declaration.value);
    if (token === null) continue;
    for (const match of declaration.selector.matchAll(/\.(?:severity|legend|level)-(info|warn|error|debug)\b/gu)) {
      const seen = stylesheet[match[1]] ?? new Set();
      seen.add(token[1]);
      stylesheet[match[1]] = seen;
    }
  }
  const panel = path.join(root, "app", "search-workspace", "panels", "visualization-panel.tsx");
  const source = await readFile(panel, "utf8");
  const body = /function categoryColor\([\s\S]*?\n\}/u.exec(source);
  const typescript = {};
  for (const match of (body?.[0] ?? "").matchAll(/(\w+):\s*"var\(\s*(--[\w-]+)\s*\)"/gu)) {
    typescript[match[1]] = match[2];
  }
  return {
    stylesheet: Object.fromEntries(
      Object.entries(stylesheet).toSorted().map(([level, names]) => [level, [...names].toSorted()]),
    ),
    typescript,
  };
}

/**
 * The categorical ramp as `time-series-line-chart.tsx` declares it.
 *
 * Returned as written, in order, so a caller can insist it is the chart-series
 * family and nothing else: a single stray hex in that array reaches every chart
 * in the product through an inline style.
 */
export async function collectSeriesPalette(root) {
  const chart = path.join(root, "app", "search-workspace", "charts", "time-series-line-chart.tsx");
  const source = await readFile(chart, "utf8");
  const array = /export const TIME_SERIES_COLORS = \[([\s\S]*?)\]/u.exec(source);
  const entries = [...(array?.[1] ?? "").matchAll(/"([^"]*)"/gu)].map((match) => match[1]);
  const panel = path.join(root, "app", "search-workspace", "panels", "visualization-panel.tsx");
  const panelSource = await readFile(panel, "utf8");
  const slice = /const CATEGORY_COLORS = TIME_SERIES_COLORS\.slice\(0,\s*(\d+)\)/u.exec(panelSource);
  return { categoryCount: slice === null ? null : Number(slice[1]), series: entries };
}
