/**
 * Static inventory of the stylesheets and of how the application refers to them.
 *
 * The CSS cleanup replaces assertions on stylesheet text with assertions on
 * rendered behaviour, so nothing that runs under `node --test` may read a
 * stylesheet and assert on its characters. This module does the reading
 * instead: it is a plain library rather than a test file, so
 * `scripts/css-invariants.test.mjs` can assert on structure without
 * reintroducing the coupling the cleanup is removing.
 *
 * Everything here is pure text analysis. It never starts a browser, never
 * builds, and never looks outside the repository root it is handed, so the
 * invariants built on it stay deterministic and fast.
 */
import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";

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
export function collectCustomPropertyReferences(css) {
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
export function walkSource(source, onCode, onLiteral) {
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
export function collectMarkupClasses(markup) {
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
 * CSS module files contribute only their `:global(...)` selectors: those name
 * global classes directly, while a module's own classes are reached through a
 * generated `styles` object and never enter the global namespace.
 */
export async function collectClassEvidence(root) {
  const files = await listRepositoryFiles(root);
  const contributions = await Promise.all(files.map(async (file) => {
    if (file.endsWith(".css")) {
      if (!file.endsWith(".module.css")) return null;
      const css = stripCssComments(await readFile(file, "utf8"));
      const global = new Set();
      for (const match of css.matchAll(/:global\(([^)]*)\)/gu)) {
        for (const selector of match[1].matchAll(/\.(-?[_a-zA-Z][\w-]*)/gu)) global.add(selector[1]);
      }
      return { interpolationPrefixes: new Set(), tokens: global };
    }
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
 * Locates reads of stylesheet text in one source file.
 *
 * Both shapes the repository has used are covered: a path literal handed
 * straight to a read call, and a path bound to a constant a read call
 * dereferences later. Handing a path to Playwright's `addStyleTag` is not a
 * read -- the browser loads the file and the assertion is on computed style --
 * so only calls that hand back the characters count.
 */
export function findStylesheetTextReads(source) {
  const masked = maskStringLiterals(source);
  const stylesheetBindings = new Set();
  for (const match of masked.matchAll(/(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=([^;\n]*)/gu)) {
    if (/\.css\b/u.test(match[2])) stylesheetBindings.add(match[1]);
  }
  const reads = [];
  const readCall = /\b(readFileSync|readFile|createReadStream)\s*\(\s*([^,)]*)/gu;
  for (const match of masked.matchAll(readCall)) {
    const argument = match[2].trim();
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
export async function listGlobalStylesheets(root) {
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
 * The stylesheets `integration/visual/application-stylesheets.ts` injects.
 *
 * The harness cannot inject `app/styles/index.css` itself: an `@import` cannot
 * be resolved inside an injected `<style>`. It therefore reads that file's
 * import list and injects the files one at a time, and this rebuilds the same
 * list so a test can assert the harness really derives it. A hand-written copy
 * would drift the moment the layer gained a file, and the fixtures would go on
 * rendering unresolved `var()` fallbacks with every contract green -- so an
 * empty result, meaning the harness no longer names `index.css` at all, is the
 * failure the caller is looking for.
 */
export async function listInjectedStylesheets(root) {
  const stylesRoot = path.join(root, "app", "styles");
  const harness = await readFile(path.join(root, "integration", "visual", "application-stylesheets.ts"), "utf8");
  if (!/index\.css/u.test(harness)) return [];
  const index = await readFile(path.join(stylesRoot, "index.css"), "utf8");
  return [...index.matchAll(/@import\s+url\("([^"]+)"\)/gu)]
    .map((match) => relativePosix(root, path.join(stylesRoot, match[1])));
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
 * CSS modules contribute only through `:global(.name)`, since a module's own
 * `.name` is scoped to a generated identifier and cannot collide.
 */
export async function collectBaseRuleSites(root, classNames) {
  const wanted = new Set(classNames);
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => {
    const scoped = file.endsWith(".module.css");
    const css = await readFile(file, "utf8");
    const found = [];
    for (const block of cssBlocks(css)) {
      if (block.prelude.startsWith("@")) continue;
      for (const selector of block.prelude.split(",")) {
        const text = selector.trim().replaceAll(/\s+/gu, " ");
        const name = scoped
          ? /^:global\(\s*\.([-\w]+)\s*\)$/u.exec(text)?.[1]
          : /^\.([-\w]+)$/u.exec(text)?.[1];
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
 * `scripts/css-invariants.test.mjs` asks the CSS-to-markup question: does every
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

/** Every class the styling layer can match, including a module's `:global()`. */
export async function collectStyledClasses(root) {
  const perFile = await Promise.all((await listStylesheets(root)).map(async (file) => {
    const css = await readFile(file, "utf8");
    if (!file.endsWith(".module.css")) return Array.from(collectStylesheetClasses(css));
    const global = [];
    for (const match of stripCssComments(css).matchAll(/:global\(([^)]*)\)/gu)) {
      for (const selector of match[1].matchAll(/\.(-?[_a-zA-Z][\w-]*)/gu)) global.push(selector[1]);
    }
    return global;
  }));
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
