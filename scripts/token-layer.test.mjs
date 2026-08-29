// Structural invariants for the two-tier token layer in `app/styles`.
//
// The layer only pays for itself if it stays a layer: one declaration site per
// name, a semantic tier that names primitives rather than restating their
// literals, and a dark theme that redefines names the light theme already has.
// Each of those is invisible to a screenshot -- a token layer can rot into a
// second pile of literals while every pixel stays identical -- so it is
// asserted here.
//
// Like `css-invariants.test.mjs`, this file never opens a stylesheet: all
// reading and parsing lives in `scripts/css-inventory.mjs`, so the rule that no
// test may read stylesheet text keeps holding.
import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

import {
  collectPrimitiveReferences,
  collectTokenLayer,
  cssBlocks,
  cssDeclarations,
  isDarkThemeContext,
  listInjectedStylesheets,
  listStylesheets,
  listTokenStylesheets,
  relativePosix,
} from "./css-inventory.mjs";

const workspace = process.cwd();

/** A colour written as characters rather than as a reference to a token. */
const COLOUR_LITERAL = /#[0-9a-f]{3,8}\b|\b(?:rgba?|hsla?|oklch|lab)\(/iu;

/** Names a value reads through `var()`. */
function referencedTokens(value) {
  return [...value.matchAll(/var\(\s*(--[\w-]+)/gu)].map((match) => match[1]);
}

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

test("the token layer is reachable and carries the scale tier", async () => {
  const layer = await collectTokenLayer(workspace);
  const files = layer.map((entry) => entry.file);
  assert.ok(
    files.includes("app/styles/tokens-scale.css"),
    `the walker no longer reaches the scale tokens, so every assertion below is vacuous: ${files.join(", ")}`,
  );
  assert.ok(
    files.includes("app/styles/tokens-color.css"),
    `the walker no longer reaches the colour tokens: ${files.join(", ")}`,
  );
  const scale = layer.find((entry) => entry.file === "app/styles/tokens-scale.css");
  assert.ok(
    scale.light.length > 20,
    `app/styles/tokens-scale.css declares only ${scale.light.length} tokens; the scales are missing`,
  );
});

test("no token name is declared in more than one place", async () => {
  const layer = await collectTokenLayer(workspace);
  const sites = new Map();
  for (const entry of layer) {
    for (const token of entry.light) {
      sites.set(token.name, [...(sites.get(token.name) ?? []), `${entry.file} (${token.selector})`]);
    }
  }
  const duplicated = [...sites.entries()]
    .filter(([, places]) => places.length > 1)
    .map(([name, places]) => `${name} declared in ${places.join(" and ")}`)
    .toSorted();
  assert.deepEqual(
    duplicated,
    [],
    "A token has two declaration sites, so which value wins depends on file order rather than on\n"
      + `intent. Keep one declaration per name:\n${describeList(duplicated)}`,
  );
});

test("every token reference inside the layer resolves within the layer", async () => {
  const layer = await collectTokenLayer(workspace);
  const declared = new Set();
  for (const entry of layer) {
    for (const token of [...entry.light, ...entry.dark]) declared.add(token.name);
  }
  const dangling = [];
  for (const entry of layer) {
    for (const token of [...entry.light, ...entry.dark]) {
      for (const reference of referencedTokens(token.value)) {
        if (!declared.has(reference)) dangling.push(`${entry.file}: ${token.name} reads ${reference}`);
      }
    }
  }
  assert.deepEqual(
    dangling.toSorted(),
    [],
    "A token reads a name the token layer does not declare, so the layer is not self-contained and\n"
      + `the value silently resolves to nothing:\n${describeList(dangling.toSorted())}`,
  );
});

test("a token that names another token carries no colour literal of its own", async () => {
  const layer = await collectTokenLayer(workspace);
  const mixed = [];
  for (const entry of layer) {
    for (const token of [...entry.light, ...entry.dark]) {
      if (referencedTokens(token.value).length === 0) continue;
      if (COLOUR_LITERAL.test(token.value)) mixed.push(`${entry.file}: ${token.name}: ${token.value}`);
    }
  }
  assert.deepEqual(
    mixed.toSorted(),
    [],
    "A semantic token restates a colour instead of naming one. Every colour a tier-2 token uses has\n"
      + "to come from a tier-1 primitive, or retheming stops being a one-file edit:\n"
      + `${describeList(mixed.toSorted())}`,
  );
});

test("the dark theme redefines only names the light theme declares", async () => {
  const layer = await collectTokenLayer(workspace);
  const light = new Set();
  for (const entry of layer) {
    for (const token of entry.light) light.add(token.name);
  }
  const introduced = [];
  for (const entry of layer) {
    for (const token of entry.dark) {
      if (!light.has(token.name)) introduced.push(`${entry.file}: ${token.name} (${token.selector})`);
    }
  }
  assert.deepEqual(
    introduced.toSorted(),
    [],
    "The dark block declares a token the light block does not, so that name is undefined for every\n"
      + `reader on the default theme:\n${describeList(introduced.toSorted())}`,
  );
});

test("nothing outside the palette file reads a primitive", async () => {
  const leaks = (await collectPrimitiveReferences(workspace))
    .map(({ file, name }) => `${file} reads ${name}`)
    .toSorted();
  assert.deepEqual(
    [...new Set(leaks)],
    [],
    "docs/theming.md: \"Nothing outside app/styles/tokens-color.css may reference a primitive.\" A rule\n"
      + "that names a step has hard-coded a hue into a component: the theme block cannot move it, and\n"
      + "no screenshot can tell it apart from the semantic token beside it. Point it at a tier-2 token,\n"
      + `or add one if no role fits:\n${describeList([...new Set(leaks)])}`,
  );
});

test("the fixture harness injects exactly the stylesheets the application loads", async () => {
  const injected = await listInjectedStylesheets(workspace);
  const layer = (await listTokenStylesheets(workspace)).map((file) => relativePosix(workspace, file));
  assert.ok(
    injected.length > layer.length,
    "integration/visual/application-stylesheets.ts no longer derives its list from\n"
      + "app/styles/index.css. It cannot inject that file directly -- an @import does not resolve\n"
      + "inside an injected <style> -- so it reads the import list instead. A hand-written copy\n"
      + "leaves every fixture rendering var() fallbacks, or missing a whole primitive, with the\n"
      + "contracts and the baselines still green.",
  );
  assert.deepEqual(
    injected.slice(0, layer.length),
    layer,
    "The token layer is no longer the first thing app/styles/index.css imports. Injection follows\n"
      + "that order, and a token file that lands after the rules reading it decides the cascade by a\n"
      + "race: the fixtures would paint fallbacks while the contracts stayed green.",
  );
});

test("every stylesheet the application ships is imported by app/styles/index.css", async () => {
  const imported = new Set(await listInjectedStylesheets(workspace));
  const orphans = (await listStylesheets(workspace))
    .map((file) => relativePosix(workspace, file))
    .filter((file) => file.startsWith("app/"))
    .filter((file) => file !== "app/styles/index.css" && !imported.has(file))
    .toSorted();
  assert.deepEqual(
    orphans,
    [],
    "app/layout.tsx imports app/styles/index.css and nothing else, so a stylesheet that file does\n"
      + "not import ships to nobody: the rules are dead and the fixture harness never injects them.\n"
      + `Add the import in cascade order, or delete the file:\n${describeList(orphans)}`,
  );
});

// The invariants above are only worth their runtime if the parsing underneath
// them can see a violation, so the parser is pinned against the shapes a
// stylesheet actually uses.

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
