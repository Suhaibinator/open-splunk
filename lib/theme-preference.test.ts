import assert from "node:assert/strict";
import test from "node:test";

import {
  applyTheme,
  DARK_SCHEME_QUERY,
  resolveTheme,
  THEME_BOOT_SCRIPT,
  THEME_PREFERENCES,
  THEME_STORAGE_KEY,
  themePreferenceFromStored,
  type Theme,
} from "./theme-preference";

/** Every stored value the resolver has to make a decision about. */
const STORED_VALUES: ReadonlyArray<string | null> = ["dark", "light", null];

/** stored × prefersDark, with the theme each combination must land on. */
const RESOLUTION_TABLE: ReadonlyArray<[string | null, boolean, Theme]> = [
  ["dark", false, "dark"],
  ["dark", true, "dark"],
  ["light", false, "light"],
  ["light", true, "light"],
  [null, false, "light"],
  [null, true, "dark"],
];

test("an explicit choice wins and anything else follows the system", () => {
  for (const [stored, prefersDark, expected] of RESOLUTION_TABLE) {
    assert.equal(resolveTheme(stored, prefersDark), expected, `stored=${stored} prefersDark=${prefersDark}`);
  }
  // A value this release never wrote is not a light-theme vote.
  assert.equal(resolveTheme("sepia", true), "dark");
  assert.equal(resolveTheme("", false), "light");
  assert.equal(resolveTheme(undefined, true), "dark");
});

test("the menu reads the three-way preference back from storage", () => {
  assert.equal(themePreferenceFromStored("dark"), "dark");
  assert.equal(themePreferenceFromStored("light"), "light");
  assert.equal(themePreferenceFromStored(null), "system");
  assert.equal(themePreferenceFromStored("sepia"), "system");
  assert.deepEqual(THEME_PREFERENCES, ["system", "light", "dark"]);
});

test("applyTheme writes the attribute the stylesheet selects on", () => {
  const attributes = new Map<string, string>();
  const document = {
    documentElement: {
      setAttribute(name: string, value: string) {
        attributes.set(name, value);
      },
    },
  };
  applyTheme(document as unknown as Document, "dark");
  assert.deepEqual([...attributes], [["data-theme", "dark"]]);
  applyTheme(document as unknown as Document, "light");
  assert.deepEqual([...attributes], [["data-theme", "light"]]);
});

interface BootRun {
  attribute: string | undefined;
  queries: string[];
  reads: string[];
}

/**
 * Runs the boot script against fakes bound over its three free names, the way
 * the browser binds them on `window`.
 */
function runBootScript(storage: { getItem(key: string): string | null }, prefersDark: boolean): BootRun {
  const run: BootRun = { attribute: undefined, queries: [], reads: [] };
  const localStorage = {
    getItem(key: string) {
      run.reads.push(key);
      return storage.getItem(key);
    },
  };
  const matchMedia = (query: string) => {
    run.queries.push(query);
    return { matches: prefersDark };
  };
  const document = {
    documentElement: {
      setAttribute(name: string, value: string) {
        assert.equal(name, "data-theme");
        run.attribute = value;
      },
    },
  };
  new Function("localStorage", "matchMedia", "document", THEME_BOOT_SCRIPT)(localStorage, matchMedia, document);
  return run;
}

test("the boot script resolves every stored × system combination as resolveTheme does", () => {
  for (const stored of STORED_VALUES) {
    for (const prefersDark of [false, true]) {
      const run = runBootScript({ getItem: () => stored }, prefersDark);
      assert.equal(run.attribute, resolveTheme(stored, prefersDark), `stored=${stored} prefersDark=${prefersDark}`);
      assert.deepEqual(run.reads, [THEME_STORAGE_KEY]);
      // The media query is consulted only when the stored value does not
      // decide, so a pinned theme never depends on matchMedia existing.
      assert.deepEqual(run.queries, stored === null ? [DARK_SCHEME_QUERY] : []);
    }
  }
});

test("the boot script still paints a theme when storage throws", () => {
  const blocked = {
    getItem(): string | null {
      throw new DOMException("The operation is insecure.", "SecurityError");
    },
  };
  assert.equal(runBootScript(blocked, true).attribute, "dark");
  assert.equal(runBootScript(blocked, false).attribute, "light");
});

test("the boot script is a single self-contained statement with no module syntax", () => {
  assert.doesNotMatch(THEME_BOOT_SCRIPT, /\b(?:import|export|require)\b/u);
  assert.doesNotMatch(THEME_BOOT_SCRIPT, /<\/script/iu);
  assert.match(THEME_BOOT_SCRIPT, /^\(function\(\)\{.*\}\)\(\)$/u);
});
