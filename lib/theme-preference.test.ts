import assert from "node:assert/strict";
import test from "node:test";

import { DEFAULT_PALETTE, PALETTES, resolvePalette, type Palette } from "./palettes";
import {
  applyInstancePalette,
  applyPalette,
  applyTheme,
  DARK_SCHEME_QUERY,
  PALETTE_STORAGE_KEY,
  resolveTheme,
  syncPalette,
  syncThemeColorMeta,
  THEME_BOOT_SCRIPT,
  THEME_PREFERENCES,
  THEME_STORAGE_KEY,
  themePreferenceFromStored,
  type Theme,
} from "./theme-preference";

/** Every stored value the resolver has to make a decision about. */
const STORED_VALUES: ReadonlyArray<string | null> = ["dark", "light", null];

/**
 * Every cached palette the boot script has to make a decision about: each
 * shipped name, no cache, a name this build does not ship, the empty string a
 * corrupt write leaves behind, and a case variant (the match is exact).
 */
const CACHED_PALETTES: ReadonlyArray<string | null> = [...PALETTES, null, "sepia", "", "CLASSIC"];

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

function fakeDocument() {
  const attributes = new Map<string, string>();
  const document = {
    documentElement: {
      setAttribute(name: string, value: string) {
        attributes.set(name, value);
      },
    },
  };
  return { attributes, document: document as unknown as Document };
}

test("applyTheme writes the attribute the stylesheet selects on", () => {
  const { attributes, document } = fakeDocument();
  applyTheme(document, "dark");
  assert.deepEqual([...attributes], [["data-theme", "dark"]]);
  applyTheme(document, "light");
  assert.deepEqual([...attributes], [["data-theme", "light"]]);
});

test("applyPalette writes the second attribute and leaves the theme alone", () => {
  const { attributes, document } = fakeDocument();
  applyTheme(document, "dark");
  for (const palette of PALETTES) {
    applyPalette(document, palette);
    assert.deepEqual([...attributes], [["data-theme", "dark"], ["data-palette", palette]]);
  }
});

interface BootRun {
  attributes: Map<string, string>;
  queries: string[];
  reads: string[];
}

interface BootStorage {
  getItem(key: string): string | null;
}

/**
 * Runs the boot script against fakes bound over its three free names, the way
 * the browser binds them on `window`. Attributes are recorded in a Map so the
 * theme and the palette can be asserted together and in order.
 */
function runBootScript(storage: BootStorage, prefersDark: boolean): BootRun {
  const run: BootRun = { attributes: new Map(), queries: [], reads: [] };
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
        run.attributes.set(name, value);
      },
    },
  };
  new Function("localStorage", "matchMedia", "document", THEME_BOOT_SCRIPT)(localStorage, matchMedia, document);
  return run;
}

/** A storage holding one theme and one palette, and nothing under any other key. */
function storageOf(theme: string | null, palette: string | null): BootStorage {
  return {
    getItem(key: string) {
      if (key === THEME_STORAGE_KEY) return theme;
      if (key === PALETTE_STORAGE_KEY) return palette;
      return null;
    },
  };
}

test("the boot script resolves every stored × system × palette combination as the module does", () => {
  for (const stored of STORED_VALUES) {
    for (const prefersDark of [false, true]) {
      for (const cached of CACHED_PALETTES) {
        const label = `stored=${stored} prefersDark=${prefersDark} palette=${JSON.stringify(cached)}`;
        const run = runBootScript(storageOf(stored, cached), prefersDark);
        assert.deepEqual([...run.attributes], [
          ["data-theme", resolveTheme(stored, prefersDark)],
          ["data-palette", resolvePalette(cached)],
        ], label);
        assert.deepEqual(run.reads, [THEME_STORAGE_KEY, PALETTE_STORAGE_KEY], label);
        // The media query is consulted only when the stored value does not
        // decide, so a pinned theme never depends on matchMedia existing.
        assert.deepEqual(run.queries, stored === null ? [DARK_SCHEME_QUERY] : [], label);
      }
    }
  }
});

test("the boot script writes the palette explicitly on every load, classic included", () => {
  const run = runBootScript(storageOf(null, null), false);
  assert.equal(run.attributes.get("data-palette"), DEFAULT_PALETTE);
  assert.equal(DEFAULT_PALETTE, "classic");
  // The script carries the same list the module resolves against, so a
  // palette added to one and not the other shows up here.
  assert.ok(THEME_BOOT_SCRIPT.includes(JSON.stringify(PALETTES)));
});

test("the boot script still paints a theme and the classic palette when storage throws", () => {
  const blocked = {
    getItem(): string | null {
      throw new DOMException("The operation is insecure.", "SecurityError");
    },
  };
  assert.deepEqual([...runBootScript(blocked, true).attributes], [["data-theme", "dark"], ["data-palette", "classic"]]);
  assert.deepEqual([...runBootScript(blocked, false).attributes], [["data-theme", "light"], ["data-palette", "classic"]]);
});

test("a throw on the palette read alone keeps the theme that was already read", () => {
  const paletteBlocked: BootStorage = {
    getItem(key: string) {
      if (key === PALETTE_STORAGE_KEY) throw new DOMException("The operation is insecure.", "SecurityError");
      return "dark";
    },
  };
  assert.deepEqual([...runBootScript(paletteBlocked, false).attributes], [["data-theme", "dark"], ["data-palette", "classic"]]);
});

test("the boot script is a single self-contained statement with no module syntax", () => {
  assert.doesNotMatch(THEME_BOOT_SCRIPT, /\b(?:import|export|require)\b/u);
  assert.doesNotMatch(THEME_BOOT_SCRIPT, /<\/script/iu);
  assert.match(THEME_BOOT_SCRIPT, /^\(function\(\)\{.*\}\)\(\)$/u);
});

/* == The window-side functions, run against a fake window ==================== */

interface FakeWindow {
  attributes: Map<string, string>;
  chromeBar: string;
  meta: { content: string | null } | null;
  storage: Map<string, string>;
  storageBlocked: boolean;
  writes: Array<[string, string]>;
}

/**
 * Installs `window` and `document` globals shaped like the slice the module
 * reads, runs the body, and removes them again so no other test sees a
 * browser. `chromeBar` is what `getComputedStyle` reports for `--chrome-bar`;
 * `meta` is the `<meta name="theme-color">` element, or `null` when the page
 * has none.
 */
function withFakeWindow<T>(setup: Partial<FakeWindow>, body: (fake: FakeWindow) => T): T {
  const fake: FakeWindow = {
    attributes: new Map(),
    chromeBar: " var(--slate-900) ",
    meta: { content: "first-paint" },
    storage: new Map(),
    storageBlocked: false,
    writes: [],
    ...setup,
  };
  const documentElement = {
    setAttribute(name: string, value: string) {
      fake.attributes.set(name, value);
    },
  };
  const meta = fake.meta === null ? null : {
    setAttribute(name: string, value: string) {
      assert.equal(name, "content");
      fake.meta!.content = value;
    },
  };
  const document = {
    documentElement,
    querySelector(selector: string) {
      assert.equal(selector, 'meta[name="theme-color"]');
      return meta;
    },
  };
  const localStorage = {
    getItem(key: string) {
      if (fake.storageBlocked) throw new DOMException("The operation is insecure.", "SecurityError");
      return fake.storage.get(key) ?? null;
    },
    setItem(key: string, value: string) {
      if (fake.storageBlocked) throw new DOMException("The operation is insecure.", "SecurityError");
      fake.storage.set(key, value);
      fake.writes.push([key, value]);
    },
  };
  const window = {
    document,
    getComputedStyle(element: unknown) {
      assert.equal(element, documentElement);
      return {
        getPropertyValue(name: string) {
          assert.equal(name, "--chrome-bar");
          return fake.chromeBar;
        },
      };
    },
    localStorage,
  };
  const globals = globalThis as { document?: unknown; window?: unknown };
  globals.window = window;
  globals.document = document;
  try {
    return body(fake);
  } finally {
    delete globals.window;
    delete globals.document;
  }
}

test("the window-side functions are inert on the server", () => {
  assert.equal(typeof (globalThis as { window?: unknown }).window, "undefined");
  assert.doesNotThrow(() => {
    syncPalette();
    syncThemeColorMeta();
    applyInstancePalette("ocean");
  });
});

test("syncThemeColorMeta copies the computed chrome bar into the theme-color meta", () => {
  withFakeWindow({ chromeBar: "  var(--slateblue-900)\n" }, (fake) => {
    syncThemeColorMeta();
    assert.equal(fake.meta?.content, "var(--slateblue-900)");
  });
  // A page without the meta, or a stylesheet that has not loaded yet, leaves
  // whatever the first paint had rather than writing an empty colour.
  withFakeWindow({ meta: null }, () => assert.doesNotThrow(syncThemeColorMeta));
  withFakeWindow({ chromeBar: "" }, (fake) => {
    syncThemeColorMeta();
    assert.equal(fake.meta?.content, "first-paint");
  });
});

test("applyInstancePalette resolves, caches, paints and recolours the chrome", () => {
  for (const palette of PALETTES) {
    withFakeWindow({ chromeBar: `--${palette}-bar` }, (fake) => {
      applyInstancePalette(palette);
      assert.equal(fake.attributes.get("data-palette"), palette);
      assert.equal(fake.storage.get(PALETTE_STORAGE_KEY), palette);
      assert.equal(fake.meta?.content, `--${palette}-bar`);
    });
  }
});

test("applyInstancePalette caches the resolved name, never the raw one", () => {
  for (const raw of ["sepia", "", "CLASSIC"]) {
    withFakeWindow({}, (fake) => {
      applyInstancePalette(raw);
      assert.equal(fake.attributes.get("data-palette"), DEFAULT_PALETTE, raw);
      assert.equal(fake.storage.get(PALETTE_STORAGE_KEY), DEFAULT_PALETTE, raw);
    });
  }
});

test("applyInstancePalette is idempotent", () => {
  withFakeWindow({}, (fake) => {
    applyInstancePalette("ember");
    applyInstancePalette("ember");
    assert.deepEqual(fake.writes, [[PALETTE_STORAGE_KEY, "ember"], [PALETTE_STORAGE_KEY, "ember"]]);
    assert.deepEqual([...fake.attributes], [["data-palette", "ember"]]);
    assert.deepEqual([...fake.storage], [[PALETTE_STORAGE_KEY, "ember"]]);
  });
});

test("applyInstancePalette still paints when storage is blocked", () => {
  withFakeWindow({ storageBlocked: true }, (fake) => {
    assert.doesNotThrow(() => applyInstancePalette("graphite"));
    assert.equal(fake.attributes.get("data-palette"), "graphite");
    assert.deepEqual(fake.writes, []);
  });
});

test("syncPalette re-applies the cache and falls back to classic", () => {
  const cases: ReadonlyArray<[string | null, Palette]> = [
    ["ocean", "ocean"],
    ["terminal", "terminal"],
    [null, "classic"],
    ["sepia", "classic"],
    ["", "classic"],
  ];
  for (const [cached, expected] of cases) {
    withFakeWindow({ storage: new Map(cached === null ? [] : [[PALETTE_STORAGE_KEY, cached]]) }, (fake) => {
      syncPalette();
      assert.equal(fake.attributes.get("data-palette"), expected, `cached=${JSON.stringify(cached)}`);
      assert.equal(resolvePalette(cached), expected);
      // A sync never writes: the cache is the server's, not this function's.
      assert.deepEqual(fake.writes, []);
    });
  }
  withFakeWindow({ storageBlocked: true }, (fake) => {
    syncPalette();
    assert.equal(fake.attributes.get("data-palette"), "classic");
  });
});
