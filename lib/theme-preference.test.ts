import assert from "node:assert/strict";
import test from "node:test";

import { DEFAULT_PALETTE, PALETTES, resolvePalette, type Palette } from "./palettes";
import {
  applyInstancePalette,
  applyPalette,
  applyTheme,
  DARK_SCHEME_QUERY,
  PALETTE_STORAGE_KEY,
  previewPalette,
  resolveTheme,
  syncPalette,
  syncTheme,
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
  /** Whether `window.getComputedStyle` exists, and whether calling it throws. */
  computedStyle: "available" | "absent" | "throws";
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
    computedStyle: "available",
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
  const window: { document: unknown; getComputedStyle?: (element: unknown) => unknown; localStorage: unknown } = {
    document,
    getComputedStyle(element: unknown) {
      assert.equal(element, documentElement);
      if (fake.computedStyle === "throws") throw new TypeError("getComputedStyle is not usable here");
      return {
        getPropertyValue(name: string) {
          assert.equal(name, "--chrome-bar");
          return fake.chromeBar;
        },
      };
    },
    localStorage,
  };
  if (fake.computedStyle === "absent") delete window.getComputedStyle;
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
    previewPalette("ocean");
  });
});

test("previewPalette paints and recolours the chrome without touching the cache", () => {
  for (const palette of PALETTES) {
    withFakeWindow({ chromeBar: `--${palette}-bar`, storage: new Map([[PALETTE_STORAGE_KEY, "ember"]]) }, (fake) => {
      previewPalette(palette);
      assert.equal(fake.attributes.get("data-palette"), palette);
      assert.equal(fake.meta?.content, `--${palette}-bar`);
      // The cache is the server's: a preview is not a value another tab or
      // the next boot may take, so it is never written.
      assert.deepEqual(fake.writes, []);
      assert.equal(fake.storage.get(PALETTE_STORAGE_KEY), "ember");
    });
  }
  // Blocked storage is not even consulted.
  withFakeWindow({ storageBlocked: true }, (fake) => {
    assert.doesNotThrow(() => previewPalette("graphite"));
    assert.equal(fake.attributes.get("data-palette"), "graphite");
  });
});

test("applyInstancePalette after a preview restores both the document and the cache", () => {
  withFakeWindow({ storage: new Map([[PALETTE_STORAGE_KEY, "classic"]]) }, (fake) => {
    previewPalette("terminal");
    assert.equal(fake.attributes.get("data-palette"), "terminal");
    applyInstancePalette("classic");
    assert.equal(fake.attributes.get("data-palette"), "classic");
    assert.deepEqual(fake.writes, [[PALETTE_STORAGE_KEY, "classic"]]);
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

/* == Break phase: hostile caches, misbehaving storage, and boot-versus-module == */

/**
 * Cached values a boot script has to survive: strings that would break out
 * of a quoted context if they were ever interpolated rather than compared,
 * names that exist on every object's prototype (an `in` or a property lookup
 * would find them), near misses of shipped names, and the spellings a
 * corrupt or foreign write leaves behind. Every one paints classic.
 */
const HOSTILE_PALETTES: readonly string[] = [
  '"ocean"',
  "'ocean'",
  "ocean\"",
  "</script><script>document.title='x'</script>",
  "ocean</script>",
  "ocean\u0000",
  "ocean\n",
  " ocean",
  "ocean ",
  "Ocean",
  "OCEAN",
  "océan",
  "classic,ocean",
  JSON.stringify(PALETTES),
  "constructor",
  "__proto__",
  "prototype",
  "hasOwnProperty",
  "toString",
  "valueOf",
  "length",
  "indexOf",
  "includes",
  "0",
  "1",
  "-1",
  "NaN",
  "undefined",
  "null",
  "[object Object]",
  "true",
];

/** Values a shimmed storage might hand back that a browser never would. */
const NON_STRING_PALETTES: ReadonlyArray<[label: string, value: unknown]> = [
  ["undefined", undefined],
  ["a number", 1],
  ["an array whose string form is a palette", ["ocean"]],
  ["a String object", new String("ocean")],
  ["an object with a palette toString", { toString: () => "ocean" }],
];

/** How storage misbehaves across the boot script's two reads. */
type ThrowMode = "none" | "first" | "second" | "both";
const THROW_MODES: readonly ThrowMode[] = ["none", "first", "second", "both"];

/**
 * A storage that throws on the reads `mode` names, counting reads from the
 * moment it is built: one instance per run, since the boot script and the
 * module each read twice.
 */
function misbehavingStorage(theme: string | null, palette: unknown, mode: ThrowMode): BootStorage {
  let reads = 0;
  return {
    getItem(key: string) {
      reads += 1;
      const first = reads === 1;
      if (mode === "both" || (mode === "first" && first) || (mode === "second" && !first)) {
        throw new DOMException("The operation is insecure.", "SecurityError");
      }
      if (key === THEME_STORAGE_KEY) return theme;
      if (key === PALETTE_STORAGE_KEY) return palette as string | null;
      return null;
    },
  };
}

/**
 * The module's own reading of the same storage: `syncTheme` then
 * `syncPalette`, each with its own guarded read, in the order the boot
 * script reads. What comes back is what the boot script must have written.
 */
function moduleResolution(storage: BootStorage, prefersDark: boolean): Array<[string, string]> {
  const attributes = new Map<string, string>();
  const document = {
    documentElement: {
      setAttribute(name: string, value: string) {
        attributes.set(name, value);
      },
    },
    querySelector: () => null,
  };
  const window = {
    document,
    localStorage: { getItem: (key: string) => storage.getItem(key) },
    matchMedia: () => ({ matches: prefersDark }),
  };
  const globals = globalThis as { document?: unknown; window?: unknown };
  globals.window = window;
  globals.document = document;
  try {
    syncTheme();
    syncPalette();
  } finally {
    delete globals.window;
    delete globals.document;
  }
  return [...attributes];
}

test("the boot script equals the module under every palette x theme x storage-throw combination", () => {
  const palettes: ReadonlyArray<string | null> = [...CACHED_PALETTES, ...HOSTILE_PALETTES];
  let runs = 0;
  for (const mode of THROW_MODES) {
    for (const stored of [...STORED_VALUES, "sepia", ""]) {
      for (const prefersDark of [false, true]) {
        for (const cached of palettes) {
          const label = `mode=${mode} stored=${JSON.stringify(stored)} prefersDark=${prefersDark} palette=${JSON.stringify(cached)}`;
          const boot = runBootScript(misbehavingStorage(stored, cached, mode), prefersDark);
          const expected = moduleResolution(misbehavingStorage(stored, cached, mode), prefersDark);
          assert.deepEqual([...boot.attributes], expected, label);
          // The expectation itself, spelled out: a throw costs only the key it
          // hit, so a blocked theme read still leaves the cached palette
          // standing and vice versa.
          const themeReadable = mode === "none" || mode === "second";
          const paletteReadable = mode === "none" || mode === "first";
          assert.deepEqual(expected, [
            ["data-theme", resolveTheme(themeReadable ? stored : null, prefersDark)],
            ["data-palette", paletteReadable ? resolvePalette(cached) : DEFAULT_PALETTE],
          ], label);
          // Both keys are always attempted: the second read never depends on
          // the first succeeding.
          assert.deepEqual(boot.reads, [THEME_STORAGE_KEY, PALETTE_STORAGE_KEY], label);
          runs += 1;
        }
      }
    }
  }
  assert.ok(runs > 1000, `only ${runs} combinations were exercised`);
});

test("a hostile cached palette paints classic in the boot script and the module alike", () => {
  for (const cached of HOSTILE_PALETTES) {
    assert.equal(resolvePalette(cached), DEFAULT_PALETTE, JSON.stringify(cached));
    const run = runBootScript(storageOf("light", cached), false);
    assert.equal(run.attributes.get("data-palette"), DEFAULT_PALETTE, JSON.stringify(cached));
    assert.equal(run.attributes.get("data-theme"), "light", JSON.stringify(cached));
  }
  // The inlined list is what makes the comparison exact rather than a
  // prefix or substring match: a shipped name never appears inside another.
  for (const palette of PALETTES) {
    for (const other of PALETTES) {
      if (other !== palette) assert.ok(!other.includes(palette), `${palette} is a substring of ${other}`);
    }
    assert.doesNotMatch(palette, /["'<>\\\s]/u, `${palette} would need escaping in the boot script`);
  }
});

test("a non-string from a shimmed storage paints classic in the boot script and the module alike", () => {
  for (const [label, value] of NON_STRING_PALETTES) {
    assert.equal(resolvePalette(value as string), DEFAULT_PALETTE, label);
    const run = runBootScript(misbehavingStorage("dark", value, "none"), false);
    assert.deepEqual([...run.attributes], [["data-theme", "dark"], ["data-palette", DEFAULT_PALETTE]], label);
    assert.deepEqual(moduleResolution(misbehavingStorage("dark", value, "none"), false), [...run.attributes], label);
  }
});

test("a prototype key in the cache neither resolves nor reaches a prototype", () => {
  // `indexOf` on the inlined array and `includes` on PALETTES both compare by
  // value; a lookup by property name would find these on Array.prototype or
  // Object.prototype and paint an undefined palette.
  for (const key of ["constructor", "__proto__", "hasOwnProperty", "toString", "length", "0"]) {
    withFakeWindow({ storage: new Map([[PALETTE_STORAGE_KEY, key]]) }, (fake) => {
      syncPalette();
      assert.equal(fake.attributes.get("data-palette"), DEFAULT_PALETTE, key);
      applyInstancePalette(key);
      assert.equal(fake.attributes.get("data-palette"), DEFAULT_PALETTE, key);
      assert.equal(fake.storage.get(PALETTE_STORAGE_KEY), DEFAULT_PALETTE, key);
    });
  }
  assert.equal(Object.getPrototypeOf(PALETTES), Array.prototype);
  assert.equal(new Function(`return ${JSON.stringify(PALETTES)}.indexOf("__proto__")`)(), -1);
});

test("applyInstancePalette with an unknown server value overwrites a stale cache so the next boot is classic", () => {
  for (const stale of PALETTES.filter((palette) => palette !== DEFAULT_PALETTE)) {
    for (const unknown of ["sepia", "", "CLASSIC", "constructor", "</script>", "ocean\n"]) {
      withFakeWindow({ storage: new Map([[PALETTE_STORAGE_KEY, stale]]) }, (fake) => {
        // The boot script painted the stale palette from the cache.
        const before = runBootScript({ getItem: (key) => fake.storage.get(key) ?? null }, false);
        assert.equal(before.attributes.get("data-palette"), stale);

        applyInstancePalette(unknown);
        assert.equal(fake.attributes.get("data-palette"), DEFAULT_PALETTE, `${stale} <- ${JSON.stringify(unknown)}`);
        assert.equal(fake.storage.get(PALETTE_STORAGE_KEY), DEFAULT_PALETTE, `${stale} <- ${JSON.stringify(unknown)}`);
        assert.deepEqual(fake.writes, [[PALETTE_STORAGE_KEY, DEFAULT_PALETTE]]);

        // The next boot reads the corrected cache, not the stale name.
        const after = runBootScript({ getItem: (key) => fake.storage.get(key) ?? null }, false);
        assert.equal(after.attributes.get("data-palette"), DEFAULT_PALETTE, `${stale} <- ${JSON.stringify(unknown)}`);
      });
    }
  }
});

test("applyInstancePalette with blocked storage cannot correct the cache, and says so by painting only", () => {
  // Nothing can be done about the stale value; the point is that the call
  // neither throws nor leaves the document on the stale palette.
  withFakeWindow({ storage: new Map([[PALETTE_STORAGE_KEY, "ocean"]]), storageBlocked: true }, (fake) => {
    assert.doesNotThrow(() => applyInstancePalette("sepia"));
    assert.equal(fake.attributes.get("data-palette"), DEFAULT_PALETTE);
    assert.deepEqual(fake.writes, []);
    assert.equal(fake.storage.get(PALETTE_STORAGE_KEY), "ocean");
  });
});

test("syncThemeColorMeta survives a window that cannot compute styles, and so do its callers", () => {
  for (const computedStyle of ["absent", "throws"] as const) {
    withFakeWindow({ computedStyle }, (fake) => {
      assert.doesNotThrow(syncThemeColorMeta, computedStyle);
      assert.equal(fake.meta?.content, "first-paint", computedStyle);
      // The palette still lands and the cache is still written: the meta is
      // the last step of each application, never a gate on it.
      assert.doesNotThrow(() => applyInstancePalette("ember"), computedStyle);
      assert.equal(fake.attributes.get("data-palette"), "ember", computedStyle);
      assert.deepEqual(fake.writes, [[PALETTE_STORAGE_KEY, "ember"]], computedStyle);
      assert.doesNotThrow(() => previewPalette("glass"), computedStyle);
      assert.equal(fake.attributes.get("data-palette"), "glass", computedStyle);
      assert.doesNotThrow(syncPalette, computedStyle);
      assert.equal(fake.meta?.content, "first-paint", computedStyle);
    });
  }
  // With the meta absent nothing is computed at all, whether or not styles could be.
  withFakeWindow({ computedStyle: "absent", meta: null }, () => assert.doesNotThrow(syncThemeColorMeta));
  withFakeWindow({ computedStyle: "throws", meta: null }, () => assert.doesNotThrow(syncThemeColorMeta));
});

test("the boot script's two reads are guarded separately", () => {
  // One try around both reads would skip the palette whenever the theme read
  // threw; the module guards each key on its own, so the script must too.
  assert.equal((THEME_BOOT_SCRIPT.match(/try\{/gu) ?? []).length, 2);
  assert.equal((THEME_BOOT_SCRIPT.match(/catch\(e\)\{\}/gu) ?? []).length, 2);
  const themeBlocked = misbehavingStorage("dark", "terminal", "first");
  assert.deepEqual([...runBootScript(themeBlocked, false).attributes], [["data-theme", "light"], ["data-palette", "terminal"]]);
});
