import assert from "node:assert/strict";
import test from "node:test";

import {
  KEYBOARD_SHORTCUTS,
  SHORTCUT_SCOPES,
  detectKeyboardPlatform,
  shortcutKeyLabels,
  shortcutMetaText,
} from "./keyboard-shortcuts";

test("no chord is bound twice within one scope, and every scope is listed", () => {
  const seen = new Map<string, string>();
  for (const shortcut of KEYBOARD_SHORTCUTS) {
    for (const chord of shortcut.chords) {
      const key = `${shortcut.scope}:${chord.join("+")}`;
      assert.equal(seen.get(key), undefined, `${key} bound by ${seen.get(key)} and ${shortcut.id}`);
      seen.set(key, shortcut.id);
    }
    assert.ok(SHORTCUT_SCOPES.some((scope) => scope.id === shortcut.scope), `${shortcut.id} has an unlisted scope`);
  }
  const ids = KEYBOARD_SHORTCUTS.map((shortcut) => shortcut.id);
  assert.deepEqual(ids, Array.from(new Set(ids)));
  for (const scope of SHORTCUT_SCOPES) {
    assert.ok(KEYBOARD_SHORTCUTS.some((shortcut) => shortcut.scope === scope.id), `${scope.id} has no shortcuts`);
  }
});

test("Mod spells as the command glyph on a Mac and as Ctrl elsewhere", () => {
  assert.deepEqual(shortcutKeyLabels(["Mod", "Enter"], "mac"), ["⌘", "Enter"]);
  assert.deepEqual(shortcutKeyLabels(["Mod", "Enter"], "other"), ["Ctrl", "Enter"]);
  assert.deepEqual(shortcutKeyLabels(["Ctrl", "Space"], "mac"), ["Ctrl", "Space"]);
  assert.deepEqual(shortcutKeyLabels(["Escape"], "other"), ["Esc"]);
});

test("the help strip keeps its compact spellings on both platforms", () => {
  const byId = new Map(KEYBOARD_SHORTCUTS.map((shortcut) => [shortcut.id, shortcut]));
  assert.equal(shortcutMetaText(byId.get("run")!, "mac"), "⌘↵ to run");
  assert.equal(shortcutMetaText(byId.get("run")!, "other"), "Ctrl+↵ to run");
  assert.equal(shortcutMetaText(byId.get("complete")!, "mac"), "Ctrl+Space for suggestions");
  assert.equal(shortcutMetaText(byId.get("history")!, "other"), "↑↓ history");
  assert.equal(shortcutMetaText(byId.get("find")!, "mac"), null);
});

test("the platform is read from the navigator and defaults to a Mac", () => {
  assert.equal(detectKeyboardPlatform(undefined), "mac");
  assert.equal(detectKeyboardPlatform({ platform: "MacIntel" }), "mac");
  assert.equal(detectKeyboardPlatform({ platform: "Win32" }), "other");
  assert.equal(detectKeyboardPlatform({ platform: "", userAgent: "Mozilla/5.0 (X11; Linux x86_64)" }), "other");
  assert.equal(detectKeyboardPlatform({ userAgent: "Mozilla/5.0 (iPad; CPU OS 17_0)" }), "mac");
});
