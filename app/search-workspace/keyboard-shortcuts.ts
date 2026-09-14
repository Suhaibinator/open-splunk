import { useSyncExternalStore } from "react";

// The one table behind the keyboard-shortcut sheet and the editor's help
// strip. A shortcut that is not listed here is one the sheet cannot show, so
// the handlers in the workspace, the editor reducers and the product shell
// each have a row -- the sheet documents what the code does, and the test
// beside this file keeps the chords unique within a scope.

export type KeyboardPlatform = "mac" | "other";

export type ShortcutScope = "editor" | "completions" | "workspace";

/** Platform-neutral key names; `Mod` is ⌘ on a Mac and Ctrl elsewhere. */
export type ShortcutKey =
  | "Mod"
  | "Ctrl"
  | "Enter"
  | "Tab"
  | "Space"
  | "Escape"
  | "ArrowUp"
  | "ArrowDown"
  | "K"
  | "?";

export type ShortcutChord = readonly ShortcutKey[];

export interface KeyboardShortcut {
  readonly id: string;
  readonly scope: ShortcutScope;
  /** Alternatives that do the same thing; every chord is shown. */
  readonly chords: readonly ShortcutChord[];
  readonly description: string;
  /** Tail of the editor help-strip entry, or absent when the strip omits it. */
  readonly meta?: string;
}

export const SHORTCUT_SCOPES: readonly { readonly id: ShortcutScope; readonly title: string }[] = [
  { id: "editor", title: "Search editor" },
  { id: "completions", title: "Completion popup" },
  { id: "workspace", title: "Anywhere in the workspace" },
];

/** In strip order: the editor's help strip lists the ones that carry `meta`. */
export const KEYBOARD_SHORTCUTS: readonly KeyboardShortcut[] = [
  { id: "complete", scope: "editor", chords: [["Ctrl", "Space"]], description: "Open or close the completion popup", meta: "for suggestions" },
  { id: "history", scope: "editor", chords: [["ArrowUp"], ["ArrowDown"]], description: "Recall an older or newer search from history when the caret is on the first or last line", meta: "history" },
  { id: "run", scope: "editor", chords: [["Mod", "Enter"]], description: "Run the search", meta: "to run" },
  { id: "move-completion", scope: "completions", chords: [["ArrowUp"], ["ArrowDown"]], description: "Move the highlight" },
  { id: "accept-completion", scope: "completions", chords: [["Enter"], ["Tab"]], description: "Insert the highlighted completion" },
  { id: "close-completions", scope: "completions", chords: [["Escape"]], description: "Close the popup without inserting" },
  { id: "escape", scope: "workspace", chords: [["Escape"]], description: "Close the open dialog, menu or popup; with nothing open, cancel the running search" },
  { id: "find", scope: "workspace", chords: [["Mod", "K"]], description: "Focus Find in the product bar" },
  { id: "shortcuts", scope: "workspace", chords: [["?"]], description: "Open this shortcut sheet (outside a text field)" },
];

const MAC_PLATFORM = /Mac|iPhone|iPad|iPod/u;
const subscribeKeyboardPlatform = () => () => {};

/** Reads the platform from a navigator; `mac` decides whether Mod is ⌘. */
export function detectKeyboardPlatform(navigatorLike: { platform?: string; userAgent?: string } | undefined): KeyboardPlatform {
  if (navigatorLike === undefined) return "mac";
  const source = navigatorLike.platform || navigatorLike.userAgent || "";
  return MAC_PLATFORM.test(source) ? "mac" : "other";
}

/**
 * The platform the shortcuts should be spelled for. The first render matches
 * the static export (Mac glyphs, which is what the strip has always shown), and
 * the effect corrects it after hydration so the markup never mismatches.
 */
export function useKeyboardPlatform(): KeyboardPlatform {
  return useSyncExternalStore(
    subscribeKeyboardPlatform,
    () => detectKeyboardPlatform(window.navigator),
    () => "mac",
  );
}

const SHEET_LABELS: Record<Exclude<ShortcutKey, "Mod">, string> = {
  Ctrl: "Ctrl",
  Enter: "Enter",
  Tab: "Tab",
  Space: "Space",
  Escape: "Esc",
  ArrowUp: "↑",
  ArrowDown: "↓",
  K: "K",
  "?": "?",
};

/** One label per key, as the sheet renders them into `<kbd>` elements. */
export function shortcutKeyLabels(chord: ShortcutChord, platform: KeyboardPlatform): string[] {
  return chord.map((key) => key === "Mod" ? (platform === "mac" ? "⌘" : "Ctrl") : SHEET_LABELS[key]);
}

/**
 * The compact spelling the editor help strip uses: glyph modifiers run into
 * the key (`⌘↵`), spelled ones join with a plus (`Ctrl+Space`).
 */
export function shortcutMetaText(shortcut: KeyboardShortcut, platform: KeyboardPlatform): string | null {
  if (shortcut.meta === undefined) return null;
  const chords = shortcut.chords.map((chord) => {
    const labels = shortcutKeyLabels(chord, platform).map((label) => label === "Enter" ? "↵" : label);
    const glyphModifier = labels.some((label) => label === "⌘");
    return labels.join(glyphModifier ? "" : "+");
  });
  return `${chords.join("")} ${shortcut.meta}`;
}

/** Whether a keydown target is somewhere typing belongs, so `?` should type. */
export function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable) return true;
  const tag = target.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
}
