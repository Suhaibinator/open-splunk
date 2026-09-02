/**
 * The user's theme preference: where it is stored, how it resolves to a theme,
 * and how the document is told.
 *
 * The stylesheet knows one attribute, `data-theme` on `<html>`, and one value
 * that changes anything, `dark` (app/styles/tokens-color.css). Everything
 * about *choosing* that value lives here, in JavaScript, because
 * `.stylelintrc.json` deliberately keeps `prefers-color-scheme` out of the
 * CSS: a media-query fallback would decide the theme in a second place the
 * switch could not override, so the operating-system preference is read
 * through `matchMedia` instead and folded into the same resolution as the
 * stored choice.
 *
 * `THEME_BOOT_SCRIPT` restates `resolveTheme` + `applyTheme` as a string
 * because the two have to run before the first paint, ahead of any module
 * script: app/layout.tsx inlines it as the first child of `<head>`, so the
 * stylesheets that follow it already see the attribute. The unit test holds
 * the two spellings to the same table so they cannot drift.
 */

/** Both themes the stylesheet can render. */
export type Theme = "dark" | "light";

/** What the user asked for: one theme, or whichever the operating system prefers. */
export type ThemePreference = Theme | "system";

/** The menu order, and the key under which each choice is stored. */
export const THEME_PREFERENCES: readonly ThemePreference[] = ["system", "light", "dark"];

/** `localStorage` key holding an explicit `light` or `dark`; absent means "system". */
export const THEME_STORAGE_KEY = "open-splunk.theme";

/** The media query the "system" preference follows. */
export const DARK_SCHEME_QUERY = "(prefers-color-scheme: dark)";

/**
 * Resolves a stored preference against the operating-system preference. Only
 * the two explicit values win; anything else (absent, corrupt, a value from a
 * future release) falls through to the system preference rather than to light,
 * so an unknown token never pins a dark-mode user to the light theme.
 */
export function resolveTheme(stored: string | null | undefined, prefersDark: boolean): Theme {
  if (stored === "dark" || stored === "light") return stored;
  return prefersDark ? "dark" : "light";
}

/** Reads the preference back as the three-way choice the menu shows. */
export function themePreferenceFromStored(stored: string | null | undefined): ThemePreference {
  return stored === "dark" || stored === "light" ? stored : "system";
}

/** Tells the stylesheet which theme to render. */
export function applyTheme(document: Pick<Document, "documentElement">, theme: Theme): void {
  document.documentElement.setAttribute("data-theme", theme);
}

/**
 * The pre-paint spelling of `resolveTheme` + `applyTheme`.
 *
 * Reads `localStorage`, `matchMedia` and `document` as free names so the unit
 * test can bind fakes over them with `new Function`, and so the browser
 * resolves them on `window` exactly as a module would. `localStorage` throws
 * in a document that blocks storage (a sandboxed frame, a browser set to deny
 * site data), which is why the read is guarded: a throw there would leave the
 * page with no theme at all rather than the system one.
 */
export const THEME_BOOT_SCRIPT = "(function(){"
  + `var stored=null;try{stored=localStorage.getItem(${JSON.stringify(THEME_STORAGE_KEY)})}catch(e){}`
  + `var dark=stored==="dark"||(stored!=="light"&&matchMedia(${JSON.stringify(DARK_SCHEME_QUERY)}).matches);`
  + 'document.documentElement.setAttribute("data-theme",dark?"dark":"light")'
  + "})()";

/** Same-document notification that the preference was written. */
const THEME_PREFERENCE_EVENT = "open-splunk:theme-preference";

function storedThemePreference(): string | null {
  try {
    return window.localStorage.getItem(THEME_STORAGE_KEY);
  } catch {
    return null;
  }
}

/** The preference as the menu should show it; `system` on the server. */
export function currentThemePreference(): ThemePreference {
  if (typeof window === "undefined") return "system";
  return themePreferenceFromStored(storedThemePreference());
}

/**
 * Observes the stored preference: writes from this document, and writes from
 * another tab, which arrive as `storage` events. `localStorage` emits no
 * same-document event, so `setThemePreference` dispatches one of its own.
 */
export function subscribeToThemePreference(listener: () => void): () => void {
  if (typeof window === "undefined") return () => undefined;
  window.addEventListener("storage", listener);
  window.addEventListener(THEME_PREFERENCE_EVENT, listener);
  return () => {
    window.removeEventListener("storage", listener);
    window.removeEventListener(THEME_PREFERENCE_EVENT, listener);
  };
}

/** Re-resolves the theme from the stored preference and the system preference. */
export function syncTheme(): void {
  if (typeof window === "undefined") return;
  applyTheme(document, resolveTheme(storedThemePreference(), window.matchMedia(DARK_SCHEME_QUERY).matches));
}

/**
 * Commits a choice: `system` removes the key so the next boot follows the
 * operating system again, the other two pin a theme. The document is repainted
 * at once, and every mounted subscriber is told.
 */
export function setThemePreference(preference: ThemePreference): void {
  if (typeof window === "undefined") return;
  try {
    if (preference === "system") window.localStorage.removeItem(THEME_STORAGE_KEY);
    else window.localStorage.setItem(THEME_STORAGE_KEY, preference);
  } catch {
    // Storage is blocked: the choice still applies to this document, it just
    // will not outlive it -- which is why the paint below resolves from the
    // argument rather than reading the key back.
  }
  applyTheme(document, resolveTheme(
    preference === "system" ? null : preference,
    window.matchMedia(DARK_SCHEME_QUERY).matches,
  ));
  window.dispatchEvent(new Event(THEME_PREFERENCE_EVENT));
}

/**
 * Keeps the document in step with the operating system while the preference
 * is `system`: the boot script read the media query once, and this is what
 * makes a later change (sunset on a scheduled-dark machine) repaint the page.
 */
export function followSystemTheme(): () => void {
  if (typeof window === "undefined") return () => undefined;
  const query = window.matchMedia(DARK_SCHEME_QUERY);
  query.addEventListener("change", syncTheme);
  return () => query.removeEventListener("change", syncTheme);
}
