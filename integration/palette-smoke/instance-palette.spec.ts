// End-to-end smoke for the instance palette: the administrator updates it over
// the browser API, `/api/system/bootstrap` serves it, and the browser paints
// it -- on first load from the network, on every later load from the cache
// the boot script reads before the first paint.
//
// `integration/palette_smoke_test.go` starts the real browser API handler
// over a real SQLite control plane and the real static export on a loopback
// port, mints the administrator bearer, and runs this spec through
// `playwright.palette-smoke.config.ts`. Like the other Playwright suites this
// is a `.spec.ts`: `scripts/test-frontend.mjs` runs `.test.ts` files under
// node, where a Playwright test cannot run.
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

import {
  GetServerAppearanceRequest,
  GetServerAppearanceResponse,
  UiPalette,
  UpdateServerAppearanceRequest,
  UpdateServerAppearanceResponse,
} from "../../gen/ts/open_splunk/server_settings_api";
import { PROTOBUF_CONTENT_TYPE } from "../../lib/api/protobuf-transport";
import { PALETTE_STORAGE_KEY, THEME_STORAGE_KEY } from "../../lib/theme-preference";

const administratorToken = requiredEnvironment("OPEN_SPLUNK_PALETTE_SMOKE_ADMINISTRATOR_TOKEN");

const BOOTSTRAP_ROUTE = "**/api/system/bootstrap";
const APPEARANCE_GET_PATH = "/api/server/appearance/get";
const APPEARANCE_UPDATE_PATH = "/api/server/appearance/update";

/** The search workspace is the page whose user menu mounts a `.floating-menu`. */
const SEARCH_URL = `/search/?${new URLSearchParams({
  q: "index=main",
  earliest: "-24h",
  latest: "now",
  timezone: "UTC",
  run: "0",
}).toString()}`;

/**
 * What the boot script wrote before any page script ran. Installed as an init
 * script, the observer sees the very first `data-palette` write on `<html>`
 * and whether `<body>` had been parsed yet. Only the inline `<head>` script
 * writes before `<body>` exists; hydration and ThemeSync run after it, so
 * `beforeBody: true` is what proves the boot script, not ThemeSync's
 * mount-time repaint, made the first write.
 */
interface FirstPaintRecord {
  beforeBody: boolean | null;
  firstPalette: string | null;
  firstTheme: string | null;
}

interface FirstPaintWindow {
  openSplunkPaletteSmoke?: FirstPaintRecord;
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function administratorHeaders(): Record<string, string> {
  return {
    Authorization: `Bearer ${administratorToken}`,
    "Content-Type": PROTOBUF_CONTENT_TYPE,
  };
}

async function readAppearance(request: APIRequestContext): Promise<GetServerAppearanceResponse> {
  const response = await request.post(APPEARANCE_GET_PATH, {
    headers: administratorHeaders(),
    data: Buffer.from(GetServerAppearanceRequest.encode({}).finish()),
  });
  expect(response.status(), "appearance get status").toBe(200);
  return GetServerAppearanceResponse.decode(new Uint8Array(await response.body()));
}

async function updateAppearance(
  request: APIRequestContext,
  expectedVersion: bigint,
  palette: UiPalette,
): Promise<UpdateServerAppearanceResponse> {
  const response = await request.post(APPEARANCE_UPDATE_PATH, {
    headers: administratorHeaders(),
    data: Buffer.from(UpdateServerAppearanceRequest.encode({ expectedVersion, palette }).finish()),
  });
  expect(response.status(), `appearance update to ${UiPalette[palette]} status`).toBe(200);
  return UpdateServerAppearanceResponse.decode(new Uint8Array(await response.body()));
}

function cachedPalette(page: Page): Promise<string | null> {
  return page.evaluate((key) => window.localStorage.getItem(key), PALETTE_STORAGE_KEY);
}

function storedTheme(page: Page): Promise<string | null> {
  return page.evaluate((key) => window.localStorage.getItem(key), THEME_STORAGE_KEY);
}

function firstPaint(page: Page): Promise<FirstPaintRecord | undefined> {
  return page.evaluate(() => (window as unknown as FirstPaintWindow).openSplunkPaletteSmoke);
}

/** Loads a page and resolves once its one bootstrap request has answered. */
async function loadWithBootstrap(page: Page, url: string): Promise<number> {
  const bootstrap = page.waitForResponse((response) => response.url().endsWith("/api/system/bootstrap"));
  await page.goto(url, { waitUntil: "domcontentloaded" });
  return (await bootstrap).status();
}

/** Alpha channel of a computed colour: `color(srgb r g b / a)` or `rgba(r, g, b, a)`; opaque forms read as 1. */
function alphaOf(color: string): number {
  const srgb = /^color\(srgb [\d.]+ [\d.]+ [\d.]+(?: \/ ([\d.%]+))?\)$/u.exec(color);
  if (srgb) return srgb[1] === undefined ? 1 : parseAlpha(srgb[1]);
  const legacy = /^rgba?\([\d.]+, [\d.]+, [\d.]+(?:, ([\d.%]+))?\)$/u.exec(color);
  if (legacy) return legacy[1] === undefined ? 1 : parseAlpha(legacy[1]);
  throw new Error(`unrecognised computed colour ${JSON.stringify(color)}`);
}

function parseAlpha(value: string): number {
  return value.endsWith("%") ? Number.parseFloat(value) / 100 : Number.parseFloat(value);
}

async function openUserMenu(page: Page) {
  const menu = page.locator(".floating-menu.user-menu");
  if (!(await menu.isVisible())) {
    await page.getByRole("button", { name: "User menu" }).click();
  }
  await expect(menu).toBeVisible();
  return menu;
}

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    const record: FirstPaintRecord = { beforeBody: null, firstPalette: null, firstTheme: null };
    (window as unknown as FirstPaintWindow).openSplunkPaletteSmoke = record;
    // `document.documentElement` may not exist yet when an init script runs,
    // so observe the document and wait for the attribute to land on <html>.
    const observer = new MutationObserver(() => {
      const root = document.documentElement;
      const palette = root?.getAttribute("data-palette") ?? null;
      if (palette === null) return;
      record.beforeBody = document.body === null;
      record.firstPalette = palette;
      record.firstTheme = root.getAttribute("data-theme");
      observer.disconnect();
    });
    observer.observe(document, { attributes: true, childList: true, subtree: true });
  });
});

test("the administrator's palette reaches bootstrap, the cache, the boot script and the paint", async ({ page, request }) => {
  const html = page.locator("html");

  await test.step("a fresh browser paints classic and caches it from bootstrap", async () => {
    expect(await loadWithBootstrap(page, "/signin/")).toBe(200);
    await expect(html).toHaveAttribute("data-palette", "classic");
    await expect(html).toHaveAttribute("data-theme", "light");
    await expect.poll(() => cachedPalette(page)).toBe("classic");
    expect(await firstPaint(page)).toEqual({ beforeBody: true, firstPalette: "classic", firstTheme: "light" });
    const current = await readAppearance(request);
    expect(current.current?.version).toBe(0n);
    expect(current.current?.palette).toBe(UiPalette.UI_PALETTE_CLASSIC);
    expect(current.defaultPalette).toBe(UiPalette.UI_PALETTE_CLASSIC);
  });

  await test.step("the administrator updates the palette to terminal", async () => {
    const updated = await updateAppearance(request, 0n, UiPalette.UI_PALETTE_TERMINAL);
    expect(updated.current?.version).toBe(1n);
    expect(updated.current?.palette).toBe(UiPalette.UI_PALETTE_TERMINAL);
  });

  await test.step("the next load paints terminal after hydration and caches it", async () => {
    expect(await loadWithBootstrap(page, "/signin/")).toBe(200);
    // The boot script still had the classic cache; the sign-in page's one
    // bootstrap request reaches ThemeSync, which corrects it.
    expect(await firstPaint(page)).toEqual({ beforeBody: true, firstPalette: "classic", firstTheme: "light" });
    await expect(html).toHaveAttribute("data-palette", "terminal");
    await expect.poll(() => cachedPalette(page)).toBe("terminal");
  });

  await test.step("the boot script paints the cached palette before any network response", async () => {
    let blockedBootstraps = 0;
    await page.route(BOOTSTRAP_ROUTE, async (route) => {
      blockedBootstraps += 1;
      await route.abort("failed");
    });
    await page.goto("/signin/", { waitUntil: "domcontentloaded" });
    expect(await firstPaint(page)).toEqual({ beforeBody: true, firstPalette: "terminal", firstTheme: "light" });
    await expect(html).toHaveAttribute("data-palette", "terminal");
    // The sign-in page asked, got nothing, and left the cached paint alone.
    await expect.poll(() => blockedBootstraps).toBeGreaterThan(0);
    await expect(html).toHaveAttribute("data-palette", "terminal");
    expect(await cachedPalette(page)).toBe("terminal");
    await page.unroute(BOOTSTRAP_ROUTE);
  });

  await test.step("the user's dark preference sits on top of the instance palette", async () => {
    expect(await loadWithBootstrap(page, SEARCH_URL)).toBe(200);
    await expect(html).toHaveAttribute("data-palette", "terminal");
    const menu = await openUserMenu(page);
    await menu.getByRole("menuitemradio", { name: "Dark" }).click();
    await expect(html).toHaveAttribute("data-theme", "dark");
    await expect(html).toHaveAttribute("data-palette", "terminal");
    expect(await storedTheme(page)).toBe("dark");
    // Both attributes now come from the boot script on the next load.
    await page.route(BOOTSTRAP_ROUTE, (route) => route.abort("failed"));
    await page.goto("/signin/", { waitUntil: "domcontentloaded" });
    expect(await firstPaint(page)).toEqual({ beforeBody: true, firstPalette: "terminal", firstTheme: "dark" });
    await expect(html).toHaveAttribute("data-theme", "dark");
    await expect(html).toHaveAttribute("data-palette", "terminal");
    await page.unroute(BOOTSTRAP_ROUTE);
  });

  await test.step("glass paints a translucent, blurred user menu", async () => {
    const updated = await updateAppearance(request, 1n, UiPalette.UI_PALETTE_GLASS);
    expect(updated.current?.version).toBe(2n);
    expect(await loadWithBootstrap(page, SEARCH_URL)).toBe(200);
    await expect(html).toHaveAttribute("data-palette", "glass");
    await expect(html).toHaveAttribute("data-theme", "dark");
    await expect.poll(() => cachedPalette(page)).toBe("glass");
    const menu = await openUserMenu(page);
    const surface = await menu.evaluate((element) => {
      const style = window.getComputedStyle(element);
      return { background: style.backgroundColor, backdrop: style.backdropFilter };
    });
    const alpha = alphaOf(surface.background);
    expect(alpha, `user menu background ${surface.background}`).toBeLessThan(1);
    expect(alpha, `user menu background ${surface.background}`).toBeGreaterThanOrEqual(0.8);
    expect(surface.backdrop, "user menu backdrop-filter").not.toBe("none");
    expect(surface.backdrop).toContain("blur(");
  });

  await test.step("an out-of-range palette is rejected and the paint stays glass", async () => {
    // Raw wire bytes: field 1 varint 2 (expected_version), field 2 varint 99
    // (palette), a number no UiPalette value carries.
    const rejected = await request.post(APPEARANCE_UPDATE_PATH, {
      headers: administratorHeaders(),
      data: Buffer.from([0x08, 0x02, 0x10, 0x63]),
    });
    expect(rejected.status(), "out-of-range palette status").toBe(400);
    const stale = await request.post(APPEARANCE_UPDATE_PATH, {
      headers: administratorHeaders(),
      data: Buffer.from(UpdateServerAppearanceRequest.encode({
        expectedVersion: 0n,
        palette: UiPalette.UI_PALETTE_OCEAN,
      }).finish()),
    });
    expect(stale.status(), "stale expected_version status").toBe(409);
    const current = await readAppearance(request);
    expect(current.current?.version).toBe(2n);
    expect(current.current?.palette).toBe(UiPalette.UI_PALETTE_GLASS);

    expect(await loadWithBootstrap(page, "/signin/")).toBe(200);
    expect(await firstPaint(page)).toEqual({ beforeBody: true, firstPalette: "glass", firstTheme: "dark" });
    await expect(html).toHaveAttribute("data-palette", "glass");
    await expect(html).toHaveAttribute("data-theme", "dark");
    expect(await cachedPalette(page)).toBe("glass");
  });
});
