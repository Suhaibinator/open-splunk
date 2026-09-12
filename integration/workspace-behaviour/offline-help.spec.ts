import { readFile } from "node:fs/promises";

import { expect, test } from "@playwright/test";

test("documentation search waits for its client handler before accepting input", async ({ page }) => {
  let releaseScripts!: () => void;
  const scriptsReady = new Promise<void>((resolve) => { releaseScripts = resolve; });
  let scriptRequested!: () => void;
  const scriptRequest = new Promise<void>((resolve) => { scriptRequested = resolve; });
  await page.route("**/_next/static/**/*.js", async (route) => {
    scriptRequested();
    await scriptsReady;
    await route.continue();
  });
  try {
    await page.goto("/help/", { waitUntil: "commit" });
    await scriptRequest;
    const search = page.getByRole("searchbox", { name: "Search documentation" });
    await expect(search).toBeVisible();
    await expect(search).toBeDisabled();
    releaseScripts();
    await expect(search).toBeEnabled();
    await search.fill("saved search");
    await expect(search).toHaveValue("saved search");
    const results = page.getByRole("region", { name: "Documentation search results" });
    await expect(results.getByRole("status")).toHaveText(/[1-9][0-9]* documentation results?\./u);
    await expect(results.getByRole("link").first()).toBeVisible();
  } finally {
    releaseScripts();
  }
});

test("bundled documentation navigates and searches with external networking blocked", async ({ page, baseURL }) => {
  const externalRequests: string[] = [];
  await page.route("**/*", async (route) => {
    if (new URL(route.request().url()).origin === new URL(baseURL!).origin) {
      await route.continue();
    } else {
      externalRequests.push(route.request().url());
      await route.abort("internetdisconnected");
    }
  });
  await page.goto("/help/");
  await expect(page.getByRole("heading", { name: "Documentation", exact: true })).toBeVisible();
  await expect(page.getByText(/Docs revision/u)).toBeVisible();
  const search = page.getByRole("searchbox", { name: "Search documentation" });
  await search.fill("timechart");
  await expect(page.getByRole("link", { name: /timechart/iu }).first()).toBeVisible();
  await search.fill("no-document-matches-this-phrase-41802");
  await expect(page.getByText(/no (?:documentation )?results|no documents/iu)).toBeVisible();
  await search.fill("");
  await page.locator('a[href="/help/collector-configuration/"]').first().click();
  await expect(page).toHaveURL(/\/help\/collector-configuration\//u);
  await expect(page.locator(".help-content")).toContainText("inputs");
  const example = page.locator('.help-content a').filter({ hasText: "configs/examples/collector-container.yaml" }).first();
  await expect(example).toHaveAttribute("href", /^\/help\//u);
  await example.click();
  await expect(page.locator(".help-content pre")).toContainText("inputs:");
  const exampleSource = await readFile("configs/examples/collector-container.yaml", "utf8");
  expect(await page.locator(".help-content pre code").textContent()).toBe(exampleSource);
  await page.reload();
  await expect(page.locator(".help-content pre")).toContainText("inputs:");
  expect(await page.locator(".help-content pre code").textContent()).toBe(exampleSource);
  expect(externalRequests).toEqual([]);
});

test("documentation retains semantic navigation, focus and mobile readability", async ({ page }) => {
  await page.setViewportSize({ width: 480, height: 900 });
  await page.goto("/help/spl/");
  await expect(page.getByRole("navigation", { name: "Documentation", exact: true })).toBeVisible();
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.locator("h1")).toHaveCount(1);
  const search = page.getByRole("searchbox", { name: "Search documentation" });
  await search.focus();
  await expect(search).toBeFocused();
  await page.keyboard.type("native ingestion");
  await page.keyboard.press("Tab");
  const focused = await page.evaluate(() => ({
    tag: document.activeElement?.tagName,
    href: document.activeElement?.getAttribute("href"),
  }));
  expect(focused.tag).toBe("A");
  expect(focused.href).toMatch(/^\/help\//u);
  const layout = await page.evaluate(() => ({
    width: document.documentElement.clientWidth,
    scroll: document.documentElement.scrollWidth,
    contentWidth: document.querySelector(".help-content")?.getBoundingClientRect().width,
  }));
  expect(layout.scroll).toBeLessThanOrEqual(layout.width);
  expect(layout.contentWidth).toBeGreaterThan(0);
});

test("shared ProductShell Help leads to the bundled documentation", async ({ page }) => {
  await page.goto("/?mode=demo");
  await page.getByRole("button", { name: "Help", exact: true }).click();
  await page.locator('#suite-help-popover a[href^="/help/"]').first().click();
  await expect(page).toHaveURL(/\/help\//u);
  await expect(page.getByRole("heading", { name: "Documentation", exact: true })).toBeVisible();
});

test("Help layout applies its responsive grid and scrollable code style contracts", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/help/spl/");
  const readStyles = () => page.evaluate(() => {
    const layout = document.querySelector(".help-layout");
    const code = document.querySelector(".help-content pre");
    if (layout === null || code === null) throw new Error("Help contract fixture is missing");
    const grid = getComputedStyle(layout);
    const codeStyle = getComputedStyle(code);
    return {
      columns: grid.gridTemplateColumns.split(" ").length,
      display: grid.display,
      overflowX: codeStyle.overflowX,
      whiteSpace: codeStyle.whiteSpace,
      codeWidth: code.getBoundingClientRect().width,
      contentWidth: code.parentElement!.getBoundingClientRect().width,
    };
  });
  const desktop = await readStyles();
  expect(desktop.display).toBe("grid");
  expect(desktop.columns).toBe(2);
  expect(desktop.overflowX).toBe("auto");
  expect(desktop.whiteSpace).toBe("pre");
  await page.setViewportSize({ width: 760, height: 900 });
  const compact = await readStyles();
  expect(compact.columns).toBe(1);
  expect(compact.codeWidth).toBeLessThanOrEqual(compact.contentWidth);
});
