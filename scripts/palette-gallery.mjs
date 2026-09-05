// Screenshot gallery of every palette x mode on three shipped pages.
//
// Drives the demo-mode static export in `out/` (build it first with
// `npm run build`) through the same static server the workspace suite uses,
// on a free port, and captures the sign-in page, the search workspace with
// results and the completion menu open, and the admin Server section, once
// per palette in light and in dark. Each capture seeds the palette and theme
// caches in localStorage before navigation, so the shipped boot script paints
// the combination pre-hydration exactly as a real load would. The demo admin
// console has no Appearance card (that is backend-only), so the card is
// rebuilt from the same copy the React card renders, `PALETTE_OPTIONS` in
// app/admin/appearance-form.ts, and inserted into the demo Server section
// after its first settings card.
//
// The palette list is `PALETTES` from lib/palettes.ts, imported rather than
// restated, so a new palette joins the gallery the moment it joins the list.
// The TypeScript modules are loaded through node's own type stripping; the
// hooks below only teach the resolver the `@/` alias tsconfig.json declares.
//
// Usage: node scripts/palette-gallery.mjs [--out <dir>] [--only <palette>]
//
// Writes one PNG per page and combination plus `manifest.json` under the
// output directory (`test-results/palette-gallery` by default), and lays the
// captures out as `gallery-<page>.png`: one row per palette, light beside
// dark. A capture that painted a different palette or theme from the one it
// seeded, or a page error, fails the run after everything has been written.
import { chromium } from "@playwright/test";
import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { createServer } from "node:net";
import path from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function parseArguments(values) {
  const options = { only: null, out: path.join(repository, "test-results", "palette-gallery") };
  const remaining = [...values];
  while (remaining.length > 0) {
    const flag = remaining.shift();
    const value = remaining.shift();
    if (value === undefined) throw new Error(`${flag} needs a value; usage: node scripts/palette-gallery.mjs [--out <dir>] [--only <palette>]`);
    if (flag === "--out") options.out = path.resolve(value);
    else if (flag === "--only") options.only = value;
    else throw new Error(`unknown option ${flag}; usage: node scripts/palette-gallery.mjs [--out <dir>] [--only <palette>]`);
  }
  return options;
}

const { only, out } = parseArguments(process.argv.slice(2));

registerHooks({
  resolve(specifier, context, next) {
    if (!specifier.startsWith("@/")) return next(specifier, context);
    const base = path.join(repository, specifier.slice(2));
    const found = [base, `${base}.ts`, `${base}.tsx`, path.join(base, "index.ts")].find((candidate) => existsSync(candidate));
    if (found === undefined) throw new Error(`cannot resolve ${specifier} under ${repository}`);
    return next(pathToFileURL(found).href, context);
  },
  load(url, context, next) {
    if (!url.endsWith(".ts")) return next(url, context);
    return { format: "module-typescript", shortCircuit: true, source: readFileSync(fileURLToPath(url), "utf8") };
  },
});

const { PALETTES } = await import(pathToFileURL(path.join(repository, "lib", "palettes.ts")).href);
const { APPEARANCE_DESCRIPTION, APPEARANCE_TITLE, paletteOptionId, paletteOptions } = await import(
  pathToFileURL(path.join(repository, "app", "admin", "appearance-form.ts")).href
);

if (only !== null && !PALETTES.includes(only)) throw new Error(`--only ${only} is not a palette; the list is ${PALETTES.join(", ")}`);

const MODES = ["light", "dark"];
const PAGES = {
  admin: "Admin: Server section",
  search: "Search workspace: results and completion menu",
  signin: "Sign-in page",
};
const WIDTH = 1280;
const HEIGHT = 900;
const CELL_WIDTH = 640;
const CELL_HEIGHT = 450;
const GRID_GAP = 14;
const GRID_PADDING = 16;

/** Runs `visit` for each value in turn; the captures share one browser and the composer one tab. */
async function sequentially(values, visit) {
  const iterator = values[Symbol.iterator]();
  async function visitNext() {
    const next = iterator.next();
    if (next.done) return;
    await visit(next.value);
    await visitNext();
  }
  await visitNext();
}

/** A port nothing else holds, so two checkouts can run the gallery at once. */
function freePort() {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address();
      probe.close(() => resolve(port));
    });
  });
}

function escapeHtml(text) {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

/** The Appearance card as app/admin/appearance-settings.tsx renders it, with `selected` saved and checked. */
function appearanceCardMarkup(selected) {
  const radios = paletteOptions().map(([palette, option]) => {
    const id = paletteOptionId(palette);
    const checked = palette === selected;
    return `<label${checked ? ' class="is-selected"' : ""} for="${id}">`
      + `<input aria-describedby="${id}-description" aria-label="${escapeHtml(option.label)}"${checked ? " checked" : ""} id="${id}" name="appearance-palette" type="radio" value="${palette}">`
      + `<span><strong>${escapeHtml(option.label)}</strong><small id="${id}-description">${escapeHtml(option.description)}</small></span></label>`;
  }).join("");
  return `<section class="suite-card settings-group"><header><h3>${escapeHtml(APPEARANCE_TITLE)}</h3>`
    + `<p>${escapeHtml(APPEARANCE_DESCRIPTION)}</p></header>`
    + `<div class="appearance-palette-options" role="radiogroup" aria-label="Palette">${radios}</div></section>`
    + '<div class="settings-actions"><button class="button button--secondary" type="button"'
    + `${selected === PALETTES[0] ? " disabled" : ""}>Reset to default</button>`
    + '<button class="button button--primary" disabled type="submit">Apply</button></div>';
}

async function waitForServer(origin) {
  const deadline = Date.now() + 30_000;
  async function attempt() {
    let body = null;
    try {
      const response = await fetch(`${origin}/signin/`);
      if (response.ok) body = await response.text();
    } catch {
      // not up yet
    }
    if (body !== null) {
      // The export must carry the palette boot branch; anything else is a
      // stale build or another checkout's export.
      if (!body.includes('setAttribute("data-palette"')) throw new Error(`${origin} serves an export without the palette boot script; run npm run build`);
      return;
    }
    if (Date.now() > deadline) throw new Error("the static server did not come up");
    await new Promise((resolve) => setTimeout(resolve, 250));
    await attempt();
  }
  await attempt();
}

/** Entry animations on menus and toasts, and the demo runner's last tick. */
async function settle(page) {
  await page.waitForTimeout(500);
}

async function captureSignIn(page, origin, file) {
  await page.goto(`${origin}/signin/`, { waitUntil: "networkidle" });
  await settle(page);
  await page.screenshot({ path: file });
}

async function captureSearch(page, origin, file) {
  await page.goto(`${origin}/search/events/?${new URLSearchParams({ q: "index=main", run: "0" })}`, { waitUntil: "networkidle" });
  const editor = page.getByTestId("search-input");
  await editor.waitFor();
  await editor.click();
  await editor.press("End");
  await page.keyboard.press("Control+Enter");
  const runButton = page.getByTestId("run-search");
  await runButton.evaluate((button) => new Promise((resolve) => {
    const done = () => button.getAttribute("aria-label") === "Run search";
    const observer = new MutationObserver(() => {
      if (done()) {
        observer.disconnect();
        resolve();
      }
    });
    observer.observe(button, { attributes: true });
    setTimeout(() => {
      observer.disconnect();
      resolve();
    }, 8000);
  }));
  await page.getByTestId("result-tab-events").waitFor();
  // The run scrolls the results into view and leaves the composer above the
  // fold; the gallery wants the editor, its menu and the first rows together.
  await page.evaluate(() => window.scrollTo(0, 0));
  await editor.click();
  await editor.press("End");
  await page.keyboard.press("Control+Space");
  const menu = page.getByTestId("completion-menu");
  await menu.waitFor();
  await settle(page);
  // Focusing the editor after the run can nudge the window by a few pixels
  // (the caret's scrollIntoView), which clips the title under the mono face;
  // pin the scroll once everything has settled and prove it stuck.
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(150);
  const scrollY = await page.evaluate(() => window.scrollY);
  if (scrollY !== 0) throw new Error(`the search page is still scrolled by ${scrollY}px`);
  const box = await menu.boundingBox();
  if (box === null || box.y < 0 || box.y + box.height > HEIGHT) throw new Error(`completion menu outside the viewport: ${JSON.stringify(box)}`);
  await page.screenshot({ path: file });
}

async function captureAdmin(page, origin, file, palette) {
  await page.goto(`${origin}/admin/?section=server`, { waitUntil: "networkidle" });
  await page.locator(".server-settings").waitFor();
  await page.evaluate((markup) => {
    const form = document.querySelector(".server-settings");
    const firstCard = form.querySelector(".settings-group");
    const template = document.createElement("template");
    template.innerHTML = markup;
    firstCard.after(template.content);
  }, appearanceCardMarkup(palette));
  await settle(page);
  await page.screenshot({ path: file });
}

/** One palette x mode: a fresh context with the caches seeded, three captures, and the attributes each page painted. */
async function captureCombination(browser, origin, shots, palette, mode) {
  const context = await browser.newContext({ colorScheme: mode, deviceScaleFactor: 1, viewport: { height: HEIGHT, width: WIDTH } });
  await context.addInitScript(([nextPalette, nextMode]) => {
    try {
      localStorage.setItem("open-splunk.palette", nextPalette);
      localStorage.setItem("open-splunk.theme", nextMode);
    } catch {
      // storage blocked
    }
  }, [palette, mode]);
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(String(error)));
  const files = Object.fromEntries(Object.keys(PAGES).map((name) => [name, path.join(shots, `${name}-${palette}-${mode}.png`)]));
  const painted = {};
  const problems = [];
  const readAttributes = async (name) => {
    painted[name] = await page.evaluate(() => [
      document.documentElement.getAttribute("data-palette"),
      document.documentElement.getAttribute("data-theme"),
    ]);
    if (painted[name][0] !== palette || painted[name][1] !== mode) problems.push(`${palette} ${mode} ${name}: the page painted ${painted[name].join("/")}`);
  };
  try {
    await captureSignIn(page, origin, files.signin);
    await readAttributes("signin");
    await captureSearch(page, origin, files.search);
    await readAttributes("search");
    await captureAdmin(page, origin, files.admin, palette);
    await readAttributes("admin");
  } finally {
    await context.close();
  }
  for (const error of errors) problems.push(`${palette} ${mode}: page error ${error}`);
  process.stdout.write(`captured ${palette} ${mode}\n`);
  return { errors, files, mode, painted, palette, problems };
}

async function dataUri(file) {
  return `data:image/png;base64,${(await readFile(file)).toString("base64")}`;
}

/** Lays the captures of one page out as a grid, one row per palette, light beside dark. */
async function composePage(browser, shots, palettes, name, title) {
  const cells = [];
  await sequentially(palettes, async (palette) => {
    await sequentially(MODES, async (mode) => {
      const uri = await dataUri(path.join(shots, `${name}-${palette}-${mode}.png`));
      cells.push(`<figure><figcaption>${palette} &middot; ${mode}</figcaption><img src="${uri}" alt="${name} ${palette} ${mode}"></figure>`);
    });
  });
  // The grid's own chrome is deliberately colourless: the captures carry the
  // palettes, and the page around them takes the browser's defaults.
  const html = `<!doctype html><html><head><meta charset="utf-8"><style>
    body { margin: 0; padding: ${GRID_PADDING}px; }
    h1 { font-size: 1.25em; margin: 0 0 12px; }
    .grid { display: grid; gap: ${GRID_GAP}px; grid-template-columns: repeat(2, ${CELL_WIDTH}px); }
    figure { margin: 0; }
    figcaption { font-weight: 600; margin-bottom: 4px; }
    img { display: block; height: ${CELL_HEIGHT}px; outline: 1px solid; width: ${CELL_WIDTH}px; }
  </style></head><body><h1>${title} at ${WIDTH}x${HEIGHT}, every palette x mode</h1><div class="grid">${cells.join("")}</div></body></html>`;
  const tab = await browser.newPage({ deviceScaleFactor: 1, viewport: { height: HEIGHT, width: CELL_WIDTH * 2 + GRID_GAP + GRID_PADDING * 2 } });
  await tab.setContent(html);
  await tab.waitForFunction(() => [...document.images].every((image) => image.complete));
  const output = path.join(out, `gallery-${name}.png`);
  await tab.screenshot({ fullPage: true, path: output });
  await tab.close();
  process.stdout.write(`${output}\n`);
}

async function main() {
  const shots = path.join(out, "shots");
  await mkdir(shots, { recursive: true });
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  const server = spawn(process.execPath, [path.join(repository, "scripts", "serve-static-ui.mjs")], {
    cwd: repository,
    env: { ...process.env, PORT: String(port) },
    stdio: "ignore",
  });
  const palettes = PALETTES.filter((name) => only === null || name === only);
  const browser = await chromium.launch();
  const manifest = [];
  try {
    await waitForServer(origin);
    await sequentially(palettes, async (palette) => {
      await sequentially(MODES, async (mode) => {
        manifest.push(await captureCombination(browser, origin, shots, palette, mode));
      });
    });
    await writeFile(path.join(shots, "manifest.json"), JSON.stringify(manifest, null, 2));
    await sequentially(Object.entries(PAGES), async ([name, title]) => {
      await composePage(browser, shots, palettes, name, title);
    });
  } finally {
    await browser.close();
    server.kill();
  }
  const problems = manifest.flatMap((entry) => entry.problems);
  if (problems.length > 0) throw new Error(`the gallery is not evidence of what it seeded:\n${problems.join("\n")}`);
}

await main();
