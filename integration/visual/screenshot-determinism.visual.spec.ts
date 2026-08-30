import { expect, test, type Page } from "@playwright/test";

import { awaitSettledSearchResults, gotoVisualRoute, settleVisualPage } from "./visual-harness";

/**
 * Proof that the visual baselines describe a fixed rendering.
 *
 * A committed baseline only pins appearance if the page paints the same thing
 * every time. When a surface samples the wall clock, a random value, or an
 * animation phase, the suite still passes whenever the drift happens to land
 * inside `maxDiffPixelRatio`, and the safety net quietly stops holding the next
 * refactor. These tests take the picture twice instead of comparing it to a
 * committed file, so nothing hides behind that ratio: back to back, which
 * catches a live animation or a timer-driven repaint, and again after a reload,
 * which catches anything seeded per page load.
 *
 * Failures name the surface rather than a pixel budget. The fix is to pin
 * whatever varies, never to widen a tolerance.
 */

/** Capture options matching the ones the committed baselines are taken with. */
const CAPTURE = { animations: "disabled", caret: "hide", scale: "css" } as const;

/**
 * Largest per-channel difference treated as the rasteriser rather than the page.
 *
 * Chromium's text rendering is not bit-reproducible: single channels move by
 * one unit between runs of the same build, which no reader can see. Anything a
 * page actually changed -- a shifted element, a different number, a half-played
 * animation -- moves channels far further than this at the edges it touches.
 */
const RASTERISER_TOLERANCE = 2;

async function capturePage(page: Page, fullPage: boolean): Promise<Buffer> {
  await settleVisualPage(page);
  return page.screenshot({ ...CAPTURE, fullPage });
}

interface RenderingDifference {
  readonly count: number;
  readonly examples: readonly string[];
  readonly summary: string;
}

/**
 * Compares two screenshots in a scratch page, ignoring rasteriser jitter.
 *
 * The decoding happens in a page of its own so the surface under test is never
 * touched, and it uses canvas rather than an image-diffing dependency: the
 * suite must stay free of new runtime and build dependencies.
 */
async function compareRenderings(page: Page, before: Buffer, after: Buffer): Promise<RenderingDifference> {
  const scratch = await page.context().newPage();
  try {
    return await scratch.evaluate(async ([left, right, tolerance]) => {
      const [one, two] = await Promise.all([left as string, right as string].map(async (encoded) => {
        const image = new Image();
        image.src = `data:image/png;base64,${encoded}`;
        await image.decode();
        const canvas = document.createElement("canvas");
        canvas.width = image.width;
        canvas.height = image.height;
        const context = canvas.getContext("2d");
        if (context === null) throw new Error("the scratch page has no 2d canvas context");
        context.drawImage(image, 0, 0);
        return context.getImageData(0, 0, image.width, image.height);
      }));
      if (one.width !== two.width || one.height !== two.height) {
        return {
          count: Number.POSITIVE_INFINITY,
          examples: [],
          summary: `the captures are ${one.width}x${one.height} and ${two.width}x${two.height}`,
        };
      }
      const limit = tolerance as number;
      const examples: string[] = [];
      let count = 0;
      for (let index = 0; index < one.data.length; index += 4) {
        let moved = false;
        for (let channel = 0; channel < 4; channel += 1) {
          if (Math.abs(one.data[index + channel] - two.data[index + channel]) > limit) moved = true;
        }
        if (!moved) continue;
        count += 1;
        if (examples.length < 5) {
          const pixel = index / 4;
          examples.push(`(${pixel % one.width},${Math.floor(pixel / one.width)})`);
        }
      }
      return { count, examples, summary: `${count} of ${one.width * one.height} pixels moved` };
    }, [before.toString("base64"), after.toString("base64"), RASTERISER_TOLERANCE] as const);
  } finally {
    await scratch.close();
  }
}

async function expectIdenticalRendering(
  page: Page,
  before: Buffer,
  after: Buffer,
  what: string,
): Promise<void> {
  const difference = await compareRenderings(page, before, after);
  expect(
    difference.count,
    `${what}: ${difference.summary}${difference.examples.length > 0 ? ` at ${difference.examples.join(" ")}` : ""}`,
  ).toBe(0);
}

/** Asserts a route paints identically twice in a row and again after a reload. */
async function expectRouteIsDeterministic(
  page: Page,
  route: string,
  ready: (page: Page) => Promise<void> = async () => undefined,
): Promise<void> {
  await gotoVisualRoute(page, route);
  await ready(page);
  const first = await capturePage(page, true);
  const second = await capturePage(page, true);
  await expectIdenticalRendering(page, first, second, `${route} recaptured without touching it`);

  await page.reload({ waitUntil: "networkidle" });
  await ready(page);
  const reloaded = await capturePage(page, true);
  await expectIdenticalRendering(page, first, reloaded, `${route} after a reload`);
}

test.describe("visual determinism", () => {
  test("home launcher", async ({ page }) => {
    await expectRouteIsDeterministic(page, "/");
  });

  test("search workspace with demo results", async ({ page }) => {
    // Waiting for the event list alone accepts two page shapes 370 pixels
    // apart, which the comparison below reports as the route painting two
    // different sizes. The baselines wait for the settled state; so must this.
    await expectRouteIsDeterministic(page, "/search/", awaitSettledSearchResults);
  });

  test("analytics charts", async ({ page }) => {
    await expectRouteIsDeterministic(page, "/analytics/");
  });

  test("activity console", async ({ page }) => {
    await expectRouteIsDeterministic(page, "/activity/");
  });

  test("index dialog entrance settles at the same frame", async ({ page }) => {
    // The dialog runs a 140ms `modal-in` entrance. Opening it twice from a
    // fresh load is the case that made the export dialog's baseline flake.
    const openDialog = async (): Promise<Buffer> => {
      await gotoVisualRoute(page, "/admin/?section=indexes");
      await page.getByRole("button", { name: "Simulate index" }).click();
      await expect(page.getByTestId("modal-layer")).toBeVisible();
      return capturePage(page, false);
    };
    const first = await openDialog();
    const second = await openDialog();
    await expectIdenticalRendering(page, first, second, "the index dialog reopened from a fresh load");
  });

  test("the comparison can tell two renderings apart", async ({ page }) => {
    // Without this, every assertion above would still pass if `page.screenshot`
    // started returning a constant, and the whole file would be decorative.
    await gotoVisualRoute(page, "/");
    const before = await capturePage(page, false);
    await page.evaluate(() => {
      // A fixed overlay rather than a body background: the app shell paints its
      // own full-height surface, so a background change under it is invisible.
      const marker = document.createElement("div");
      marker.setAttribute("style", "background:#ff00ff;inset:0;position:fixed;z-index:2147483647");
      document.body.append(marker);
    });
    const after = await capturePage(page, false);
    const difference = await compareRenderings(page, before, after);
    expect(difference.count, "capturing the page twice cannot distinguish two renderings").toBeGreaterThan(0);
  });
});
