import { expect, type Locator, type Page } from "@playwright/test";

/**
 * Every visual page renders against this instant so relative labels, resolved
 * time ranges, and greeting copy cannot drift between runs. Timers keep
 * running, so client components still reach their settled state.
 */
export const VISUAL_FIXED_CLOCK = new Date("2026-03-17T15:30:00.000Z");

/**
 * Navigates to an exported route and waits until it is visually settled.
 *
 * CSS animations, transitions, and the text caret are frozen by the shared
 * `toHaveScreenshot` options in `playwright.visual.config.ts`; this only has
 * to pin the clock and wait for fonts and two paints.
 */
export async function gotoVisualRoute(page: Page, route: string): Promise<void> {
  await page.clock.setFixedTime(VISUAL_FIXED_CLOCK);
  await page.goto(route, { waitUntil: "networkidle" });
  await settleVisualPage(page);
}

/**
 * Waits for font metrics, entrance animations, and two animation frames so
 * layout has stopped moving.
 *
 * `toHaveScreenshot` fast-forwards animations at capture time, but it first
 * compares consecutive frames to decide the page has stopped changing, and a
 * short entrance animation can satisfy that comparison while still in flight:
 * `.modal-card` runs `modal-in` for 140ms, so a dialog screenshot could land on
 * the half-travelled `translateY(-8px)` frame instead of the resting one.
 * Awaiting the finite animations pins the finished state every run.
 */
export async function settleVisualPage(page: Page): Promise<void> {
  await page.evaluate(async () => {
    await document.fonts.ready;
    await Promise.all(
      document.getAnimations().map(async (animation) => {
        // A looping or paused animation never reaches `finished`; the capture
        // freezes those instead, so waiting on them would hang the test.
        const timing = animation.effect?.getComputedTiming();
        if (animation.playState === "paused" || timing?.iterations === Number.POSITIVE_INFINITY) {
          return;
        }
        await animation.finished.catch(() => undefined);
      }),
    );
    await new Promise((resolve) => {
      requestAnimationFrame(() => requestAnimationFrame(resolve));
    });
  });
}

/**
 * Waits for the exact result state the search workspace is photographed in.
 *
 * The event list appears before the job settles, so waiting only for it accepts
 * more than one page shape. Two of them differ by roughly 370 pixels of height:
 * above the phone breakpoint the first event is expanded over its EVENT FIELDS
 * panel, and at or below it a mount effect collapses that event along with the
 * fields rail. A run that hydrates with the phone layout in effect therefore
 * paints a much shorter desktop page -- which fails a baseline on a surface
 * nobody touched, and reads to the determinism gate as the route rendering two
 * different pages (1440x1583 against 1440x1213). Pinning the finished status
 * and the expansion the viewport implies turns that race into a wait, and into
 * an honest failure if it never resolves.
 */
export async function awaitSettledSearchResults(page: Page): Promise<void> {
  await expect(page.getByTestId("search-workspace")).toBeVisible();
  await expect(page.getByTestId("event-list")).toBeVisible();

  const strip = page.getByTestId("job-strip");
  await expect(strip).toContainText("Completed");
  await expect(strip).toContainText("100%");

  // The same query the workspace itself uses to decide the phone layout.
  const phoneLayout = await page.evaluate(() => globalThis.matchMedia("(max-width: 760px)").matches);
  const firstEvent = page.locator(".event-row").first();
  if (phoneLayout) await expect(firstEvent).not.toHaveClass(/\bexpanded\b/u);
  else await expect(firstEvent).toHaveClass(/\bexpanded\b/u);

  await settleVisualPage(page);
}

/** Pins a full exported page, including everything below the fold. */
export async function expectPageScreenshot(page: Page, name: string): Promise<void> {
  await expect(page).toHaveScreenshot(`${name}.png`, { fullPage: true });
}

/**
 * Pins only the viewport.
 *
 * Overlay surfaces (modal backdrops, drawers) are positioned against the
 * viewport, so stitching a taller page would not describe what a reader sees.
 */
export async function expectViewportScreenshot(page: Page, name: string): Promise<void> {
  await expect(page).toHaveScreenshot(`${name}.png`, { fullPage: false });
}

/** Pins one component surface without the surrounding page chrome. */
export async function expectRegionScreenshot(region: Locator, name: string): Promise<void> {
  await expect(region).toHaveScreenshot(`${name}.png`);
}

/**
 * Pins one component surface where it sits in the document.
 *
 * An element screenshot scrolls its subject into view first. For a component
 * taller than the viewport that puts the component's top row at the top of the
 * viewport -- underneath the sticky product and app bars, which then occupy the
 * pixels the component's own header should be in. The fields rail is 1137px
 * against a 900px viewport, so its baseline photographed 72px of product chrome
 * where "Hide Fields" and the filter input belong.
 *
 * Clipping a full-page capture to the component's document box captures the
 * same rectangle without scrolling, so the sticky bars stay where an unscrolled
 * reader sees them and the component is photographed whole.
 */
export async function expectDocumentRegionScreenshot(region: Locator, name: string): Promise<void> {
  const page = region.page();
  await page.evaluate(() => window.scrollTo(0, 0));
  await settleVisualPage(page);
  const clip = await region.evaluate((element: HTMLElement) => {
    const box = element.getBoundingClientRect();
    return {
      height: box.height,
      width: box.width,
      x: box.left + window.scrollX,
      y: box.top + window.scrollY,
    };
  });
  await expect(page).toHaveScreenshot(`${name}.png`, { clip, fullPage: true });
}
