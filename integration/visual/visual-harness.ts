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
