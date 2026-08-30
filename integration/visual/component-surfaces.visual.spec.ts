import { expect, test } from "@playwright/test";

import { statsSparklineSegments } from "../../app/search-workspace/statistics-sparkline";

import { addApplicationStyles } from "./application-stylesheets";
import { expectRegionScreenshot, settleVisualPage } from "./visual-harness";

/**
 * Appearance of surfaces the application stylesheets style that no exported page
 * reaches.
 *
 * The rest of this suite photographs the shipped exports, which is the right
 * default: it pins what a reader actually sees. But a rule whose element never
 * renders in either export is invisible to every one of those baselines, so a
 * recolor or a geometry change there lands green. The statistics sparkline is
 * the case that proved it — it needs a server-supplied multivalue column that
 * neither the demo fixtures nor the intercepted backend responses produce — and
 * its stroke color was changed during this very phase without moving a pixel in
 * any committed PNG.
 *
 * These fixtures therefore mount the production markup against the real
 * stylesheet and photograph the component itself. They are as deterministic as
 * a page screenshot and considerably cheaper: no navigation, no clock, no data.
 */

/** Matches `STATS_SPARKLINE_WIDTH`/`HEIGHT` in the statistics panel. */
const SPARKLINE_WIDTH = 128;
const SPARKLINE_HEIGHT = 28;

/**
 * A fixed series with a rise, a fall, and one missing bucket.
 *
 * The gap splits the rendering into two polylines and one isolated circle, so
 * the baseline covers every shape `StatsSparklineCell` can emit.
 */
const SPARKLINE_SERIES: Array<number | null> = [
  4, 11, 7, 19, 16, 24, null, 31, null, 9, 15, 12, 27,
];

function sparklineMarkup(): string {
  const shapes = statsSparklineSegments(SPARKLINE_SERIES, SPARKLINE_WIDTH, SPARKLINE_HEIGHT)
    .map((segment) => (segment.length === 1
      ? `<circle cx="${segment[0]?.split(",")[0]}" cy="${segment[0]?.split(",")[1]}" r="1.75" />`
      : `<polyline points="${segment.join(" ")}" />`))
    .join("");
  return `<table class="statistics-table"><tbody><tr><td>`
    + `<svg class="statistics-sparkline" viewBox="0 0 ${SPARKLINE_WIDTH} ${SPARKLINE_HEIGHT}"`
    + ` aria-label="Sparkline">${shapes}</svg>`
    + `</td></tr></tbody></table>`;
}

test("the statistics sparkline renders its stroke, caps, and gaps", async ({ page }) => {
  await page.setContent(`<main>${sparklineMarkup()}</main>`);
  await addApplicationStyles(page);
  const sparkline = page.locator(".statistics-sparkline");
  await expect(sparkline).toBeVisible();
  await settleVisualPage(page);
  // A text-free fixture pins its colour as well as its shape. It used to need
  // its own tolerances for that, against a suite running on Playwright's
  // per-pixel default -- a YIQ distance loose enough to call two different
  // blues equal, and a recolor is what a token cleanup is most likely to get
  // wrong. The suite's own `threshold` and its zero pixel budget are both
  // stricter than those overrides were, so the fixture takes the suite's terms.
  await expectRegionScreenshot(sparkline, "statistics-sparkline");
});
