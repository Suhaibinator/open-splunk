import { defineConfig } from "@playwright/test";
import process from "node:process";

import visualConfig from "./playwright.visual.config";

/**
 * The visual suite, pointed at a scratch baseline set and run with no tolerance.
 *
 * `scripts/visual-determinism.mjs` drives it twice over one build: the first
 * pass writes every screenshot into a temporary directory, the second compares
 * against what the first wrote. Because the comparison is exact, a surface that
 * samples the clock, a random value, or an animation phase fails here even when
 * its drift is small enough to hide inside the committed baselines' tolerance.
 *
 * The build and the static servers belong to the runner so both passes render
 * the same bytes, which is why this configuration drops the `webServer` the
 * visual configuration starts.
 */

const snapshotRoot = process.env.OPEN_SPLUNK_VISUAL_SNAPSHOT_ROOT;
if (snapshotRoot === undefined || snapshotRoot.length === 0) {
  throw new Error(
    "OPEN_SPLUNK_VISUAL_SNAPSHOT_ROOT must name the scratch baseline directory. "
      + "Run this configuration through `npm run test:visual:determinism`.",
  );
}

export default defineConfig({
  ...visualConfig,
  expect: {
    toHaveScreenshot: {
      animations: "disabled",
      caret: "hide",
      // Not one pixel may differ. Both passes render one build on one machine,
      // so a moved element has nowhere to hide behind a ratio.
      //
      // `threshold` stays at Playwright's default rather than dropping to 0:
      // Chromium's text rasteriser is not bit-reproducible and jitters single
      // channels by one unit between runs, which is invisible and would only
      // teach a reader to ignore this gate. Anything a person could see clears
      // the default per-pixel threshold and fails here.
      maxDiffPixelRatio: 0,
      maxDiffPixels: 0,
      scale: "css",
    },
  },
  outputDir: "./test-results/visual-determinism",
  snapshotPathTemplate: `${snapshotRoot}/{projectName}/{arg}{ext}`,
  webServer: undefined,
});
