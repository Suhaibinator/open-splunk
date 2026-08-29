import { defineConfig, devices } from "@playwright/test";

import {
  BACKEND_EXPORT_PORT,
  BACKEND_EXPORT_ROOT,
  DEMO_EXPORT_PORT,
  DEMO_EXPORT_ROOT,
  DEMO_EXPORT_URL,
} from "./integration/visual/visual-servers";

/**
 * Visual-regression harness for the shipped CSS.
 *
 * The suite renders the exported demo UI from a dependency-free static server,
 * so it needs neither the Go server nor ClickHouse. It exists to pin
 * appearance while `app/globals.css` and the CSS modules are refactored.
 *
 * It used to run on Playwright's default per-pixel `threshold` of 0.2, which is
 * generous enough in YIQ space to call two visibly different blues equal: the
 * Phase 2 token sweep moved colour by up to 55 units on a channel across most
 * of every page and the suite stayed green, so "62 of 62 passed" was read as
 * evidence about colour when it was only evidence about layout. The threshold
 * below is tight enough that a hue move of that size fails.
 *
 * It also used to carry `maxDiffPixelRatio: 0.002`, described as absorbing
 * sub-pixel antialiasing. On a 1440x1583 page that is a budget of 4,560 pixels,
 * and Phase 3 spent it: seventeen baselines went stale for that phase's own
 * deliberate restyles -- a 2,504-pixel repaint of a whole status column among
 * them -- and the suite stayed green, so "84 of 84 passed" was again read as
 * evidence the baselines were current. The budget is 0 now. Nothing here is
 * sampled: the clock is fixed, animations are disabled, the device scale factor
 * is 1 and the baselines are recorded per platform, and
 * `npm run test:visual:determinism` already asserts two captures of one build
 * match exactly. A baseline that no longer matches is therefore a change to
 * record, not noise to absorb. No capture in the suite overrides these terms
 * either: the sparkline fixture used to, against the looser suite this
 * replaced, and its comment records why it no longer needs to.
 */

export default defineConfig({
  expect: {
    toHaveScreenshot: {
      animations: "disabled",
      caret: "hide",
      // Zero: a baseline either matches this build or is out of date.
      maxDiffPixelRatio: 0,
      scale: "css",
      // A tenth of Playwright's default, so a token substitution that moves a
      // hue has to be recorded in the baselines rather than absorbed here.
      threshold: 0.02,
    },
  },
  forbidOnly: process.env.CI !== undefined,
  fullyParallel: true,
  outputDir: "./test-results/visual",
  projects: [
    {
      name: "desktop",
      use: { ...devices["Desktop Chrome"], viewport: { height: 900, width: 1440 } },
    },
    {
      name: "mobile",
      use: { ...devices["Desktop Chrome"], viewport: { height: 1000, width: 760 } },
    },
  ],
  reporter: [["list"]],
  retries: 0,
  snapshotPathTemplate: "{testDir}/__screenshots__/{platform}/{projectName}/{arg}{ext}",
  testDir: "./integration/visual",
  testMatch: /.*\.visual\.spec\.ts$/u,
  timeout: 60_000,
  use: {
    baseURL: DEMO_EXPORT_URL,
    colorScheme: "light",
    deviceScaleFactor: 1,
    locale: "en-US",
    timezoneId: "UTC",
    trace: "off",
    video: "off",
  },
  // The build is part of the server command so a stale export can never be
  // screenshotted; the run therefore never reuses an already-listening server.
  webServer: {
    command: [
      "node scripts/build-visual-exports.mjs",
      "&&",
      "node scripts/serve-static.mjs",
      `--root ${DEMO_EXPORT_ROOT} --port ${DEMO_EXPORT_PORT}`,
      `--root ${BACKEND_EXPORT_ROOT} --port ${BACKEND_EXPORT_PORT}`,
    ].join(" "),
    reuseExistingServer: false,
    stderr: "pipe",
    stdout: "ignore",
    timeout: 300_000,
    url: DEMO_EXPORT_URL,
  },
  workers: process.env.CI === undefined ? undefined : 2,
});
