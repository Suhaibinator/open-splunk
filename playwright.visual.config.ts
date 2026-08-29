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
 * appearance while `app/globals.css` and the CSS modules are refactored: a
 * hue normalisation of a few RGB units stays inside the tolerance, while a
 * layout shift fails.
 */

export default defineConfig({
  expect: {
    toHaveScreenshot: {
      animations: "disabled",
      caret: "hide",
      // Small but nonzero: absorbs sub-pixel antialiasing, not layout movement.
      maxDiffPixelRatio: 0.002,
      scale: "css",
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
