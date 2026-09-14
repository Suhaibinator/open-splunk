import { defineConfig, devices } from "@playwright/test";

// The instance-palette smoke is driven by `integration/palette_smoke_test.go`,
// which starts the real browser API handler and static export on a random
// loopback port and hands the origin and the administrator bearer over the
// environment. There is no web server here to start: the Go test owns it.
const baseURL = process.env.OPEN_SPLUNK_PALETTE_SMOKE_BASE_URL?.trim();
if (!baseURL) {
  throw new Error("OPEN_SPLUNK_PALETTE_SMOKE_BASE_URL is required; run the smoke through go test");
}
const browserExecutable = process.env.OPEN_SPLUNK_BROWSER_EXECUTABLE?.trim();

export default defineConfig({
  testDir: "./integration/palette-smoke",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  workers: 1,
  forbidOnly: process.env.CI === "true",
  reporter: "line",
  outputDir: "./test-results/palette-smoke",
  use: {
    baseURL,
    launchOptions: browserExecutable ? { executablePath: browserExecutable } : {},
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
