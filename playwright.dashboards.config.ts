import { defineConfig, devices } from "@playwright/test";

const port = 4175;
const baseURL = `http://127.0.0.1:${port}`;

export default defineConfig({
  testDir: "./integration/dashboard-behaviour",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  forbidOnly: process.env.CI === "true",
  reporter: "line",
  outputDir: "./test-results/dashboard-behaviour",
  use: { baseURL, screenshot: "only-on-failure", trace: "retain-on-failure" },
  webServer: {
    command: `npm run dev -- --port ${port}`,
    env: { OPEN_SPLUNK_DATA_MODE: "backend", OPEN_SPLUNK_NEXT_DIST_DIR: ".cache/dashboard-next" },
    reuseExistingServer: process.env.CI !== "true",
    url: `${baseURL}/dashboards/`,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
