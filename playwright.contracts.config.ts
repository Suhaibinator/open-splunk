import { defineConfig, devices } from "@playwright/test";

// Computed-style contracts render fixture markup against the application
// stylesheets inside a real browser, so they need no application server and no
// backend fixtures.
export default defineConfig({
  testDir: "./integration/style-contracts",
  testMatch: "css-contracts.spec.ts",
  fullyParallel: true,
  forbidOnly: process.env.CI === "true",
  reporter: "line",
  outputDir: "./test-results/css-contracts",
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
