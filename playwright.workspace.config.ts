import { defineConfig, devices } from "@playwright/test";

// Workspace behaviour tests drive the demo-mode static export in a real
// browser. The export is what the Go binary embeds, so `npm run build` must
// have produced `out/` first; the server below only reads it.
const port = 4173;
const baseURL = `http://127.0.0.1:${port}`;

export default defineConfig({
  testDir: "./integration/workspace-behaviour",
  testMatch: "*.spec.ts",
  fullyParallel: true,
  forbidOnly: process.env.CI === "true",
  reporter: "line",
  outputDir: "./test-results/workspace-behaviour",
  use: { baseURL },
  webServer: {
    command: "node scripts/serve-static-ui.mjs",
    env: { PORT: String(port) },
    reuseExistingServer: process.env.CI !== "true",
    url: `${baseURL}/search/`,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
