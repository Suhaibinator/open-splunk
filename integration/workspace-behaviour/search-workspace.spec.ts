// Behaviour of the search workspace in the demo-mode static export.
//
// The editor reducers in `lib/search/spl-editor-interaction.ts` are unit
// tested; these tests check that the workspace wires them, the time picker
// and the result tabs to real keyboard and pointer input in the export the
// Go binary embeds. They run against `scripts/serve-static-ui.mjs` and touch
// no backend. Like `integration/style-contracts/css-contracts.spec.ts` this
// is deliberately a `.spec.ts`: `scripts/test-frontend.mjs` runs `.test.ts`
// files under node, and Playwright tests cannot run there.
import { expect, test, type Page } from "@playwright/test";

const SEEDED_QUERY = "index=main";

function launchUrl(parameters: Record<string, string>): string {
  return `/search/?${new URLSearchParams({ q: SEEDED_QUERY, run: "0", ...parameters }).toString()}`;
}

async function openSeededWorkspace(page: Page, parameters: Record<string, string> = {}): Promise<void> {
  await page.goto(launchUrl(parameters));
  await expect(page.getByTestId("search-input")).toHaveValue(SEEDED_QUERY);
}

async function focusEditorEnd(page: Page): Promise<void> {
  const editor = page.getByTestId("search-input");
  await editor.click();
  await editor.press("End");
}

// The demo runner walks its phases on timers and completes in under two
// seconds; the run button's label is the user-visible signal for both edges.
async function runFromEditor(page: Page): Promise<void> {
  const runButton = page.getByTestId("run-search");
  await focusEditorEnd(page);
  await page.keyboard.press("Control+Enter");
  await expect(runButton).toHaveAttribute("aria-label", "Cancel search");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");
}

test("Ctrl+Space opens the command menu and Enter inserts the highlighted command as a new stage", async ({ page }) => {
  await openSeededWorkspace(page);
  await focusEditorEnd(page);
  await expect(page.getByTestId("completion-menu")).toHaveCount(0);

  await page.keyboard.press("Control+Space");
  const menu = page.getByTestId("completion-menu");
  await expect(menu).toBeVisible();
  await expect(menu.locator("[data-highlighted=true]")).toHaveCount(1);

  await page.keyboard.press("ArrowDown");
  const highlighted = menu.locator("[data-highlighted=true]");
  await expect(highlighted).toHaveId("spl-completion-1");
  const command = await highlighted.locator("code").innerText();

  await page.keyboard.press("Enter");
  await expect(menu).toHaveCount(0);
  const editor = page.getByTestId("search-input");
  await expect(editor).toHaveValue(new RegExp(`^${SEEDED_QUERY}\\n\\| ${command}\\b`, "u"));
  await expect(editor).toBeFocused();
});

test("Ctrl+Enter runs the seeded search and the job strip reports the seeded time range", async ({ page }) => {
  await openSeededWorkspace(page, { earliest: "-7d", latest: "now" });
  const runButton = page.getByTestId("run-search");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");
  await expect(page.getByTestId("job-strip")).toHaveAttribute("aria-busy", "false");

  await focusEditorEnd(page);
  await page.keyboard.press("Control+Enter");

  await expect(page.getByTestId("job-strip")).toHaveAttribute("aria-busy", "true");
  await expect(runButton).toHaveAttribute("aria-label", "Cancel search");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");
  await expect(page.getByTestId("job-strip")).toHaveAttribute("aria-busy", "false");
  await expect(page.getByTestId("result-tab-events")).toBeVisible();
  await expect(page.getByTestId("job-time-range")).toHaveText("Last 7 days");
  await expect(runButton).toBeEnabled();
});

test("Escape closes the command menu and typing a pipe reopens it", async ({ page }) => {
  await openSeededWorkspace(page);
  await focusEditorEnd(page);

  await page.keyboard.press("Control+Space");
  await expect(page.getByTestId("completion-menu")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByTestId("completion-menu")).toHaveCount(0);

  await page.keyboard.type("\n| ");
  await expect(page.getByTestId("search-input")).toHaveValue(`${SEEDED_QUERY}\n| `);
  await expect(page.getByTestId("completion-menu")).toBeVisible();
});

test("ArrowUp on the first line recalls the previous search and ArrowDown walks back to the draft", async ({ page }) => {
  await openSeededWorkspace(page);
  const editor = page.getByTestId("search-input");
  const status = page.locator("#spl-completion-status");
  await expect(page.locator("#editor-help")).toContainText("↑↓ history");
  await focusEditorEnd(page);

  await page.keyboard.press("ArrowUp");
  await expect(editor).toHaveValue("index=gradethis (level=ERROR OR status>=500) | sort -_time");
  await expect(status).toHaveText(/^Recalled search 1 of \d+$/u);
  await expect(page.getByTestId("completion-menu")).toHaveCount(0);

  await page.keyboard.press("ArrowUp");
  await expect(editor).toHaveValue('index=gradethis trace_id="4b9f0f06d2cc47c89bd04ce9a7318fd1" | sort _time');
  await expect(status).toHaveText(/^Recalled search 2 of \d+$/u);

  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("ArrowDown");
  await expect(editor).toHaveValue(SEEDED_QUERY);
  await expect(status).toHaveText("Restored draft");
  await expect(editor).toBeFocused();

  // Typing ends the walk: the next ArrowUp starts again from the edited draft.
  await page.keyboard.type(" level=ERROR");
  await page.keyboard.press("ArrowUp");
  await expect(editor).toHaveValue("index=gradethis (level=ERROR OR status>=500) | sort -_time");
  await page.keyboard.press("ArrowDown");
  await expect(editor).toHaveValue(`${SEEDED_QUERY} level=ERROR`);
});

test("choosing a preset in the time picker updates the range the next run submits", async ({ page }) => {
  await openSeededWorkspace(page);
  const rangeButton = page.getByTestId("time-range-button");
  await expect(rangeButton).toContainText("Last 24 hours");

  await rangeButton.click();
  const dialog = page.getByTestId("time-picker-dialog");
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Last 4 hours", exact: true }).click();
  await dialog.getByRole("button", { name: "Apply" }).click();

  await expect(dialog).toHaveCount(0);
  await expect(rangeButton).toContainText("Last 4 hours");

  await runFromEditor(page);
  await expect(page.getByTestId("job-time-range")).toHaveText("Last 4 hours");
});

test("arrow keys move the selected result tab and focus follows it", async ({ page }) => {
  await openSeededWorkspace(page);
  const events = page.getByTestId("result-tab-events");
  const patterns = page.getByTestId("result-tab-patterns");
  // The result views only exist once a search has been submitted.
  await expect(events).toBeHidden();
  await runFromEditor(page);
  await expect(events).toBeVisible();
  await expect(events).toHaveAttribute("aria-selected", "true");
  await expect(patterns).toHaveAttribute("aria-selected", "false");

  await events.focus();
  await page.keyboard.press("ArrowRight");
  await expect(patterns).toHaveAttribute("aria-selected", "true");
  await expect(events).toHaveAttribute("aria-selected", "false");
  await expect(patterns).toBeFocused();

  await page.keyboard.press("ArrowLeft");
  await expect(events).toHaveAttribute("aria-selected", "true");
  await expect(patterns).toHaveAttribute("aria-selected", "false");
  await expect(events).toBeFocused();

  await page.keyboard.press("End");
  await expect(page.getByTestId("result-tab-visualization")).toHaveAttribute("aria-selected", "true");
  await page.keyboard.press("Home");
  await expect(events).toHaveAttribute("aria-selected", "true");
});
