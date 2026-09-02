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
  await expect(menu).toHaveRole("listbox");
  await expect(menu.locator("[aria-selected=\"true\"]")).toHaveCount(1);
  await expect(page.getByTestId("search-input")).toHaveAttribute("aria-activedescendant", "spl-completion-0");

  await page.keyboard.press("ArrowDown");
  const highlighted = menu.locator("[aria-selected=\"true\"]");
  await expect(highlighted).toHaveId("spl-completion-1");
  await expect(highlighted).toHaveRole("option");
  await expect(page.getByTestId("search-input")).toHaveAttribute("aria-activedescendant", "spl-completion-1");
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

test("typing a field comparison offers the values the summary saw and Enter spells them as SPL", async ({ page }) => {
  await openSeededWorkspace(page);
  const editor = page.getByTestId("search-input");
  const menu = page.getByTestId("completion-menu");
  await editor.fill("");
  await editor.pressSequentially("index=");

  await expect(menu).toBeVisible();
  const index = menu.getByRole("group", { name: "Indexes" }).getByRole("option");
  await expect(index).toHaveCount(1);
  await expect(index).toHaveAttribute("data-kind", "index");
  await expect(index).toHaveAttribute("aria-selected", "true");
  await expect(index.locator("code")).toHaveText("gradethis");
  await page.keyboard.press("Enter");
  await expect(menu).toHaveCount(0);
  await expect(editor).toHaveValue("index=gradethis");

  await editor.pressSequentially(" level=E");
  const value = menu.getByRole("group", { name: "Values" }).getByRole("option");
  await expect(value).toHaveCount(1);
  await expect(value).toHaveAttribute("data-kind", "value");
  await expect(value.locator("code")).toHaveText("\"ERROR\"");
  await page.keyboard.press("Enter");
  await expect(menu).toHaveCount(0);
  await expect(editor).toHaveValue("index=gradethis level=\"ERROR\"");
  await expect(editor).toBeFocused();
});

test("an unsupported stage is underlined, dotted in the gutter, and listed with a jump to it", async ({ page }) => {
  await openSeededWorkspace(page);
  const editor = page.getByTestId("search-input");
  await editor.fill("index=main\n| transaction");

  const mark = page.locator(".editor-highlight mark.spl-diagnostic");
  await expect(mark).toHaveCount(1);
  await expect(mark).toHaveText("transaction");
  await expect(mark).toHaveAttribute("data-severity", "error");
  await expect(page.getByTestId("editor-gutter-marker-2")).toBeVisible();
  await expect(page.getByTestId("editor-gutter-marker-1")).toHaveCount(0);
  await expect(editor).toHaveAttribute("aria-invalid", "true");

  const problems = page.getByTestId("search-diagnostics");
  const row = problems.getByRole("listitem");
  await expect(row).toHaveCount(1);
  await expect(row).toHaveAttribute("data-severity", "error");
  await expect(row).toContainText("Line 2, column 3");
  await editor.blur();
  await row.locator(".diagnostic-problem").click();
  await expect(editor).toBeFocused();
  expect(await editor.evaluate((element) => (element as HTMLTextAreaElement).selectionStart)).toBe(13);

  await row.getByRole("button", { name: "Remove stage" }).click();
  await expect(editor).toHaveValue("index=main");
  await expect(problems).toHaveCount(0);
  await expect(mark).toHaveCount(0);
  await expect(editor).not.toHaveAttribute("aria-invalid", "true");
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

test("Back restores the previous draft without a new run, and Forward runs the launch again", async ({ page }) => {
  await openSeededWorkspace(page, { earliest: "-7d", latest: "now" });
  const editor = page.getByTestId("search-input");
  const runButton = page.getByTestId("run-search");
  const jobStrip = page.getByTestId("job-strip");
  const progress = page.getByLabel("Search progress");
  await expect(page).toHaveURL(/run=0/u);
  await expect(jobStrip).toHaveClass(/is-closed/u);

  await focusEditorEnd(page);
  await page.keyboard.type("\n| stats count");
  await page.keyboard.press("Control+Enter");
  await expect(runButton).toHaveAttribute("aria-label", "Cancel search");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");
  await expect(page).toHaveURL(/[?&]q=index%3Dmain%0A%7C\+stats\+count(?:&|$)/u);
  await expect(page).toHaveURL(/[?&]run=1(?:&|$)/u);
  await expect(jobStrip).not.toHaveClass(/is-closed/u);
  await expect(progress).toHaveJSProperty("value", 100);

  await page.goBack();
  await expect(page).toHaveURL(/[?&]q=index%3Dmain(?:&|$)/u);
  await expect(page).toHaveURL(/[?&]run=0(?:&|$)/u);
  await expect(editor).toHaveValue(SEEDED_QUERY);
  await expect(page.getByTestId("time-range-button")).toContainText("Last 7 days");
  await expect(jobStrip).toHaveClass(/is-closed/u);
  await expect(jobStrip).toHaveAttribute("aria-busy", "false");
  await expect(progress).toHaveJSProperty("value", 0);
  await expect(runButton).toHaveAttribute("aria-label", "Run search");

  // The demo has no retained job to reopen, so a run entry runs again in place.
  await page.goForward();
  await expect(editor).toHaveValue(`${SEEDED_QUERY}\n| stats count`);
  await expect(runButton).toHaveAttribute("aria-label", "Cancel search");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");
  await expect(jobStrip).not.toHaveClass(/is-closed/u);
  await expect(page).toHaveURL(/[?&]q=index%3Dmain%0A%7C\+stats\+count(?:&|$)/u);
  await expect(page).toHaveURL(/[?&]run=1(?:&|$)/u);
});

test("Escape with nothing open cancels the running search", async ({ page }) => {
  await openSeededWorkspace(page);
  const runButton = page.getByTestId("run-search");
  await expect(runButton).not.toHaveAttribute("aria-keyshortcuts", "Escape");

  await focusEditorEnd(page);
  await page.keyboard.press("Control+Enter");
  await expect(runButton).toHaveAttribute("aria-label", "Cancel search");
  await expect(runButton).toHaveAttribute("aria-keyshortcuts", "Escape");
  await expect(page.getByTestId("completion-menu")).toHaveCount(0);

  await page.keyboard.press("Escape");
  await expect(page.getByTestId("toast")).toContainText("Search canceled.");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");
  await expect(page.getByTestId("job-strip")).toContainText("Canceled");
  await expect(page.getByTestId("search-input")).toBeFocused();
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
