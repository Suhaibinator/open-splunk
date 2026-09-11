// Series labels in the timechart are selectable and copyable in the same
// static export that the Go binary embeds. These checks intentionally use
// real pointer and keyboard input because the pinned inspector exists to keep
// an exact bucket stable while a user selects or copies its text.
import { expect, test, type Locator, type Page } from "@playwright/test";

const SPLIT_TIMECHART_QUERY = "index=main | timechart count by service";

async function openSplitTimechart(page: Page): Promise<Locator> {
  await page.goto("/search/events/?q=index%3Dmain&run=0");
  const editor = page.getByTestId("search-input");
  await editor.fill(SPLIT_TIMECHART_QUERY);
  await editor.press("Control+Enter");

  const runButton = page.getByTestId("run-search");
  await expect(runButton).toHaveAttribute("aria-label", "Cancel search");
  await expect(runButton).toHaveAttribute("aria-label", "Run search");

  const inspector = page.locator(".time-series-chart__inspect");
  await expect(inspector).toBeVisible();
  return inspector;
}

async function prepareClipboard(page: Page): Promise<void> {
  const origin = new URL(page.url()).origin;
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"], { origin });
  await page.evaluate(() => navigator.clipboard.writeText("clipboard sentinel"));
}

async function clipboardText(page: Page): Promise<string> {
  return page.evaluate(() => navigator.clipboard.readText());
}

async function tabTo(page: Page, control: Locator): Promise<void> {
  await page.keyboard.press("Tab");
  if (await control.evaluate((element) => element === document.activeElement)) return;
  await page.keyboard.press("Tab");
  if (await control.evaluate((element) => element === document.activeElement)) return;
  await page.keyboard.press("Tab");
  if (await control.evaluate((element) => element === document.activeElement)) return;
  await page.keyboard.press("Tab");
  await expect(control).toBeFocused();
}

test("mouse selection keeps a pinned bucket stable and series copy uses the visible label", async ({ page }) => {
  const inspector = await openSplitTimechart(page);
  await prepareClipboard(page);

  const legend = page.locator(".chart-legend");
  const legendCopy = legend.getByRole("button", { name: "Copy series label worker", exact: true });
  await expect(legendCopy).toBeVisible();
  await legendCopy.click();
  await expect.poll(() => clipboardText(page)).toBe("worker");

  const inspectorBounds = await inspector.boundingBox();
  expect(inspectorBounds).not.toBeNull();
  await page.mouse.move(
    inspectorBounds!.x + inspectorBounds!.width * 0.28,
    inspectorBounds!.y + inspectorBounds!.height * 0.5,
  );
  await expect(page.getByRole("tooltip")).toBeVisible();
  await expect(page.getByRole("group", { name: /^Pinned chart values for /u })).toHaveCount(0);

  await page.mouse.click(
    inspectorBounds!.x + inspectorBounds!.width * 0.28,
    inspectorBounds!.y + inspectorBounds!.height * 0.5,
  );
  const pinned = page.getByRole("group", { name: /^Pinned chart values for /u });
  await expect(pinned).toBeVisible();
  const pinnedBucket = await pinned.getAttribute("aria-label");
  expect(pinnedBucket).not.toBeNull();

  const label = pinned.locator(".time-series-chart__pinned-label").filter({ hasText: /^worker$/u });
  await expect(label).toHaveText("worker");
  const labelBounds = await label.boundingBox();
  expect(labelBounds).not.toBeNull();
  await page.mouse.move(labelBounds!.x + 0.5, labelBounds!.y + labelBounds!.height / 2);
  await page.mouse.down();
  await page.mouse.move(
    labelBounds!.x + labelBounds!.width - 0.5,
    labelBounds!.y + labelBounds!.height / 2,
    { steps: 8 },
  );
  await page.mouse.up();

  await expect.poll(() => page.evaluate(() => window.getSelection()?.toString() ?? ""))
    .toBe("worker");
  await expect(pinned).toHaveAttribute("aria-label", pinnedBucket!);

  await page.evaluate(() => navigator.clipboard.writeText("clipboard sentinel"));
  await pinned.getByRole("button", { name: "Copy series label worker", exact: true }).click();
  await expect.poll(() => clipboardText(page)).toBe("worker");
  await expect(pinned).toHaveAttribute("aria-label", pinnedBucket!);

  await page.getByLabel("Title").click();
  await expect(pinned).toHaveCount(0);
});

test("keyboard inspection pins, copies, and dismisses without losing the chart", async ({ page }) => {
  const inspector = await openSplitTimechart(page);
  await prepareClipboard(page);

  await inspector.focus();
  await expect(inspector).toBeFocused();
  const firstBucket = await inspector.getAttribute("aria-label");
  expect(firstBucket).not.toBeNull();

  await page.keyboard.press("ArrowRight");
  await expect.poll(() => inspector.getAttribute("aria-label")).not.toBe(firstBucket);
  await expect(page.getByRole("tooltip")).toBeVisible();
  await expect(page.getByRole("group", { name: /^Pinned chart values for /u })).toHaveCount(0);

  await page.keyboard.press("Enter");
  let pinned = page.getByRole("group", { name: /^Pinned chart values for /u });
  await expect(pinned).toBeVisible();
  const copyApi = pinned.getByRole("button", { name: "Copy series label api", exact: true });
  await tabTo(page, copyApi);
  await expect(copyApi).toBeFocused();
  await page.keyboard.press("Enter");
  await expect.poll(() => clipboardText(page)).toBe("api");

  await page.keyboard.press("Escape");
  await expect(pinned).toHaveCount(0);
  await expect(inspector).toBeFocused();

  await page.keyboard.press("Space");
  pinned = page.getByRole("group", { name: /^Pinned chart values for /u });
  await expect(pinned).toBeVisible();
  await pinned.getByRole("button", { name: "Close pinned chart values", exact: true }).click();
  await expect(pinned).toHaveCount(0);
  await expect(inspector).toBeFocused();
});
