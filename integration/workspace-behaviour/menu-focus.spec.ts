import { expect, test } from "@playwright/test";

test("closing the user menu does not override a newer deliberate focus", async ({ page }) => {
  await page.goto("/search/events/?q=index%3Dmain&run=0");
  const editor = page.getByTestId("search-input");
  await editor.fill("index=main | timechart count by service");
  await editor.click();
  await page.keyboard.press("Control+Enter");
  await expect(page.getByTestId("run-search")).toHaveAttribute("aria-label", "Cancel search");
  await expect(page.getByTestId("run-search")).toHaveAttribute("aria-label", "Run search");

  const userMenu = page.locator(".suite-user-button");
  await userMenu.click();
  await page.getByRole("menuitemradio", { name: "Dark", exact: true }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");

  // Deliver close-time animation frames only after the next intentional focus.
  // This exposes the event ordering deterministically without a timing delay.
  await page.evaluate(() => {
    const request = window.requestAnimationFrame.bind(window);
    const cancel = window.cancelAnimationFrame.bind(window);
    const pending = new Map<number, FrameRequestCallback>();
    let next = -1;
    window.requestAnimationFrame = (callback) => {
      const id = next--;
      pending.set(id, callback);
      return id;
    };
    window.cancelAnimationFrame = (id) => {
      if (!pending.delete(id)) cancel(id);
    };
    Object.assign(window, {
      flushMenuFocusFrames() {
        window.requestAnimationFrame = request;
        window.cancelAnimationFrame = cancel;
        for (const callback of pending.values()) callback(performance.now());
        pending.clear();
      },
    });
  });

  await page.keyboard.press("Escape");
  await expect(userMenu).toHaveAttribute("aria-expanded", "false");
  const stacking = page.getByLabel("Stacking");
  await stacking.focus();
  await page.evaluate(() => {
    (window as typeof window & { flushMenuFocusFrames(): void }).flushMenuFocusFrames();
  });
  await expect(stacking).toBeFocused();
  await page.keyboard.press("End");
  await expect(stacking).toHaveAttribute("aria-expanded", "true");
  const listbox = page.locator(`[id="${await stacking.getAttribute("aria-controls")}"]`);
  await expect(listbox.getByRole("option", { name: "100%", exact: true }))
    .toHaveAttribute("data-active", "true");
});
