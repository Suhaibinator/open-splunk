// Structural and behavioural contracts for the consolidated chrome.
//
// Phase 3 replaced four hand-written product bars with one `ProductShell` and
// moved `Modal` next to the `modal-surface` helper it installs. Neither change
// is visible to a screenshot: a page that renders its own bar at the same
// height looks identical to one rendered by the shell, and a dialog whose focus
// trap stopped being installed photographs exactly like one whose trap works.
// The assertions here are therefore geometric and behavioural rather than
// per-pixel, and they take no screenshots, so they add no baselines.
//
// They live in the visual suite because they need the real exported build in a
// real browser -- the same two viewport projects every other spec runs under,
// which is how "at both viewports" is covered without repeating a single
// assertion.
import { expect, test, type Locator, type Page } from "@playwright/test";

import { gotoVisualRoute, settleVisualPage } from "./visual-harness";

/** The route every other route's chrome is measured against. */
const REFERENCE_ROUTE = "/";

/** Every route that renders through `ProductShell`. `/signin/` deliberately does not. */
const SHELL_ROUTES = [
  REFERENCE_ROUTE,
  "/search/",
  "/analytics/",
  "/datasets/",
  "/reports/",
  "/activity/",
  "/dashboards/",
  "/admin/",
];

/**
 * A route that paints menus into the product bar, and the selector the open one
 * renders as.
 *
 * The shell's own menus are `.suite-popover`; the search workspace passes its
 * `.floating-menu` markup into the same bar slots, so the two spellings share a
 * stacking context and have to be checked separately -- a fix that clears one
 * scrim does not necessarily clear the other.
 */
const BAR_MENU_ROUTES = [
  { popoverSelector: ".suite-popover", route: "/reports/" },
  { popoverSelector: ".suite-product-bar .floating-menu", route: "/search/" },
];

interface ChromeMeasurement {
  appBarHeight: number;
  mainTop: number;
  productBarHeight: number;
}

/**
 * Measures the chrome band the shell puts above a page's content.
 *
 * `mainTop` is the number that matters to a reader: the first row of pixels the
 * page itself owns. It is read from the document rather than added up from the
 * bars, so a route that grew an extra band between them would move it.
 */
async function measureChrome(page: Page): Promise<ChromeMeasurement> {
  return page.evaluate(() => {
    const main = document.querySelector("main");
    if (main === null) throw new Error("the page renders no <main> landmark");
    const productBar = document.querySelector(".suite-product-bar");
    const appBar = document.querySelector(".suite-app-bar");
    return {
      appBarHeight: appBar === null ? 0 : Math.round(appBar.getBoundingClientRect().height),
      mainTop: Math.round(main.getBoundingClientRect().top + window.scrollY),
      productBarHeight: productBar === null ? 0 : Math.round(productBar.getBoundingClientRect().height),
    };
  });
}

/** True when the page's skip link points at a `<main>` element on that same page. */
async function skipLinkResolves(page: Page): Promise<boolean> {
  return page.evaluate(() => {
    const skipLink = document.querySelector<HTMLAnchorElement>("a.skip-link");
    if (skipLink === null) return false;
    const target = document.getElementById(new URL(skipLink.href).hash.slice(1));
    return target !== null && target.tagName === "MAIN";
  });
}

/**
 * Names, for each item of an open menu, whatever a click at its centre would
 * actually reach.
 *
 * Hit-testing rather than clicking, because a click that lands on a scrim
 * *closes the menu*: the second assertion would then fail because the popover
 * is gone, and report a missing element instead of the covering one.
 */
async function menuItemHitTargets(popover: Locator): Promise<string[]> {
  return popover.evaluate((element: HTMLElement) => (
    [...element.querySelectorAll<HTMLElement>('[role="menuitem"]')].map((item) => {
      const box = item.getBoundingClientRect();
      const hit = document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2);
      if (hit !== null && item.contains(hit)) return "itself";
      const label = item.textContent?.trim().split("\n")[0] ?? "an item";
      const covering = hit === null
        ? "nothing (the point is outside the viewport)"
        : `${hit.tagName.toLowerCase()}${[...hit.classList].map((name) => `.${name}`).join("")}`;
      return `"${label}" is covered by ${covering}`;
    })
  ));
}

/** Opens the administration index dialog and returns the button that opened it. */
async function openIndexDialog(page: Page): Promise<Locator> {
  await gotoVisualRoute(page, "/admin/?section=indexes");
  const trigger = page.getByRole("button", { name: "Simulate index" });
  await trigger.click();
  await expect(page.getByTestId("modal-layer")).toBeVisible();
  await settleVisualPage(page);
  return trigger;
}

/** The label of whatever inside `dialog` holds focus, or null when focus escaped. */
async function focusedInsideDialog(dialog: Locator): Promise<string | null> {
  return dialog.evaluate((element: HTMLElement) => {
    const active = document.activeElement;
    if (active === null || !element.contains(active)) return null;
    return active.getAttribute("aria-label") ?? active.textContent?.trim() ?? active.tagName;
  });
}

/** Whether each of the two sticky chrome bars is currently inert. */
async function chromeInertness(page: Page): Promise<{ appBar: boolean | null; productBar: boolean | null }> {
  return page.evaluate(() => ({
    appBar: document.querySelector<HTMLElement>(".suite-app-bar")?.inert ?? null,
    productBar: document.querySelector<HTMLElement>(".suite-product-bar")?.inert ?? null,
  }));
}

test.describe("product chrome", () => {
  for (const route of SHELL_ROUTES) {
    test(`${route} renders exactly one ProductShell`, async ({ page }) => {
      await gotoVisualRoute(page, route);
      await expect(page.locator(".suite-shell"), "shell").toHaveCount(1);
      await expect(page.locator(".suite-product-bar"), "product bar").toHaveCount(1);
      await expect(page.locator(".suite-app-bar"), "app bar").toHaveCount(1);
      await expect(page.locator("main"), "main landmark").toHaveCount(1);
      await expect(page.locator("a.skip-link"), "skip link").toHaveCount(1);
      expect(await skipLinkResolves(page), "the skip link resolves to this page's own <main>").toBe(true);
    });
  }

  for (const route of SHELL_ROUTES.filter((candidate) => candidate !== REFERENCE_ROUTE)) {
    test(`${route} sits under the same chrome as ${REFERENCE_ROUTE}`, async ({ page }) => {
      // One shell means one chrome height, at whichever viewport the project
      // runs at. A route that measures differently is either rendering a bar of
      // its own again or overriding the shell's -- the state this phase
      // removed, and one no screenshot of a single page can see.
      await gotoVisualRoute(page, REFERENCE_ROUTE);
      const reference = await measureChrome(page);
      expect(reference.productBarHeight, "the product bar has no height at all").toBeGreaterThan(0);
      await gotoVisualRoute(page, route);
      expect(await measureChrome(page)).toEqual(reference);
    });
  }

  for (const { popoverSelector, route } of BAR_MENU_ROUTES) {
    test(`every item of an open bar menu can be clicked on ${route}`, async ({ page }) => {
      // The product bar is `position: sticky` with a z-index, so it is a stacking
      // context and the popover inside it cannot rise above the bar's own layer,
      // however high the popover's own z-index is. Anything painted later at a
      // higher layer -- the dismiss scrim, the app bar -- then sits on top of an
      // open menu, and every menu item stops responding to the pointer while
      // still looking, and reading, exactly as before.
      await gotoVisualRoute(page, route);
      const trigger = page.locator(".suite-product-bar .suite-menu-anchor > button").filter({ visible: true }).first();
      await trigger.click();
      const popover = page.locator(popoverSelector);
      await expect(popover).toBeVisible();
      await settleVisualPage(page);
      const targets = await menuItemHitTargets(popover);
      expect(targets.length, "the open menu has no items to click").toBeGreaterThan(0);
      expect(
        targets.filter((target) => target !== "itself"),
        "a pointer must reach every item of an open menu",
      ).toEqual([]);
    });
  }

  test("/search/ can change app at every viewport", async ({ page }) => {
    // The shell folds its OWN app switcher away below 760px because the drawer
    // lists the same apps underneath it. A page that supplies a switcher owns
    // the catalog, so it gets the drawer's single-app branch instead -- folding
    // its switcher away too would leave the one page that switches apps in
    // place with no way to switch at all. That is a control, not a pixel, and
    // no screenshot of a bar can see it missing.
    await gotoVisualRoute(page, "/search/");
    const switcher = page.locator(".suite-product-bar .suite-app-switcher");
    await expect(switcher).toBeVisible();
    await switcher.click();
    const popover = page.locator(".suite-product-bar .floating-menu");
    await expect(popover).toBeVisible();
    expect(
      await popover.getByRole("menuitem").count(),
      "the app switcher offers nothing to switch to",
    ).toBeGreaterThan(1);
  });
});

test.describe("modal surface", () => {
  test("opening a dialog moves focus into it and makes the chrome behind it inert", async ({ page }) => {
    await openIndexDialog(page);
    const dialog = page.locator("dialog.modal-card");
    await expect(dialog).toBeVisible();
    expect(
      await dialog.evaluate((element: HTMLElement) => element.contains(document.activeElement)),
      "focus stayed outside the dialog, so the trap was never installed",
    ).toBe(true);
    // `installModalSurface` walks up from the dialog marking every *sibling*
    // inert, so the bars go inert while `<main>`, an ancestor of the dialog,
    // does not. Asserting the bars is asserting the walk actually ran.
    expect(
      await chromeInertness(page),
      "the chrome behind the dialog is still reachable by pointer and by screen reader",
    ).toEqual({ appBar: true, productBar: true });
    expect(
      await page.evaluate(() => document.body.style.overflow),
      "the page behind the dialog still scrolls",
    ).toBe("hidden");
  });

  test("Tab cycles inside a dialog instead of leaving it", async ({ page }) => {
    await openIndexDialog(page);
    const dialog = page.locator("dialog.modal-card");
    const controlCount = await dialog.evaluate((element: HTMLElement) => (
      element.querySelectorAll("button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), a[href]").length
    ));
    expect(controlCount, "the dialog has no focusable controls to trap").toBeGreaterThan(1);
    // One full cycle plus one step, taken one at a time against the same page:
    // a trap that leaks loses focus somewhere in the lap, and a trap that pins
    // focus to one control never moves at all. The steps chain through a
    // promise rather than a loop because they share one keyboard and must stay
    // in order.
    const visited = await Array.from({ length: controlCount + 1 })
      .reduce<Promise<Array<string | null>>>(async (previous) => {
        const seen = await previous;
        await page.keyboard.press("Tab");
        return [...seen, await focusedInsideDialog(dialog)];
      }, Promise.resolve([]));
    expect(
      visited.flatMap((label, index) => label === null ? [`Tab step ${index + 1} left the dialog`] : []),
      "Tab must never take focus out of an open dialog",
    ).toEqual([]);
    expect(new Set(visited).size, "Tab never moved focus off the first control").toBeGreaterThan(1);
  });

  test("Shift+Tab wraps backwards inside a dialog", async ({ page }) => {
    await openIndexDialog(page);
    const dialog = page.locator("dialog.modal-card");
    // Three steps back from the control the dialog opened on: the first two
    // walk backwards through the dialog and at least one of them wraps past its
    // first control, which is where an uninstalled trap lets focus out.
    await page.keyboard.press("Shift+Tab");
    const first = await focusedInsideDialog(dialog);
    await page.keyboard.press("Shift+Tab");
    const second = await focusedInsideDialog(dialog);
    await page.keyboard.press("Shift+Tab");
    const third = await focusedInsideDialog(dialog);
    expect(
      [first, second, third]
        .flatMap((label, index) => label === null ? [`Shift+Tab step ${index + 1} left the dialog`] : []),
      "Shift+Tab must wrap inside the dialog rather than escape it",
    ).toEqual([]);
  });

  test("Escape closes a dialog and gives focus back to what opened it", async ({ page }) => {
    const trigger = await openIndexDialog(page);
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("modal-layer")).toHaveCount(0);
    await expect(trigger).toBeFocused();
    expect(
      await chromeInertness(page),
      "the chrome behind the dialog stayed inert after it closed",
    ).toEqual({ appBar: false, productBar: false });
    expect(
      await page.evaluate(() => document.body.style.overflow),
      "the scroll lock outlived the dialog",
    ).not.toBe("hidden");
  });
});
