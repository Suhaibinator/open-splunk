/**
 * Mounting and driving a component on the fake browser, for a test that
 * walks a flow -- type, submit, click, wait for the client -- rather than
 * rendering one frame.
 *
 * Import this module before anything that loads `react-dom/client`: it loads
 * the fake browser first, which is the order react-dom needs to find a DOM.
 *
 * Test-only: no page or library module imports this file.
 */

import { browser } from "./fake-browser";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { fakeEvent, type FakeElement } from "./fake-dom";

export { browser };

export interface Mounted {
  readonly container: FakeElement;
  unmount(): Promise<void>;
}

export async function mount(node: ReactNode): Promise<Mounted> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => root.render(node));
  return {
    container,
    unmount: async () => {
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
}

/** Lets pending client promises, and the renders they cause, settle. */
export async function settle(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, 5);
    });
  });
}

/** The first element matching `selector` whose text contains `text`, or null. */
export function findByText(container: FakeElement, selector: string, text: string): FakeElement | null {
  return container.querySelectorAll(selector).find((element) => element.textContent.includes(text)) ?? null;
}

function required(element: FakeElement | null, what: string): FakeElement {
  if (element === null) throw new Error(`missing ${what}`);
  return element;
}

/** Clicks the button whose text contains `label`; throws when there is none. */
export async function clickButton(container: FakeElement, label: string): Promise<void> {
  const button = required(findByText(container, "button", label), `button "${label}"`);
  await act(async () => {
    button.dispatchEvent(fakeEvent("click"));
  });
  await settle();
}

/** Sets the input with `id` to `value` the way typing does. */
export async function typeInto(container: FakeElement, id: string, value: string): Promise<void> {
  const input = required(container.querySelector(`#${id}`), `input #${id}`);
  await act(async () => {
    input.value = value;
    input.dispatchEvent(fakeEvent("input"));
  });
}

/** Submits the form named `label` (its `aria-label`), as pressing Enter in it does. */
export async function submitForm(container: FakeElement, label: string): Promise<void> {
  const form = required(container.querySelector(`form[aria-label="${label}"]`), `form "${label}"`);
  await act(async () => {
    form.dispatchEvent(fakeEvent("submit"));
  });
  await settle();
}

/** `url` without the slash that ends its path: `/signin/?a=1` is `/signin?a=1`. */
export function withoutTrailingSlash(url: string): string {
  return url.replace(/(?<=.)\/(?=[?#]|$)/u, "");
}

/**
 * The `href` of the link whose text contains `text`, or null, without its
 * path's trailing slash: outside a Next build `next/link` drops it (the
 * export's `trailingSlash` lives in `next.config.ts`), so a test compares
 * the destination, not that spelling.
 */
export function linkHref(container: FakeElement, text: string): string | null {
  const href = findByText(container, "a", text)?.getAttribute("href") ?? null;
  return href === null ? null : withoutTrailingSlash(href);
}

/** The id of the element that has focus, or null. */
export function focusedId(): string | null {
  return browser.document.activeElement?.getAttribute("id") ?? null;
}
