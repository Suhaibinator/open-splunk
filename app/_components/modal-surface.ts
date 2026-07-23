"use client";

const DEFAULT_FOCUSABLE_SELECTOR = [
  "button:not(:disabled)",
  "input:not(:disabled)",
  "select:not(:disabled)",
  "textarea:not(:disabled)",
  "a[href]",
  '[tabindex]:not([tabindex="-1"])',
].join(", ");

let bodyLockCount = 0;
let bodyOverflowBeforeLock = "";
const inertReferenceCounts = new WeakMap<HTMLElement, number>();

export interface ModalSurfaceOptions {
  container: HTMLElement;
  excludedSiblingClassNames?: readonly string[];
  focusableSelector?: string;
  inertSiblings?: boolean;
  initialFocusSelector?: string;
  onEscape?: () => void;
  returnFocus?: HTMLElement | null;
}

function lockBodyScroll(): () => void {
  if (bodyLockCount === 0) {
    bodyOverflowBeforeLock = document.body.style.overflow;
    document.body.style.overflow = "hidden";
  }
  bodyLockCount += 1;
  return () => {
    bodyLockCount = Math.max(0, bodyLockCount - 1);
    if (bodyLockCount === 0) document.body.style.overflow = bodyOverflowBeforeLock;
  };
}

function inertOutsideSurface(
  container: HTMLElement,
  excludedSiblingClassNames: readonly string[],
): () => void {
  const managedElements: HTMLElement[] = [];
  let current: HTMLElement | null = container;
  while (current !== null && current.parentElement !== null && current !== document.body) {
    const parent: HTMLElement = current.parentElement;
    for (const sibling of parent.children) {
      if (
        sibling === current
        || !(sibling instanceof HTMLElement)
        || excludedSiblingClassNames.some((className) => sibling.classList.contains(className))
      ) continue;
      const referenceCount = inertReferenceCounts.get(sibling);
      if (referenceCount === undefined && sibling.inert) continue;
      if (referenceCount === undefined) sibling.inert = true;
      inertReferenceCounts.set(sibling, (referenceCount ?? 0) + 1);
      managedElements.push(sibling);
    }
    current = parent;
  }
  return () => {
    for (const element of managedElements) {
      const referenceCount = inertReferenceCounts.get(element);
      if (referenceCount === undefined || referenceCount <= 1) {
        inertReferenceCounts.delete(element);
        element.inert = false;
      } else {
        inertReferenceCounts.set(element, referenceCount - 1);
      }
    }
  };
}

function focusableControls(container: HTMLElement, selector: string): HTMLElement[] {
  return Array.from(container.querySelectorAll<HTMLElement>(selector))
    .filter((element) => !element.hasAttribute("hidden") && element.getClientRects().length > 0);
}

export function installModalSurface({
  container,
  excludedSiblingClassNames = [],
  focusableSelector = DEFAULT_FOCUSABLE_SELECTOR,
  inertSiblings = true,
  initialFocusSelector,
  onEscape,
  returnFocus = null,
}: ModalSurfaceOptions): () => void {
  const unlockBodyScroll = lockBodyScroll();
  const restoreOutsideSurface = inertSiblings
    ? inertOutsideSurface(container, excludedSiblingClassNames)
    : () => undefined;
  const focusFrame = window.requestAnimationFrame(() => {
    const requestedControl = initialFocusSelector === undefined
      ? null
      : container.querySelector<HTMLElement>(initialFocusSelector);
    (requestedControl ?? focusableControls(container, focusableSelector)[0] ?? container).focus();
  });

  function handleKeyDown(event: KeyboardEvent) {
    if (event.key === "Escape" && onEscape !== undefined) {
      event.preventDefault();
      onEscape();
      return;
    }
    if (event.key !== "Tab") return;
    const controls = focusableControls(container, focusableSelector);
    if (controls.length === 0) {
      event.preventDefault();
      container.focus();
      return;
    }
    const first = controls[0];
    const last = controls.at(-1) ?? first;
    if (event.shiftKey && (document.activeElement === first || !container.contains(document.activeElement))) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (document.activeElement === last || !container.contains(document.activeElement))) {
      event.preventDefault();
      first.focus();
    }
  }

  document.addEventListener("keydown", handleKeyDown);
  return () => {
    window.cancelAnimationFrame(focusFrame);
    document.removeEventListener("keydown", handleKeyDown);
    restoreOutsideSurface();
    unlockBodyScroll();
    if (returnFocus?.isConnected) window.requestAnimationFrame(() => returnFocus.focus());
  };
}
