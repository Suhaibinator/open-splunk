"use client";

const SECRET_RECOVERY_HISTORY_MARKER = "openSplunkSecretRecovery";

function protectOneTimeSecretUnload(event: BeforeUnloadEvent) {
  event.preventDefault();
  event.returnValue = "";
}

/** Installs the browser-level guard used throughout one secret issuance. */
export function installOneTimeSecretNavigationProtection(
  navigationWindow: Window,
  navigationDocument: Document,
  onBlocked: () => void = () => {},
): () => void {
  const marker = `${Date.now()}-${Math.random()}`;
  const protectedHref = navigationWindow.location.href;
  const initialState = typeof navigationWindow.history.state === "object"
    && navigationWindow.history.state !== null
    ? navigationWindow.history.state as Record<string, unknown>
    : {};

  navigationWindow.history.pushState(
    { ...initialState, [SECRET_RECOVERY_HISTORY_MARKER]: marker },
    "",
    protectedHref,
  );

  const protectLinkNavigation = (event: MouseEvent) => {
    if (event.defaultPrevented) return;
    const target = event.target as { closest?: (selector: string) => HTMLAnchorElement | null } | null;
    const link = target?.closest?.("a[href]") ?? null;
    if (link === null || link.target === "_blank" || link.hasAttribute("download")) return;
    event.preventDefault();
    event.stopPropagation();
    onBlocked();
  };
  const protectHistoryNavigation = () => {
    navigationWindow.history.pushState(
      { ...initialState, [SECRET_RECOVERY_HISTORY_MARKER]: marker },
      "",
      protectedHref,
    );
    onBlocked();
  };

  navigationWindow.addEventListener("beforeunload", protectOneTimeSecretUnload);
  navigationWindow.addEventListener("popstate", protectHistoryNavigation);
  navigationDocument.addEventListener("click", protectLinkNavigation, true);
  return () => {
    navigationWindow.removeEventListener("beforeunload", protectOneTimeSecretUnload);
    navigationWindow.removeEventListener("popstate", protectHistoryNavigation);
    navigationDocument.removeEventListener("click", protectLinkNavigation, true);
    if (navigationWindow.history.state?.[SECRET_RECOVERY_HISTORY_MARKER] === marker) {
      navigationWindow.history.back();
    }
  };
}
