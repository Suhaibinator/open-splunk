export type ViewNavigationMode = "push" | "replace";

function normalizedBasePath(basePath: string): string {
  const withLeadingSlash = basePath.startsWith("/") ? basePath : `/${basePath}`;
  return withLeadingSlash.endsWith("/") ? withLeadingSlash : `${withLeadingSlash}/`;
}

export function routedViewPath<View extends string>(basePath: string, view: View): string {
  return `${normalizedBasePath(basePath)}${view}/`;
}

export function routedViewHref<View extends string>(
  currentHref: string,
  basePath: string,
  view: View,
): string {
  const url = new URL(currentHref);
  url.pathname = routedViewPath(basePath, view);
  return `${url.pathname}${url.search}${url.hash}`;
}

export function resolveRoutedView<View extends string>(
  pathname: string,
  basePath: string,
  views: readonly View[],
): View | null {
  const normalizedBase = normalizedBasePath(basePath);
  const normalizedPath = pathname.endsWith("/") ? pathname : `${pathname}/`;
  if (normalizedPath === normalizedBase) return null;
  if (!normalizedPath.startsWith(normalizedBase)) return null;
  const candidate = normalizedPath.slice(normalizedBase.length, -1);
  return candidate.length > 0 && !candidate.includes("/") && views.includes(candidate as View)
    ? candidate as View
    : null;
}

export function commitRoutedView<View extends string>(
  navigationWindow: Window,
  basePath: string,
  view: View,
  mode: ViewNavigationMode,
  state: unknown = navigationWindow.history.state,
): void {
  const href = routedViewHref(navigationWindow.location.href, basePath, view);
  if (mode === "push") navigationWindow.history.pushState(state, "", href);
  else navigationWindow.history.replaceState(state, "", href);
}
