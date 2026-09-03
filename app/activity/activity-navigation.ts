import { resolveRoutedView, routedViewPath } from "@/lib/view-navigation";

export const ACTIVITY_BASE_PATH = "/activity/";
export const ACTIVITY_VIEWS = ["jobs", "history", "exports", "mutations", "attempts"] as const;
export type ActivityView = typeof ACTIVITY_VIEWS[number];

export function isActivityView(value: string): value is ActivityView {
  return ACTIVITY_VIEWS.includes(value as ActivityView);
}

export function activityViewPath(view: ActivityView): string {
  return routedViewPath(ACTIVITY_BASE_PATH, view);
}

export function activityViewFromPathname(pathname: string): ActivityView | null {
  return resolveRoutedView(pathname, ACTIVITY_BASE_PATH, ACTIVITY_VIEWS);
}
