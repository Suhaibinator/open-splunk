export type CategoricalActivation = "drilldown" | "inspect";

/**
 * Mouse clicks and keyboard activation retain Splunk's direct drilldown.
 * Touch-like pointers need their first activation for a stable value inspector,
 * where an explicit drilldown action remains available.
 */
export function categoricalActivation(pointerType: string | null): CategoricalActivation {
  return pointerType === "touch" || pointerType === "pen" ? "inspect" : "drilldown";
}
