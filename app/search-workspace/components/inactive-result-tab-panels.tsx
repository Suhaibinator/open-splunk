import type { ResultTab } from "../model";

const RESULT_TABS: readonly ResultTab[] = [
  "events",
  "patterns",
  "statistics",
  "visualization",
];

/** Keeps every tab's aria-controls target present while another view is active. */
export function InactiveResultTabPanels({ activeTab }: { activeTab: ResultTab }) {
  return RESULT_TABS.filter((tab) => tab !== activeTab).map((tab) => (
    <section
      aria-labelledby={`tab-${tab}`}
      hidden
      id={`panel-${tab}`}
      key={tab}
      role="tabpanel"
    />
  ));
}
