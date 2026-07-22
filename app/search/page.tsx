import type { Metadata } from "next";

import { SearchWorkspace } from "../search-workspace";

export const metadata: Metadata = { title: "Search & Reporting" };

export default function SearchPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return <SearchWorkspace dataMode={dataMode} apiBaseUrl={process.env.OPEN_SPLUNK_API_BASE_URL ?? ""} />;
}
