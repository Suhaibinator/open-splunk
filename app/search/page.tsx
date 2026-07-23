import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { SearchWorkspace } from "../search-workspace";

export const metadata: Metadata = { title: "Search & Reporting" };

export default function SearchPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return <SearchWorkspace dataMode={dataMode} apiBaseUrl={apiBaseUrl} />;
}
