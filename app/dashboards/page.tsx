import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { OperationsDashboard } from "./operations-dashboard";

export const metadata: Metadata = { title: "Dashboards" };

export default function DashboardsPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <OperationsDashboard apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
  );
}
