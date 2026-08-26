import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { OperationsDashboard } from "./operations-dashboard";

export const metadata: Metadata = { title: "GradeThis Operations" };

export default function DashboardsPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="dashboards" apiBaseUrl={apiBaseUrl} appName="GradeThis Operations" dataMode={dataMode}>
      <OperationsDashboard apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
    </ProductShell>
  );
}
