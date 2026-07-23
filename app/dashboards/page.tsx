import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { OperationsDashboard } from "./operations-dashboard";

export const metadata: Metadata = { title: "GradeThis Operations" };

export default function DashboardsPage() {
  const { dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="dashboards" appName="GradeThis Operations" dataMode={dataMode}>
      <OperationsDashboard dataMode={dataMode} />
    </ProductShell>
  );
}
