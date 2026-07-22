import type { Metadata } from "next";

import { ProductShell } from "../_components/product-shell";
import { OperationsDashboard } from "./operations-dashboard";

export const metadata: Metadata = { title: "GradeThis Operations" };

export default function DashboardsPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return (
    <ProductShell activeSection="dashboards" appName="GradeThis Operations" dataMode={dataMode}>
      <OperationsDashboard />
    </ProductShell>
  );
}
