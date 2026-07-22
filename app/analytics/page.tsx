import type { Metadata } from "next";

import { ProductShell } from "../_components/product-shell";
import { AnalyticsConsole } from "./analytics-console";

export const metadata: Metadata = {
  title: "Analytics",
  description: "Investigate search performance, query cost, and field coverage.",
};

export default function AnalyticsPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";

  return (
    <ProductShell activeSection="analytics" appName="Analytics" dataMode={dataMode}>
      <AnalyticsConsole />
    </ProductShell>
  );
}
