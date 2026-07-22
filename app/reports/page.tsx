import type { Metadata } from "next";

import { ProductShell } from "../_components/product-shell";
import { ReportsConsole } from "./reports-console";

export const metadata: Metadata = { title: "Reports" };

export default function ReportsPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";

  return (
    <ProductShell activeSection="reports" appName="Search & Reporting" dataMode={dataMode}>
      <ReportsConsole />
    </ProductShell>
  );
}
