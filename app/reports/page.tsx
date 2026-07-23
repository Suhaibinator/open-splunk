import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { ReportsConsole } from "./reports-console";

export const metadata: Metadata = { title: "Reports" };

export default function ReportsPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();

  return (
    <ProductShell activeSection="reports" appName="Search & Reporting" dataMode={dataMode}>
      <ReportsConsole dataMode={dataMode} apiBaseUrl={apiBaseUrl} />
    </ProductShell>
  );
}
