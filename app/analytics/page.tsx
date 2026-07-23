import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { AnalyticsConsole } from "./analytics-console";

export const metadata: Metadata = {
  title: "Analytics",
  description: "Investigate search performance, query cost, and field coverage.",
};

export default function AnalyticsPage() {
  const { dataMode } = getFrontendRuntimeConfig();

  return (
    <ProductShell activeSection="analytics" appName="Analytics" dataMode={dataMode}>
      <AnalyticsConsole dataMode={dataMode} />
    </ProductShell>
  );
}
