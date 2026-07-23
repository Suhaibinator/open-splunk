import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { ActivityConsole } from "./activity-console";

export const metadata: Metadata = { title: "Activity" };

export default function ActivityPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="activity" appName="Activity" dataMode={dataMode}>
      <ActivityConsole dataMode={dataMode} apiBaseUrl={apiBaseUrl} />
    </ProductShell>
  );
}
