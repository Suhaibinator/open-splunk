import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { DatasetsConsole } from "./datasets-console";

export const metadata: Metadata = { title: "Datasets" };

export default function DatasetsPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="datasets" appName="Data Manager" dataMode={dataMode}>
      <DatasetsConsole dataMode={dataMode} apiBaseUrl={apiBaseUrl} />
    </ProductShell>
  );
}
