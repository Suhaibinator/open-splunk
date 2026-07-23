import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { AdminConsole } from "./admin-console";

export const metadata: Metadata = { title: "Administration" };

export default function AdminPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="admin" appName="Settings" dataMode={dataMode}>
      <AdminConsole dataMode={dataMode} apiBaseUrl={apiBaseUrl} />
    </ProductShell>
  );
}
