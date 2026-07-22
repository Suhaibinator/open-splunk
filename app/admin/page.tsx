import type { Metadata } from "next";

import { ProductShell } from "../_components/product-shell";
import { AdminConsole } from "./admin-console";

export const metadata: Metadata = { title: "Administration" };

export default function AdminPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return (
    <ProductShell activeSection="admin" appName="Settings" dataMode={dataMode}>
      <AdminConsole />
    </ProductShell>
  );
}
