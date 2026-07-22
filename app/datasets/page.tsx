import type { Metadata } from "next";

import { ProductShell } from "../_components/product-shell";
import { DatasetsConsole } from "./datasets-console";

export const metadata: Metadata = { title: "Datasets" };

export default function DatasetsPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return (
    <ProductShell activeSection="datasets" appName="Data Manager" dataMode={dataMode}>
      <DatasetsConsole />
    </ProductShell>
  );
}
