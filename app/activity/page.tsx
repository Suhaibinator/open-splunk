import type { Metadata } from "next";

import { ProductShell } from "../_components/product-shell";
import { ActivityConsole } from "./activity-console";

export const metadata: Metadata = { title: "Activity" };

export default function ActivityPage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return (
    <ProductShell activeSection="activity" appName="Activity" dataMode={dataMode}>
      <ActivityConsole />
    </ProductShell>
  );
}
