import type { Metadata } from "next";

import { ProductShell } from "./_components/product-shell";
import { LegacyHashRedirect } from "./_components/legacy-hash-redirect";
import { HomeDashboard } from "./home-dashboard";

export const metadata: Metadata = { title: { absolute: "Home | Open Splunk" } };

export default function HomePage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return (
    <>
    <LegacyHashRedirect />
    <ProductShell activeSection="home" appName="Launcher" dataMode={dataMode}>
      <HomeDashboard dataMode={dataMode} />
    </ProductShell>
    </>
  );
}
