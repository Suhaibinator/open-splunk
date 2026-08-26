import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "./_components/product-shell";
import { LegacyHashRedirect } from "./_components/legacy-hash-redirect";
import { HomeDashboard } from "./home-dashboard";

export const metadata: Metadata = { title: { absolute: "Home | Open Splunk" } };

export default function HomePage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      <LegacyHashRedirect />
      <ProductShell activeSection="home" apiBaseUrl={apiBaseUrl} appName="Launcher" dataMode={dataMode}>
        <HomeDashboard apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      </ProductShell>
    </>
  );
}
