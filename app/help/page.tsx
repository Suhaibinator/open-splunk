import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { HelpBrowser } from "./help-browser";

export const metadata: Metadata = { title: "Documentation" };

export default function HelpPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return <HelpBrowser apiBaseUrl={apiBaseUrl} dataMode={dataMode} initialSlug="" />;
}
