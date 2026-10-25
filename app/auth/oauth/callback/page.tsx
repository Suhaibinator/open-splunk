import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../../../_components/theme-sync";
import { OAuthCallbackRoute } from "../../../signin/auth-routes";

export const metadata: Metadata = { title: "Finishing sign-in" };

/** The redirect URI each OAuth provider is registered with. */
export default function OAuthCallbackPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <OAuthCallbackRoute dataMode={dataMode} />
    </>
  );
}
