import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../../../_components/theme-sync";
import { EmailVerificationRoute } from "../../../signin/auth-routes";

export const metadata: Metadata = { title: "Verify email" };

/** go-common links here from its registration email (`EmailVerificationPath`). */
export default function EmailVerificationPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <EmailVerificationRoute dataMode={dataMode} />
    </>
  );
}
