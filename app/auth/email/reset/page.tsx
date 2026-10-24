import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../../../_components/theme-sync";
import { PasswordResetCompleteRoute } from "../../../signin/auth-routes";

export const metadata: Metadata = { title: "Choose a new password" };

/** go-common links here from its password-reset email (`PasswordResetPath`). */
export default function PasswordResetCompletePage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <PasswordResetCompleteRoute dataMode={dataMode} />
    </>
  );
}
