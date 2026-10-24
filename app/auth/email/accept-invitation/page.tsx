import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../../../_components/theme-sync";
import { AccountInvitationRoute } from "../../../signin/auth-routes";

export const metadata: Metadata = { title: "Set up your account" };

/** go-common links here from an administrator-created account's invitation (`AccountInvitationPath`). */
export default function AccountInvitationPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <AccountInvitationRoute dataMode={dataMode} />
    </>
  );
}
