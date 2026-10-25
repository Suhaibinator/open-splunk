import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../../_components/theme-sync";
import { OrganizationInvitationRoute } from "../../signin/auth-routes";

export const metadata: Metadata = { title: "Invitation" };

export default function OrganizationInvitationPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <OrganizationInvitationRoute dataMode={dataMode} />
    </>
  );
}
