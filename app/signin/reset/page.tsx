import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../../_components/theme-sync";
import { PasswordResetRequestRoute } from "../auth-routes";

export const metadata: Metadata = { title: "Reset password" };

export default function PasswordResetRequestPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      {/* No product shell here either, so this page asks for the instance palette itself. */}
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <PasswordResetRequestRoute dataMode={dataMode} />
    </>
  );
}
