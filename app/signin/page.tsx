import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { InstancePaletteFetch } from "../_components/theme-sync";
import { SignInScreen } from "./sign-in-screen";

export const metadata: Metadata = { title: "Sign in" };

export default function SignInPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <>
      {/*
        No product shell or console here, so nothing else asks bootstrap for
        the instance palette; this is the one page that has to ask for it.
      */}
      <InstancePaletteFetch apiBaseUrl={apiBaseUrl} dataMode={dataMode} />
      <SignInScreen dataMode={dataMode} />
    </>
  );
}
