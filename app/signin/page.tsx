import type { Metadata } from "next";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { SignInScreen } from "./sign-in-screen";

export const metadata: Metadata = { title: "Sign in" };

export default function SignInPage() {
  const { dataMode } = getFrontendRuntimeConfig();
  return <SignInScreen dataMode={dataMode} />;
}
