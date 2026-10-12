import { Suspense } from "react";
import { GoogleCallback } from "./google-callback";
import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

export default function OAuthCallbackPage() {
  const { apiBaseUrl } = getFrontendRuntimeConfig();
  return <Suspense fallback={<main className="signin-page" role="status">Completing sign-in…</main>}><GoogleCallback apiBaseUrl={apiBaseUrl} /></Suspense>;
}
