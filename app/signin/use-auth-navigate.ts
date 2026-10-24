"use client";

import { useRouter } from "next/navigation";
import { useCallback } from "react";

import type { AuthNavigate } from "./auth-hooks";
import { isSameOriginPath } from "./return-to";

/**
 * Same-origin paths go through the router, so the page realm -- and with it
 * the demo's in-memory identity directory, its OAuth states and emailed
 * tokens -- survives the move; a provider's authorization URL is another
 * origin and needs a document load.
 */
export function useAuthNavigate(): AuthNavigate {
  const router = useRouter();
  return useCallback((url: string) => {
    if (isSameOriginPath(url)) router.push(url);
    else window.location.assign(url);
  }, [router]);
}
