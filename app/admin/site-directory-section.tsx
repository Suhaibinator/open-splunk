"use client";

import type { SearchDataMode } from "@/lib/search/backend-data";

import { IdentityProvider } from "../_components/identity-provider";
import { SiteOrganizationsPanel } from "./site-organizations-panel";
import { SiteUsersPanel } from "./site-users-panel";

export type SiteDirectoryView = "users" | "organizations";

/**
 * The Users or Organizations panel with the identity it reads. Nothing above
 * the admin console mounts an `IdentityProvider` yet, so the section mounts
 * its own for the page's data mode: the demo export gets the shared fake (the
 * client is one per page, so both sections see the same directory), and a
 * backend page gets the server binding, which reports `not_configured` until
 * the go-common routes are mounted.
 */
export function SiteDirectorySection({ dataMode, view }: { readonly dataMode: SearchDataMode; readonly view: SiteDirectoryView }) {
  return (
    <IdentityProvider dataMode={dataMode}>
      {view === "users" ? <SiteUsersPanel /> : <SiteOrganizationsPanel />}
    </IdentityProvider>
  );
}
