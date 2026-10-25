"use client";

import type { SearchDataMode } from "@/lib/search/backend-data";

import { ShellIdentityBoundary } from "../_components/product-identity";
import { SiteOrganizationsPanel } from "./site-organizations-panel";
import { SiteUsersPanel } from "./site-users-panel";

export type SiteDirectoryView = "users" | "organizations";

/**
 * The Users or Organizations panel with the identity it reads: the product
 * shell's provider when the section sits inside the shell, so revoking one's
 * own sessions or losing site-admin access moves the bar, the navigation and
 * the access gate with the panel; otherwise its own for the page's data mode.
 * The demo export gets the shared fake (the client is one per page, so both
 * sections see the same directory), and a backend page gets the server
 * binding, which reports `not_configured` until the go-common routes are
 * mounted.
 */
export function SiteDirectorySection({ dataMode, view }: { readonly dataMode: SearchDataMode; readonly view: SiteDirectoryView }) {
  return (
    <ShellIdentityBoundary dataMode={dataMode}>
      {view === "users" ? <SiteUsersPanel /> : <SiteOrganizationsPanel />}
    </ShellIdentityBoundary>
  );
}
