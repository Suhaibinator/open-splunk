"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useEffectEvent, useMemo, useState, useSyncExternalStore, type ReactNode } from "react";
import { Select, SelectOption } from "./select";
import { currentIdentity, INITIAL_IDENTITY, isSiteAdministrator, selectOrganization, subscribeToSessionRevision, subscribeToSessionUnauthorized } from "@/lib/api/identity-session";
import { createIdentityClient, logoutIdentity, refreshIdentity } from "@/lib/api/identity-client";

export function useIdentity() {
  return useSyncExternalStore(subscribeToSessionRevision, currentIdentity, () => INITIAL_IDENTITY);
}

export function IdentityControls({ apiBaseUrl = "" }: { apiBaseUrl?: string }) {
  const identity = useIdentity();
  const [error, setError] = useState<string | null>(null);
  if (!identity.user) return null;
  return <div className="suite-menu-anchor">
    {identity.organizations.length > 0 ? <label>Organization <Select aria-label="Organization" value={identity.organizationId?.toString() ?? ""} onValueChange={(value) => selectOrganization(BigInt(value))}>{identity.organizations.map((org) => <SelectOption key={org.id.toString()} value={org.id.toString()}>{org.name}</SelectOption>)}</Select></label> : null}
    <Link className="button button--secondary button--compact" href="/organizations/">Organizations</Link>
    <button className="button button--secondary button--compact" type="button" onClick={() => { void logoutIdentity(createIdentityClient(apiBaseUrl)).then(() => window.location.assign("/signin/")).catch((reason: unknown) => setError(reason instanceof Error ? reason.message : "Sign out failed.")); }}>Sign out</button>
    {error ? <span role="alert">{error}</span> : null}
  </div>;
}

export function IdentityProvider({ children, dataMode, apiBaseUrl }: { children: ReactNode; dataMode: "backend" | "demo"; apiBaseUrl: string }) {
  const identity = useIdentity();
  const pathname = usePathname();
  const client = useMemo(() => createIdentityClient(apiBaseUrl), [apiBaseUrl]);
  const refreshCurrentIdentity = useEffectEvent(() => refreshIdentity(client));
  useEffect(() => {
    if (dataMode !== "backend") return;
    let pending: Promise<void> | null = null;
    const refresh = () => { pending ??= refreshCurrentIdentity().finally(() => { pending = null; }); };
    const visible = () => { if (document.visibilityState === "visible") refresh(); };
    refresh();
    const unsubscribe = subscribeToSessionUnauthorized(refresh);
    document.addEventListener("visibilitychange", visible);
    return () => { unsubscribe(); document.removeEventListener("visibilitychange", visible); };
  }, [dataMode]);
  if (dataMode === "demo" || pathname.startsWith("/signin") || pathname.startsWith("/auth/oauth/callback") || pathname.startsWith("/help")) return children;
  if (identity.status === "loading") return <main className="signin-page"><div className="signin-card" role="status">Verifying your session…</div></main>;
  if (identity.status === "error") return <main className="signin-page"><div className="signin-card"><p role="alert">{identity.error}</p><button className="button" onClick={() => { void refreshIdentity(client); }}>Try again</button></div></main>;
  if (!identity.user) return <main className="signin-page"><div className="signin-card"><h1>Sign in to Open Splunk</h1><Link className="button" href="/signin/">Continue with Google</Link></div></main>;
  if (!isSiteAdministrator() && identity.organizationId === null) return <main className="signin-page"><div className="signin-card"><h1>Your account is ready</h1><p>You don’t have access to an organization yet. Ask an organization administrator to add your account.</p><p>{identity.user.email} · Account ID {identity.user.id.toString()}</p><button className="button" onClick={() => { void refreshIdentity(client); }}>Check access again</button><IdentityControls apiBaseUrl={apiBaseUrl} /></div></main>;
  // Session/organization changes unmount every private workspace and its socket,
  // cancelling local requests before another tenant can publish results.
  return <div key={identity.revision}>{children}</div>;
}
