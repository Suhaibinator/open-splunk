"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";

import {
  IdentityError,
  createGoCommonIdentityClient,
  isIdentityError,
  principalHasCapability,
  type Capability,
  type IdentityClient,
  type IdentityOrganization,
  type OrganizationId,
  type Principal,
} from "@/lib/api/identity";
import { getDemoIdentityClient } from "@/lib/demo/identity-data";
import type { SearchDataMode } from "@/lib/search/backend-data";

/**
 * - `loading`: the principal has not been read yet.
 * - `signed_in` / `signed_out`: the last read succeeded.
 * - `unavailable`: no principal could be read (identity is not configured, or
 *   the service failed before anyone was known); `error` says why.
 */
export type IdentityStatus = "loading" | "signed_in" | "signed_out" | "unavailable";

export interface IdentitySnapshot {
  readonly status: IdentityStatus;
  readonly principal: Principal | null;
  /** The last failure, kept beside a still-valid principal when a refresh fails. */
  readonly error: IdentityError | null;
}

/** The session state behind the provider, usable without React. */
export interface IdentitySessionStore {
  getSnapshot(): IdentitySnapshot;
  subscribe(listener: () => void): () => void;
  /** Re-reads the principal; a newer refresh or adoption supersedes an older one. */
  refresh(): Promise<Principal | null>;
  /** Installs a principal a flow already holds (a sign-in result, say), or null after a sign-out elsewhere. */
  adopt(principal: Principal | null): void;
  signOut(options?: { readonly everywhere?: boolean }): Promise<void>;
  setActiveOrganization(organizationId: OrganizationId): Promise<Principal>;
}

function asIdentityError(error: unknown): IdentityError {
  if (isIdentityError(error)) return error;
  return new IdentityError("network", error instanceof Error ? error.message : "The identity service failed.", { cause: error });
}

function snapshotFor(principal: Principal | null): IdentitySnapshot {
  return { status: principal === null ? "signed_out" : "signed_in", principal, error: null };
}

const NO_CLIENT_ERROR = new IdentityError("not_configured", "No identity client is configured.");

export function createIdentitySessionStore(
  client: IdentityClient | null,
  initial: { readonly principal?: Principal | null; readonly error?: IdentityError } = {},
): IdentitySessionStore {
  let snapshot: IdentitySnapshot = client === null
    ? { status: "unavailable", principal: null, error: initial.error ?? NO_CLIENT_ERROR }
    : initial.principal === undefined
      ? { status: "loading", principal: null, error: null }
      : snapshotFor(initial.principal);
  let epoch = 0;
  const listeners = new Set<() => void>();

  function publish(next: IdentitySnapshot): void {
    snapshot = next;
    for (const listener of Array.from(listeners)) {
      try {
        listener();
      } catch {
        // One broken consumer cannot keep the others on the previous session.
      }
    }
  }

  function requireClient(): IdentityClient {
    if (client === null) throw snapshot.error ?? NO_CLIENT_ERROR;
    return client;
  }

  function adopt(principal: Principal | null): void {
    epoch += 1;
    publish(snapshotFor(principal));
  }

  return {
    getSnapshot: () => snapshot,
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    async refresh() {
      const identity = requireClient();
      epoch += 1;
      const requested = epoch;
      try {
        const principal = await identity.getPrincipal();
        if (requested === epoch) publish(snapshotFor(principal));
        return principal;
      } catch (caught) {
        const error = asIdentityError(caught);
        if (requested === epoch) {
          publish(error.kind === "unauthenticated"
            ? snapshotFor(null)
            : snapshot.principal === null
              ? { status: "unavailable", principal: null, error }
              : { ...snapshot, error });
        }
        throw error;
      }
    },
    adopt,
    async signOut(options) {
      try {
        await requireClient().logout(options);
      } catch (caught) {
        // A session that already ended -- expired, revoked, or signed out in
        // another tab -- is what signing out asked for; anything else failed.
        if (!isIdentityError(caught, "unauthenticated")) throw caught;
      }
      adopt(null);
    },
    async setActiveOrganization(organizationId) {
      const principal = await requireClient().setActiveOrganization(organizationId);
      adopt(principal);
      return principal;
    },
  };
}

/** The identity client for a page in this data mode; the server binding refuses until it exists. */
export function resolveIdentityClient(dataMode: SearchDataMode): IdentityClient {
  return dataMode === "demo" ? getDemoIdentityClient() : createGoCommonIdentityClient();
}

export interface IdentityContextValue extends IdentitySnapshot {
  /** Null while identity is unavailable. */
  readonly client: IdentityClient | null;
  readonly activeOrganization: IdentityOrganization | null;
  hasCapability(capability: Capability): boolean;
  signOut(options?: { readonly everywhere?: boolean }): Promise<void>;
  refresh(): Promise<Principal | null>;
  adoptPrincipal(principal: Principal | null): void;
  setActiveOrganization(organizationId: OrganizationId): Promise<Principal>;
}

const IdentityContext = createContext<IdentityContextValue | null>(null);

/**
 * A provider takes an explicit client (a test, or a page that built one) or
 * the page's data mode, never neither: a backend page must not fall back to
 * the demo directory. Both are read once, on mount.
 */
export type IdentityProviderProps = (
  | { readonly client: IdentityClient; readonly dataMode?: undefined }
  | { readonly client?: undefined; readonly dataMode: SearchDataMode }
) & {
  /** A principal already known (from bootstrap, or a test); skips the initial read. Read once, on mount. */
  readonly initialPrincipal?: Principal | null;
  readonly children?: ReactNode;
};

interface ProviderBinding {
  readonly client: IdentityClient | null;
  readonly store: IdentitySessionStore;
}

function bindProvider(props: IdentityProviderProps): ProviderBinding {
  let client = props.client ?? null;
  let error: IdentityError | undefined;
  if (client === null && props.dataMode !== undefined) {
    try {
      client = resolveIdentityClient(props.dataMode);
    } catch (caught) {
      error = asIdentityError(caught);
    }
  }
  return { client, store: createIdentitySessionStore(client, { principal: props.initialPrincipal, error }) };
}

/** Reads the principal once on mount and shares it, with the client, below. */
export function IdentityProvider(props: IdentityProviderProps) {
  const [{ client, store }] = useState(() => bindProvider(props));
  const snapshot = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
  const needsRead = snapshot.status === "loading";

  useEffect(() => {
    if (!needsRead) return;
    // The store records the failure; nothing else waits on this read.
    store.refresh().catch(() => undefined);
  }, [needsRead, store]);

  const { principal } = snapshot;
  const hasCapability = useCallback(
    (capability: Capability) => principalHasCapability(principal, capability),
    [principal],
  );

  const value = useMemo<IdentityContextValue>(() => ({
    ...snapshot,
    client,
    activeOrganization: snapshot.principal?.activeOrganization ?? null,
    hasCapability,
    signOut: store.signOut,
    refresh: store.refresh,
    adoptPrincipal: store.adopt,
    setActiveOrganization: store.setActiveOrganization,
  }), [client, hasCapability, snapshot, store]);

  return <IdentityContext.Provider value={value}>{props.children}</IdentityContext.Provider>;
}

/** The identity context; only valid below an `IdentityProvider`. */
export function useIdentity(): IdentityContextValue {
  const value = useContext(IdentityContext);
  if (value === null) throw new Error("useIdentity needs an IdentityProvider above it.");
  return value;
}

/**
 * The identity context, or null when no `IdentityProvider` is above: for a
 * shared component (the product shell) that mounts its own provider only when
 * the page has not already supplied one.
 */
export function useOptionalIdentity(): IdentityContextValue | null {
  return useContext(IdentityContext);
}

export interface RequireCapabilityProps {
  /** One capability, or several that must all be held (or any one, with `match="any"`). */
  readonly capability: Capability | readonly Capability[];
  readonly match?: "all" | "any";
  /** Shown when the principal lacks the capability, is signed out, or identity is unavailable. */
  readonly fallback?: ReactNode;
  /** Shown while the principal is still being read. */
  readonly loading?: ReactNode;
  readonly children?: ReactNode;
}

/** Renders its children only for a principal holding the capability. */
export function RequireCapability({
  capability,
  match = "all",
  fallback = null,
  loading = null,
  children,
}: RequireCapabilityProps) {
  const { hasCapability, status } = useIdentity();
  if (status === "loading") return <>{loading}</>;
  const required: readonly Capability[] = typeof capability === "string" ? [capability] : capability;
  const allowed = required.length > 0
    && (match === "any" ? required.some(hasCapability) : required.every(hasCapability));
  return <>{allowed ? children : fallback}</>;
}
