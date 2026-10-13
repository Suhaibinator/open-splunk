/** Credentials belong to the server's HttpOnly cookie, never this store. */
export interface IdentityUser {
  id: bigint;
  email: string;
  name: string;
  userType: number;
}

export interface IdentityOrganization {
  id: bigint;
  name: string;
  role: number;
}

export interface IdentitySnapshot {
  status: "loading" | "anonymous" | "authenticated" | "error";
  user: IdentityUser | null;
  organizations: readonly IdentityOrganization[];
  organizationId: bigint | null;
  revision: number;
  error: string | null;
}

export const INITIAL_IDENTITY: IdentitySnapshot = Object.freeze({
  status: "loading", user: null, organizations: [], organizationId: null, revision: 0, error: null,
});
let snapshot: IdentitySnapshot = INITIAL_IDENTITY;
const listeners = new Set<() => void>();

export function currentIdentity(): IdentitySnapshot { return snapshot; }
export function currentSessionRevision(): number { return snapshot.revision; }
export function currentOrganizationId(): bigint | null { return snapshot.organizationId; }
export function isSiteAdministrator(user: IdentityUser | null = snapshot.user): boolean {
  return user !== null && user.userType >= 44;
}
export function subscribeToSessionRevision(listener: () => void): () => void {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}
export function publishIdentity(next: Omit<IdentitySnapshot, "revision">, forceTransition = false): void {
  const changed = forceTransition || snapshot.user?.id !== next.user?.id
    || snapshot.user?.userType !== next.user?.userType
    || snapshot.organizationId !== next.organizationId || snapshot.status !== next.status
    || snapshot.organizations.map((org) => `${org.id}:${org.role}`).join(",")
      !== next.organizations.map((org) => `${org.id}:${org.role}`).join(",");
  snapshot = { ...next, revision: snapshot.revision + (changed ? 1 : 0) };
  for (const listener of Array.from(listeners)) listener();
}
export function selectOrganization(id: bigint): void {
  if (!isSiteAdministrator() && !snapshot.organizations.some((org) => org.id === id)) {
    throw new Error("Organization membership is required.");
  }
  if (id <= 0n) throw new Error("Organization ID must be positive.");
  publishIdentity({ ...snapshot, organizationId: id }, true);
}
export function clearIdentitySession(): void {
  publishIdentity({ status: "anonymous", user: null, organizations: [], organizationId: null, error: null }, true);
}

const unauthorizedListeners = new Set<() => void>();
export function reportSessionUnauthorized(): void {
  for (const listener of Array.from(unauthorizedListeners)) listener();
}
export function subscribeToSessionUnauthorized(listener: () => void): () => void {
  unauthorizedListeners.add(listener);
  return () => { unauthorizedListeners.delete(listener); };
}
