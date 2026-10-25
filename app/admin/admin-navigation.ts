import { ADMIN_CONSOLE_ACCESS, requireAll, requireAny, type AccessRequirement } from "@/lib/product-access";

export const ADMIN_SECTION_QUERY_PARAMETER = "section";

export function resolveAdminSection<Section extends string>(
  value: string | string[] | null | undefined,
  allowed: readonly Section[],
  fallback: Section,
): Section {
  const candidate = Array.isArray(value) ? value[0] : value;
  return candidate !== undefined
    && candidate !== null
    && allowed.includes(candidate as Section)
    ? candidate as Section
    : fallback;
}

export function adminSectionPath(currentHref: string, section: string): string {
  const url = new URL(currentHref);
  url.searchParams.set(ADMIN_SECTION_QUERY_PARAMETER, section);
  return `${url.pathname}${url.search}${url.hash}`;
}

/**
 * What each administration section asks of the principal. The console itself
 * opens for any of `ADMIN_CONSOLE_CAPABILITIES`; a section the principal
 * cannot use is left out of the navigation, and opening it by URL shows the
 * forbidden state. Keys cover both consoles (the demo preview's and the
 * backend's) and the identity sections being added beside them; a section not
 * listed here asks for the console's own requirement.
 */
export const ADMIN_SECTION_ACCESS: Readonly<Record<string, AccessRequirement>> = {
  overview: ADMIN_CONSOLE_ACCESS,
  apps: requireAll("apps.admin"),
  indexes: requireAll("indexes.admin"),
  "collector-fleet": requireAll("collectors.admin"),
  collectors: requireAny("ingest_tokens.admin", "collectors.admin", "hec.admin"),
  knowledge: requireAny("knowledge.write", "knowledge.admin"),
  lookups: requireAny("lookups.write", "knowledge.admin"),
  access: requireAny("members.admin", "users.admin"),
  members: requireAll("members.admin"),
  users: requireAll("users.admin"),
  organizations: requireAll("orgs.admin"),
  server: requireAll("server.admin"),
};

export function adminSectionAccess(section: string): AccessRequirement {
  return Object.hasOwn(ADMIN_SECTION_ACCESS, section) ? ADMIN_SECTION_ACCESS[section] : ADMIN_CONSOLE_ACCESS;
}

/** The navigation entries a principal may use, in their order; `offers` is the shell's answer for a requirement. */
export function adminNavigationFor<Item extends { readonly key: string }>(
  items: readonly Item[],
  offers: (requirement: AccessRequirement) => boolean,
): Item[] {
  return items.filter((item) => offers(adminSectionAccess(item.key)));
}
