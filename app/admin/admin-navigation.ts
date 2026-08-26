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
