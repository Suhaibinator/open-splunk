export const ORGANIZATION_ROLES = [{ value: 1, label: "Viewer" }, { value: 23, label: "Admin" }, { value: 44, label: "Owner" }] as const;

/** GoCommon permits Owners to manage peers; other roles must rank higher. */
export function canManageOrganizationRole(actorRole: number, targetRole: number, siteAdministrator: boolean): boolean {
  if (!ORGANIZATION_ROLES.some((role) => role.value === targetRole)) return false;
  return siteAdministrator || actorRole === 44 || actorRole === 23 && targetRole < actorRole;
}

export function grantableOrganizationRoles(actorRole: number, siteAdministrator: boolean) {
  return ORGANIZATION_ROLES.filter((role) => canManageOrganizationRole(actorRole, role.value, siteAdministrator));
}
