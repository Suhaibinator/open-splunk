import {
  type DashboardDefinition,
  DashboardDefinition as DashboardDefinitionCodec,
} from "@/gen/ts/open_splunk/dashboard";

function encodedDefinitionsEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  return left.every((value, index) => value === right[index]);
}

export function cloneDashboardDefinition(definition: DashboardDefinition): DashboardDefinition {
  return DashboardDefinitionCodec.fromPartial(definition);
}

/** Compares the complete protobuf definition, including nested panel searches. */
export function dashboardDefinitionsEqual(
  left: DashboardDefinition | null | undefined,
  right: DashboardDefinition | null | undefined,
): boolean {
  if (left === right) return true;
  if (left == null || right == null) return false;
  return encodedDefinitionsEqual(
    DashboardDefinitionCodec.encode(left).finish(),
    DashboardDefinitionCodec.encode(right).finish(),
  );
}

/**
 * Applies the authoritative saved definition only when the editor still holds
 * the exact snapshot that was submitted. Newer local edits always win.
 */
export function reconcileSavedDashboardDraft(
  current: DashboardDefinition | null,
  submitted: DashboardDefinition,
  persisted: DashboardDefinition,
): DashboardDefinition | null {
  return dashboardDefinitionsEqual(current, submitted)
    ? cloneDashboardDefinition(persisted)
    : current;
}
