import { SharingScope } from "@/gen/ts/open_splunk/common";
import { SearchDefinition } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";

import {
  adaptSavedSearch,
  type ServerSavedSearch,
} from "./server-objects";

export type EditableSavedSearchScope =
  | SharingScope.SHARING_SCOPE_PRIVATE
  | SharingScope.SHARING_SCOPE_APP
  | SharingScope.SHARING_SCOPE_GLOBAL;

export const DEFAULT_SAVED_SEARCH_SCOPE = SharingScope.SHARING_SCOPE_PRIVATE;

export const SAVED_SEARCH_SCOPE_OPTIONS: ReadonlyArray<{
  label: string;
  value: EditableSavedSearchScope;
}> = [
  { label: "Private", value: SharingScope.SHARING_SCOPE_PRIVATE },
  { label: "App", value: SharingScope.SHARING_SCOPE_APP },
  { label: "Global", value: SharingScope.SHARING_SCOPE_GLOBAL },
];

export function isEditableSavedSearchScope(scope: SharingScope): scope is EditableSavedSearchScope {
  return SAVED_SEARCH_SCOPE_OPTIONS.some((option) => option.value === scope);
}

export function savedSearchScopeLabel(scope: SharingScope): string {
  return SAVED_SEARCH_SCOPE_OPTIONS.find((option) => option.value === scope)?.label ?? "Unknown";
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

/**
 * Checks the server reply before the Reports editor adopts it. A scope-only
 * update must not silently replace search, ownership, or descriptive metadata
 * even if a newer server returns a malformed envelope. The joined schedule is
 * allowed to advance independently while this request is in flight.
 */
export function assertSavedSearchScopeUpdate(
  baseline: ServerSavedSearch,
  proposedScope: EditableSavedSearchScope,
  updated: ServerSavedSearch,
): void {
  if (
    updated.id !== baseline.id
    || updated.version !== baseline.version + 1n
    || updated.sharingScope !== proposedScope
    || updated.name !== baseline.name
    || updated.description !== baseline.description
    || updated.ownerId !== baseline.ownerId
    || !sameBytes(
      SearchDefinition.encode(updated.search).finish(),
      SearchDefinition.encode(baseline.search).finish(),
    )
  ) {
    throw new TypeError("The server returned an invalid saved-search sharing update.");
  }
}

/** Writes organizational sharing metadata under the exact loaded version. */
export async function updateSavedSearchScope(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  baseline: ServerSavedSearch,
  proposedScope: EditableSavedSearchScope,
  options?: ProtobufRequestOptions,
): Promise<ServerSavedSearch> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    throw new Error("The server does not advertise saved-search updates.");
  }
  if (
    baseline.id.trim().length === 0
    || baseline.version <= 0n
    || !isEditableSavedSearchScope(baseline.sharingScope)
    || !isEditableSavedSearchScope(proposedScope)
  ) {
    throw new TypeError("The saved-search sharing update has an invalid baseline or proposed scope.");
  }
  if (proposedScope === SharingScope.SHARING_SCOPE_APP && !baseline.search.appId?.trim()) {
    throw new TypeError("App sharing requires a saved-search app.");
  }
  const response = await client.savedSearches.update({
    savedSearchId: baseline.id,
    expectedVersion: baseline.version,
    definition: {
      name: baseline.name,
      description: baseline.description || undefined,
      search: baseline.search,
      sharingScope: proposedScope,
      ownerId: baseline.ownerId ?? undefined,
      schedule: undefined,
    },
    updateMask: ["sharing_scope"],
  }, options);
  if (response.savedSearch === undefined) {
    throw new TypeError("The server returned an empty saved-search sharing update.");
  }
  const updated = adaptSavedSearch(response.savedSearch);
  assertSavedSearchScopeUpdate(baseline, proposedScope, updated);
  return updated;
}
