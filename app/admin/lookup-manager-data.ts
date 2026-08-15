import { SortDirection } from "@/gen/ts/open_splunk/v1/common";
import {
  Lookup,
  LookupState,
  type Lookup as LookupMessage,
  type LookupDefinition,
} from "@/gen/ts/open_splunk/v1/lookup";
import {
  CreateLookupRequest,
  DeleteLookupRequest,
  GetLookupRequest,
  ListLookupsRequest,
  LookupSortBy,
  PreviewLookupRequest,
  ReplaceLookupRequest,
  SetLookupStateRequest,
  type CreateLookupResponse,
  type DeleteLookupResponse,
  type GetLookupResponse,
  type ListLookupsResponse,
  type PreviewLookupResponse,
  type ReplaceLookupResponse,
  type SetLookupStateResponse,
} from "@/gen/ts/open_splunk/v1/lookup_api";
import {
  ProtobufTransport,
  type ProtobufRequestOptions,
  type ProtobufTransportOptions,
} from "@/lib/api/protobuf-transport";
import { lookupRoutes } from "@/lib/api/routes";

import {
  LOOKUP_MANAGER_CONTRACT,
  isBoundedCanonicalLookupDefinition,
  isManagementIdentity,
  textBytes,
} from "./lookup-manager-contract";

const MAXIMUM_SQLITE_VERSION = (1n << 63n) - 1n;

export interface LookupManagerClient {
  create(
    definition: LookupDefinition,
    csvData: Uint8Array,
    options?: ProtobufRequestOptions,
  ): Promise<LookupMessage>;
  get(lookupId: string, version?: bigint, options?: ProtobufRequestOptions): Promise<LookupMessage>;
  list(appId?: string, options?: ProtobufRequestOptions): Promise<readonly LookupMessage[]>;
  replace(
    lookupId: string,
    expectedVersion: bigint,
    definition: LookupDefinition,
    csvData?: Uint8Array,
    options?: ProtobufRequestOptions,
  ): Promise<LookupMessage>;
  setState(
    lookupId: string,
    expectedVersion: bigint,
    state: LookupState,
    options?: ProtobufRequestOptions,
  ): Promise<LookupMessage>;
  delete(
    lookupId: string,
    expectedVersion: bigint,
    confirmationName: string,
    options?: ProtobufRequestOptions,
  ): Promise<bigint>;
  preview(
    definition: LookupDefinition,
    csvData: Uint8Array,
    maximumRows?: number,
    options?: ProtobufRequestOptions,
  ): Promise<PreviewLookupResponse>;
}

export function createLookupManagerClient(
  options: ProtobufTransportOptions = {},
): LookupManagerClient {
  const transport = new ProtobufTransport(options);
  return {
    create: async (definition, csvData, requestOptions) => {
      validateCSVBytes(csvData);
      const response: CreateLookupResponse = await transport.post(
        lookupRoutes.create,
        CreateLookupRequest.fromPartial({ definition, csvData }),
        requestOptions,
      );
      const lookup = requiredLookup(response.lookup);
      if (lookup.version !== 1n || lookup.state !== LookupState.LOOKUP_STATE_ACTIVE) {
        throw new TypeError("Lookup create response authority is invalid.");
      }
      return lookup;
    },
    get: async (lookupId, version, requestOptions) => {
      const response: GetLookupResponse = await transport.post(
        lookupRoutes.get,
        GetLookupRequest.fromPartial({ lookupId, version }),
        requestOptions,
      );
      const lookup = requiredLookup(response.lookup);
      if (lookup.lookupId !== lookupId || (version !== undefined && lookup.version !== version)) {
        throw new TypeError("Lookup get response authority is invalid.");
      }
      return lookup;
    },
    list: async (appId, requestOptions) => {
      const result: LookupMessage[] = [];
      const seenIDs = new Set<string>();
      const seenTokens = new Set<string>();
      let pageToken: string | undefined;
      let pageCount = 0;
      let authority: { readonly tenantId: string; readonly ownerId: string } | undefined;
      for (;;) {
        pageCount += 1;
        if (pageCount > LOOKUP_MANAGER_CONTRACT.maximumListPages) {
          throw new RangeError("Lookup list exceeds its bounded page count.");
        }
        // eslint-disable-next-line no-await-in-loop -- each opaque cursor is authority for the following page.
        const response: ListLookupsResponse = await transport.post(
          lookupRoutes.list,
          ListLookupsRequest.fromPartial({
            page: { pageSize: LOOKUP_MANAGER_CONTRACT.listPageSize, pageToken },
            appId,
            stateFilters: [LookupState.LOOKUP_STATE_ACTIVE, LookupState.LOOKUP_STATE_DISABLED],
            sortBy: LookupSortBy.LOOKUP_SORT_BY_NAME,
            sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
          }),
          requestOptions,
        );
        if (response.page === undefined || response.lookups.length > LOOKUP_MANAGER_CONTRACT.listPageSize) {
          throw new TypeError("Lookup list page is outside its bounded contract.");
        }
        for (const value of response.lookups) {
          const lookup = requiredLookup(value);
          if (
            (lookup.state !== LookupState.LOOKUP_STATE_ACTIVE && lookup.state !== LookupState.LOOKUP_STATE_DISABLED)
            || (appId !== undefined && lookup.definition?.appId !== appId)
          ) {
            throw new TypeError("Lookup list item disagrees with the requested filter authority.");
          }
          authority ??= { tenantId: lookup.tenantId, ownerId: lookup.ownerId };
          if (lookup.tenantId !== authority.tenantId || lookup.ownerId !== authority.ownerId) {
            throw new TypeError("Lookup list crosses an authenticated management scope.");
          }
          if (seenIDs.has(lookup.lookupId)) {
            throw new TypeError("Lookup list contains a repeated identifier.");
          }
          seenIDs.add(lookup.lookupId);
          result.push(lookup);
          if (result.length > LOOKUP_MANAGER_CONTRACT.maximumManagedLookups) {
            throw new RangeError("Lookup catalog exceeds its managed-object limit.");
          }
        }
        const next = response.page.nextPageToken;
        if (next === undefined || next.length === 0) return result;
        if (
          response.lookups.length !== LOOKUP_MANAGER_CONTRACT.listPageSize
          || textBytes(next) > LOOKUP_MANAGER_CONTRACT.maximumPageTokenBytes
          || seenTokens.has(next)
        ) {
          throw new TypeError("Lookup list returned an invalid or repeated page cursor.");
        }
        seenTokens.add(next);
        pageToken = next;
      }
    },
    replace: async (
      lookupId,
      expectedVersion,
      definition,
      csvData,
      requestOptions,
    ) => {
      if (csvData !== undefined) validateCSVBytes(csvData);
      const response: ReplaceLookupResponse = await transport.post(
        lookupRoutes.replace,
        ReplaceLookupRequest.fromPartial({ lookupId, expectedVersion, definition, csvData }),
        requestOptions,
      );
      const lookup = requiredLookup(response.lookup);
      if (lookup.lookupId !== lookupId || lookup.version !== expectedVersion + 1n) {
        throw new TypeError("Lookup replace response authority is invalid.");
      }
      return lookup;
    },
    setState: async (lookupId, expectedVersion, state, requestOptions) => {
      const response: SetLookupStateResponse = await transport.post(
        lookupRoutes.setState,
        SetLookupStateRequest.fromPartial({ lookupId, expectedVersion, state }),
        requestOptions,
      );
      const lookup = requiredLookup(response.lookup);
      if (lookup.lookupId !== lookupId || lookup.version !== expectedVersion + 1n || lookup.state !== state) {
        throw new TypeError("Lookup state response authority is invalid.");
      }
      return lookup;
    },
    delete: async (lookupId, expectedVersion, confirmationName, requestOptions) => {
      const response: DeleteLookupResponse = await transport.post(
        lookupRoutes.delete,
        DeleteLookupRequest.fromPartial({ lookupId, expectedVersion, confirmationName }),
        requestOptions,
      );
      if (response.lookupId !== lookupId || response.version !== expectedVersion + 1n) {
        throw new TypeError("Lookup delete response authority is invalid.");
      }
      return response.version;
    },
    preview: async (definition, csvData, maximumRows, requestOptions) => {
      validateCSVBytes(csvData);
      const boundedRows = maximumRows ?? LOOKUP_MANAGER_CONTRACT.maximumPreviewRows;
      if (!Number.isSafeInteger(boundedRows) || boundedRows < 1 || boundedRows > LOOKUP_MANAGER_CONTRACT.maximumPreviewRows) {
        throw new RangeError(`Lookup preview rows must be between 1 and ${LOOKUP_MANAGER_CONTRACT.maximumPreviewRows}.`);
      }
      return transport.post(
        lookupRoutes.preview,
        PreviewLookupRequest.fromPartial({ definition, csvData, maximumRows: boundedRows }),
        requestOptions,
      );
    },
  };
}

export function validateCSVBytes(csvData: Uint8Array): void {
  if (!(csvData instanceof Uint8Array) || csvData.byteLength === 0) {
    throw new TypeError("Lookup CSV must be a nonempty byte sequence.");
  }
  if (csvData.byteLength > LOOKUP_MANAGER_CONTRACT.maximumUploadBytes) {
    throw new RangeError(`Lookup CSV exceeds ${LOOKUP_MANAGER_CONTRACT.maximumUploadBytes} bytes.`);
  }
}

function requiredLookup(value: LookupMessage | undefined): LookupMessage {
  if (value === undefined) throw new TypeError("Lookup response is missing its lookup.");
  if (!boundedLookupDefinitionShape(value.definition)) {
    throw new TypeError("Lookup response definition exceeds its entry limit.");
  }
  const detached = Lookup.fromPartial(value);
  if (
    !isManagementIdentity(detached.lookupId, LOOKUP_MANAGER_CONTRACT.maximumLookupIdBytes)
    || !isManagementIdentity(detached.tenantId, LOOKUP_MANAGER_CONTRACT.maximumTenantIdBytes)
    || !isManagementIdentity(detached.ownerId, LOOKUP_MANAGER_CONTRACT.maximumOwnerIdBytes)
    || detached.version <= 0n
    || detached.version > MAXIMUM_SQLITE_VERSION
    || detached.definition === undefined
    || detached.columns.length === 0
    || detached.columns.length > LOOKUP_MANAGER_CONTRACT.maximumColumns
    || detached.columns.some((column) => !isLookupHeader(column))
    || new Set(detached.columns).size !== detached.columns.length
    || detached.rowCount < 0n
    || detached.rowCount > BigInt(LOOKUP_MANAGER_CONTRACT.maximumAssetRows)
    || detached.canonicalSizeBytes < 1n
    || detached.canonicalSizeBytes > BigInt(LOOKUP_MANAGER_CONTRACT.maximumUploadBytes)
    || detached.sourceSha256.byteLength !== LOOKUP_MANAGER_CONTRACT.sha256Bytes
    || detached.contentSha256.byteLength !== LOOKUP_MANAGER_CONTRACT.sha256Bytes
    || detached.createdAt === undefined
    || detached.updatedAt === undefined
    || !validDate(detached.createdAt)
    || !validDate(detached.updatedAt)
    || detached.updatedAt < detached.createdAt
    || (detached.state !== LookupState.LOOKUP_STATE_ACTIVE
      && detached.state !== LookupState.LOOKUP_STATE_DISABLED
      && detached.state !== LookupState.LOOKUP_STATE_DELETED)
  ) {
    throw new TypeError("Lookup response authority is invalid.");
  }
  if (!isBoundedCanonicalLookupDefinition(detached.definition, detached.columns)) {
    throw new TypeError("Lookup response definition authority is invalid.");
  }
  if (
    (detached.state === LookupState.LOOKUP_STATE_ACTIVE
      && (detached.disabledAt !== undefined || detached.deletedAt !== undefined))
    || (detached.state === LookupState.LOOKUP_STATE_DISABLED
      && (!validLifecycleDate(detached.disabledAt, detached.createdAt, detached.updatedAt)
        || detached.deletedAt !== undefined))
    || (detached.state === LookupState.LOOKUP_STATE_DELETED
      && (!validLifecycleDate(detached.disabledAt, detached.createdAt, detached.updatedAt)
        || !validLifecycleDate(detached.deletedAt, detached.disabledAt!, detached.updatedAt)))
  ) throw new TypeError("Lookup response lifecycle authority is invalid.");
  return detached;
}

function boundedLookupDefinitionShape(definition: LookupDefinition | undefined): boolean {
  if (
    definition === undefined
    || definition.keyMappings.length > LOOKUP_MANAGER_CONTRACT.maximumKeyMappings
    || definition.outputMappings.length > LOOKUP_MANAGER_CONTRACT.maximumOutputMappings
  ) {
    return false;
  }
  const selector = definition.selector;
  return selector === undefined
    || (selector.indexPatterns.length <= LOOKUP_MANAGER_CONTRACT.maximumSelectorPatternsPerDimension
      && selector.hostPatterns.length <= LOOKUP_MANAGER_CONTRACT.maximumSelectorPatternsPerDimension
      && selector.sourcePatterns.length <= LOOKUP_MANAGER_CONTRACT.maximumSelectorPatternsPerDimension
      && selector.sourcetypePatterns.length <= LOOKUP_MANAGER_CONTRACT.maximumSelectorPatternsPerDimension);
}

function isLookupHeader(value: string): boolean {
  return value.length > 0
    && textBytes(value) <= LOOKUP_MANAGER_CONTRACT.maximumHeaderBytes
    && value.trim() === value
    && !value.includes("\0")
    && !/[\p{Cc}\p{Cf}]/u.test(value);
}

function validDate(value: Date): boolean {
  return !Number.isNaN(value.valueOf());
}

function validLifecycleDate(value: Date | undefined, earliest: Date, latest: Date): value is Date {
  return value !== undefined && validDate(value) && value >= earliest && value <= latest;
}
