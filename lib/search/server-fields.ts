import {
  type FieldProfile,
  type FieldSummary,
  type FieldValueCount,
} from "@/gen/ts/open_splunk/v1/result";
import { ServerFeature } from "@/gen/ts/open_splunk/v1/system_api";
import { ValueType, type TypedValue } from "@/gen/ts/open_splunk/v1/value";
import type {
  DemoField,
  DemoFieldValue,
  DemoScalar,
} from "@/lib/demo/search-data";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { recordNextPageToken } from "@/lib/api/pagination";
import {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import { exactDurationNumericText } from "@/lib/search/backend-data";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";

export interface ServerFieldProfile {
  name: string;
  displayName: string;
  valueType: ValueType;
  observedValueTypes: ValueType[];
  eventCount: bigint;
  nullCount: bigint;
  missingCount: bigint;
  distinctCount: bigint | null;
  distinctCountIsApproximate: boolean;
  selected: boolean;
  interesting: boolean;
}

export interface ServerFieldValue {
  value: DemoScalar;
  typedIdentity: string;
  typeLabel: string;
  count: bigint;
  countIsApproximate: boolean;
  pivotable: boolean;
}

export interface ServerFieldSummary {
  profile: ServerFieldProfile;
  topValues: ServerFieldValue[];
  topValuesAreApproximate: boolean;
}

export interface ServerFieldCatalog {
  fields: ServerFieldProfile[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  complete: boolean;
}

function typedValueToScalar(value: TypedValue | undefined): DemoScalar {
  switch (value?.kind?.$case) {
    case "nullValue":
    case "missingValue":
      return null;
    case "stringValue":
    case "doubleValue":
    case "boolValue":
      return value.kind.value;
    case "sint64Value":
    case "uint64Value": {
      const number = Number(value.kind.value);
      return Number.isSafeInteger(number) ? number : value.kind.value.toString();
    }
    case "timestampValue":
      return value.kind.value.toISOString();
    case "durationValue":
      return exactDurationNumericText(value.kind.value.seconds, value.kind.value.nanos);
    case "decimalValue":
      return value.kind.value.value;
    case "bytesValue":
      return `[${value.kind.value.byteLength} bytes]`;
    case "listValue":
      return JSON.stringify(value.kind.value.values.map(typedValueToScalar));
    case "objectValue":
      return JSON.stringify(Object.fromEntries(
        value.kind.value.fields.map((field) => [field.name, typedValueToScalar(field.value)]),
      ));
    default:
      return null;
  }
}

export function adaptFieldProfile(profile: FieldProfile): ServerFieldProfile {
  return {
    name: profile.fieldName,
    displayName: profile.displayName || profile.fieldName,
    valueType: profile.valueType,
    observedValueTypes: [...profile.observedValueTypes],
    eventCount: profile.eventCount,
    nullCount: profile.nullCount,
    missingCount: profile.missingCount,
    distinctCount: profile.distinctCount ?? null,
    distinctCountIsApproximate: profile.distinctCountIsApproximate,
    selected: profile.selected,
    interesting: profile.interesting,
  };
}

function typedValueIdentity(value: TypedValue | undefined): string {
  switch (value?.kind?.$case) {
    case "nullValue":
      return "null";
    case "missingValue":
      return "missing";
    case "stringValue":
      return `string:${JSON.stringify(value.kind.value)}`;
    case "doubleValue":
      return `double:${Object.is(value.kind.value, -0) ? "-0" : String(value.kind.value)}`;
    case "boolValue":
      return `bool:${value.kind.value}`;
    case "sint64Value":
      return `sint64:${value.kind.value.toString()}`;
    case "uint64Value":
      return `uint64:${value.kind.value.toString()}`;
    case "timestampValue":
      return `timestamp:${value.kind.value.toISOString()}`;
    case "durationValue":
      return `duration:${value.kind.value.seconds.toString()}:${value.kind.value.nanos}`;
    case "decimalValue":
      return `decimal:${value.kind.value.value}`;
    case "bytesValue":
      return `bytes:${Array.from(value.kind.value).join(",")}`;
    case "listValue":
      return `list:[${value.kind.value.values.map(typedValueIdentity).join("|")}]`;
    case "objectValue":
      return `object:{${value.kind.value.fields.map((field) => `${JSON.stringify(field.name)}=${typedValueIdentity(field.value)}`).join("|")}}`;
    default:
      return "unspecified";
  }
}

function typedValueTypeLabel(value: TypedValue | undefined): string {
  return value?.kind?.$case?.replace(/Value$/, "") ?? "unspecified";
}

function adaptFieldValue(value: FieldValueCount): ServerFieldValue {
  return {
    value: typedValueToScalar(value.value),
    typedIdentity: typedValueIdentity(value.value),
    typeLabel: typedValueTypeLabel(value.value),
    count: value.count,
    countIsApproximate: value.countIsApproximate,
    pivotable: typedValueIsPivotable(value.value),
  };
}

function typedValueIsPivotable(value: TypedValue | undefined): boolean {
  switch (value?.kind?.$case) {
    case "nullValue":
    case "stringValue":
    case "boolValue":
      return true;
    case "doubleValue":
      return Number.isFinite(value.kind.value);
    case "sint64Value":
    case "uint64Value":
      return Number.isSafeInteger(Number(value.kind.value));
    default:
      return false;
  }
}

export function adaptFieldSummary(summary: FieldSummary): ServerFieldSummary {
  if (summary.profile === undefined) {
    throw new TypeError("The field summary response did not include a profile.");
  }
  return {
    profile: adaptFieldProfile(summary.profile),
    topValues: summary.topValues.map(adaptFieldValue),
    topValuesAreApproximate: summary.topValuesAreApproximate,
  };
}

function countProjection(value: bigint): { coordinate: number; exact?: string } {
  const coordinate = Number(value);
  return {
    coordinate,
    exact: Number.isSafeInteger(coordinate) ? undefined : value.toString(),
  };
}

function demoFieldType(valueType: ValueType): DemoField["type"] {
  if (valueType === ValueType.VALUE_TYPE_BOOL) return "boolean";
  if (
    valueType === ValueType.VALUE_TYPE_SINT64
    || valueType === ValueType.VALUE_TYPE_UINT64
    || valueType === ValueType.VALUE_TYPE_DOUBLE
    || valueType === ValueType.VALUE_TYPE_DECIMAL
    || valueType === ValueType.VALUE_TYPE_DURATION
  ) {
    return "number";
  }
  return "string";
}

/** Adapts authoritative field data to the existing field-sidebar view model. */
export function serverFieldToDemoField(
  profile: ServerFieldProfile,
  values: readonly ServerFieldValue[] = [],
): DemoField {
  const eventCount = countProjection(profile.eventCount);
  const distinctCount = profile.distinctCount === null
    ? null
    : countProjection(profile.distinctCount);
  return {
    name: profile.name,
    displayName: profile.displayName,
    distinctCount: distinctCount?.coordinate ?? null,
    distinctCountExact: distinctCount?.exact,
    distinctCountIsApproximate: profile.distinctCountIsApproximate || undefined,
    eventCount: eventCount.coordinate,
    eventCountExact: eventCount.exact,
    selected: profile.selected,
    interesting: profile.interesting,
    type: demoFieldType(profile.valueType),
    values: values.map((value): DemoFieldValue => {
      const count = countProjection(value.count);
      return {
        value: value.value,
        count: count.coordinate,
        exactCount: count.exact,
        countIsApproximate: value.countIsApproximate || undefined,
        pivotable: value.pivotable,
        typedIdentity: value.typedIdentity,
        typeLabel: value.typeLabel,
      };
    }),
  };
}

export interface GetServerFieldCatalogOptions extends ProtobufRequestOptions {
  nameFilter?: string;
  interestingOnly?: boolean;
  pageSize?: number;
  pageToken?: string;
  /** Protects callers from a malformed server repeating endless cursors. */
  maximumPages?: number;
}

export async function getServerFieldCatalog(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  options: GetServerFieldCatalogOptions = {},
): Promise<OptionalFeatureResult<ServerFieldCatalog>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_FIELD_DISCOVERY)) {
    return featureNotAdvertised;
  }
  const id = searchJobId.trim();
  if (id.length === 0) throw new TypeError("Search job ID is required.");
  const configuredMaximum = bootstrap.limits.maximumPageSize;
  const defaultPageSize = configuredMaximum > 0 ? configuredMaximum : undefined;
  const requestedPageSize = options.pageSize ?? defaultPageSize;
  if (
    requestedPageSize !== undefined
    && (!Number.isInteger(requestedPageSize) || requestedPageSize <= 0)
  ) {
    throw new RangeError("Field catalog page size must be a positive integer.");
  }
  const pageSize = requestedPageSize === undefined
    ? undefined
    : configuredMaximum > 0
      ? Math.min(requestedPageSize, configuredMaximum)
      : requestedPageSize;
  const maximumPages = options.maximumPages ?? 256;
  if (!Number.isInteger(maximumPages) || maximumPages <= 0) {
    throw new RangeError("Field catalog maximum pages must be a positive integer.");
  }

  const fields: ServerFieldProfile[] = [];
  const initialPageToken = options.pageToken?.trim() || undefined;
  const seenTokens = new Set<string>(initialPageToken === undefined ? [] : [initialPageToken]);
  let pageToken = initialPageToken;
  let totalSize: bigint | null = null;
  let totalSizeExact = false;
  try {
    for (let pageIndex = 0; pageIndex < maximumPages; pageIndex += 1) {
      // Cursor pages are causally ordered and cannot be requested in parallel.
      // eslint-disable-next-line no-await-in-loop
      const response = await client.search.fields({
        searchJobId: id,
        page: {
          pageSize,
          pageToken,
          includeTotalSize: pageIndex === 0 && initialPageToken === undefined,
        },
        nameFilter: options.nameFilter?.trim() || undefined,
        interestingOnly: options.interestingOnly ?? false,
      }, options);
      fields.push(...response.fields.map(adaptFieldProfile));
      if (pageIndex === 0) {
        totalSize = response.page?.totalSize ?? null;
        totalSizeExact = response.page?.totalSizeExact ?? false;
      }
      const nextToken = recordNextPageToken(
        seenTokens,
        response.page?.nextPageToken,
        "The field catalog",
      );
      if (nextToken === null) {
        return {
          status: "available",
          value: {
            fields,
            nextPageToken: null,
            totalSize,
            totalSizeExact,
            complete: true,
          },
        };
      }
      pageToken = nextToken;
    }
    return {
      status: "available",
      value: {
        fields,
        nextPageToken: pageToken ?? null,
        totalSize,
        totalSizeExact,
        complete: false,
      },
    };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface GetServerFieldSummaryOptions extends ProtobufRequestOptions {
  maxValues?: number;
}

export async function getServerFieldSummary(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  fieldName: string,
  options: GetServerFieldSummaryOptions = {},
): Promise<OptionalFeatureResult<ServerFieldSummary>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_FIELD_DISCOVERY)) {
    return featureNotAdvertised;
  }
  const id = searchJobId.trim();
  const name = fieldName;
  if (id.length === 0) throw new TypeError("Search job ID is required.");
  if (name.length === 0) throw new TypeError("Field name is required.");
  const requestedMaximum = options.maxValues;
  if (requestedMaximum !== undefined && (!Number.isInteger(requestedMaximum) || requestedMaximum <= 0)) {
    throw new RangeError("Field summary value maximum must be a positive integer.");
  }
  const configuredMaximum = bootstrap.limits.maximumFieldSummaryValues;
  const maxValues = requestedMaximum === undefined
    ? undefined
    : configuredMaximum > 0
      ? Math.min(requestedMaximum, configuredMaximum)
      : requestedMaximum;
  try {
    const response = await client.search.fieldSummary({
      searchJobId: id,
      fieldName: name,
      maxValues,
    }, options);
    if (response.fieldSummary === undefined) {
      throw new TypeError("The server returned an empty field summary.");
    }
    return { status: "available", value: adaptFieldSummary(response.fieldSummary) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}
