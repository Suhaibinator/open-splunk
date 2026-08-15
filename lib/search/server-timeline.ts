import type { Duration } from "@/gen/ts/google/protobuf/duration";
import type { GetSearchTimelineResponse } from "@/gen/ts/open_splunk/v1/search_api";
import { ServerFeature } from "@/gen/ts/open_splunk/v1/system_api";
import type { TimelinePoint } from "@/lib/demo/search-data";
import { durationToMilliseconds } from "@/lib/api/duration";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";

export interface ServerTimelineBucket {
  id: string;
  earliest: string;
  latest: string;
  eventCount: bigint;
  partial: boolean;
}

export interface ServerTimeline {
  buckets: ServerTimelineBucket[];
  points: TimelinePoint[];
  bucketWidthMs: number;
  complete: boolean;
}

function millisecondsToDuration(milliseconds: number): Duration {
  const seconds = Math.floor(milliseconds / 1_000);
  return {
    seconds: BigInt(seconds),
    nanos: Math.round((milliseconds - seconds * 1_000) * 1_000_000),
  };
}

function validDate(date: Date | undefined): date is Date {
  return date !== undefined && !Number.isNaN(date.valueOf());
}

const TIMELINE_LABEL_FORMAT_OPTIONS = Object.freeze<Intl.DateTimeFormatOptions>({
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

export function adaptSearchTimeline(response: GetSearchTimelineResponse): ServerTimeline {
  const buckets = response.buckets.flatMap((bucket, index) => {
    if (!validDate(bucket.earliest) || !validDate(bucket.latest) || bucket.latest <= bucket.earliest) {
      return [];
    }
    return [{
      id: `server-bucket-${index}-${bucket.earliest.valueOf()}`,
      earliest: bucket.earliest.toISOString(),
      latest: bucket.latest.toISOString(),
      eventCount: bucket.eventCount,
      partial: bucket.partial,
    }];
  });
  const timelineFormatter = buckets.length === 0
    ? null
    : new Intl.DateTimeFormat("en-US", TIMELINE_LABEL_FORMAT_OPTIONS);
  return {
    buckets,
    points: timelineFormatter === null ? [] : buckets.map((bucket) => {
      const count = Number(bucket.eventCount);
      const coordinateApproximate = !Number.isSafeInteger(count);
      return {
        id: bucket.id,
        label: timelineFormatter.format(new Date(bucket.earliest)),
        count,
        exactCount: coordinateApproximate ? bucket.eventCount.toString() : undefined,
        coordinateApproximate: coordinateApproximate || undefined,
        earliest: bucket.earliest,
        latest: bucket.latest,
      };
    }),
    bucketWidthMs: durationToMilliseconds(response.bucketWidth),
    complete: response.complete,
  };
}

export interface GetServerTimelineOptions extends ProtobufRequestOptions {
  maxBuckets?: number;
  preferredBucketWidthMs?: number;
}

export async function getServerTimeline(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  options: GetServerTimelineOptions = {},
): Promise<OptionalFeatureResult<ServerTimeline>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_TIMELINE)) {
    return featureNotAdvertised;
  }
  const id = searchJobId.trim();
  if (id.length === 0) throw new TypeError("Search job ID is required.");
  const configuredMaximum = bootstrap.limits.maximumTimelineBuckets;
  const requestedMaximum = options.maxBuckets;
  if (requestedMaximum !== undefined && (!Number.isInteger(requestedMaximum) || requestedMaximum <= 0)) {
    throw new RangeError("Timeline bucket maximum must be a positive integer.");
  }
  const maxBuckets = requestedMaximum === undefined
    ? undefined
    : configuredMaximum > 0
      ? Math.min(requestedMaximum, configuredMaximum)
      : requestedMaximum;
  if (
    options.preferredBucketWidthMs !== undefined
    && (
      !Number.isSafeInteger(options.preferredBucketWidthMs)
      || options.preferredBucketWidthMs < 1_000
      || options.preferredBucketWidthMs % 1_000 !== 0
    )
  ) {
    throw new RangeError("Preferred timeline bucket width must be a positive whole number of seconds.");
  }
  try {
    const response = await client.search.timeline({
      searchJobId: id,
      maxBuckets,
      preferredBucketWidth: options.preferredBucketWidthMs === undefined
        ? undefined
        : millisecondsToDuration(options.preferredBucketWidthMs),
    }, options);
    return { status: "available", value: adaptSearchTimeline(response) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}
