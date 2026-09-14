import type {
  GetHECOperationalSnapshotResponse,
  HECFixedHistogram,
  HECReconciliationOperationalMetrics,
} from "@/gen/ts/open_splunk/hec_admin_api";

import { formatMediumDateTime } from "../_components/date-format";

const UINT64_MAX = (1n << 64n) - 1n;
const DURATION_MAX_SECONDS = 315_576_000_000n;

const HEC_SHAPE_UPPER_BOUNDS = [
  1n, 10n, 64n, 100n, 500n, 1_000n, 2_500n, 5_000n, 10_000n, 16_384n,
  32_768n, 50_000n, 65_536n,
] as const;
const HEC_BYTE_UPPER_BOUNDS = [
  1_024n, 4_096n, 16_384n, 65_536n, 262_144n, 1_048_576n, 4_194_304n,
  8_388_608n, 16_777_216n, 25_165_824n, 33_554_432n, 50_331_648n, 67_108_864n,
] as const;
const HEC_LATENCY_UPPER_BOUNDS_MICROSECONDS = [
  1_000n, 10_000n, 50_000n, 200_000n, 1_000_000n, 5_000_000n, 30_000_000n,
] as const;

interface NamedHistogram {
  readonly description: string;
  readonly histogram: HECFixedHistogram | undefined;
  readonly name: string;
  readonly unit: string;
  readonly upperBounds: readonly bigint[];
}

function isUint64(value: unknown): value is bigint {
  return typeof value === "bigint" && value >= 0n && value <= UINT64_MAX;
}

function isOperationalDuration(
  value: { seconds: bigint; nanos: number } | undefined,
): boolean {
  return value === undefined || (
    typeof value.seconds === "bigint"
    && value.seconds >= 0n
    && value.seconds <= DURATION_MAX_SECONDS
    && Number.isInteger(value.nanos)
    && value.nanos >= 0
    && value.nanos <= 999_999_999
  );
}

function sameUint64Values(actual: readonly bigint[], expected: readonly bigint[]): boolean {
  return actual.length === expected.length
    && actual.every((value, index) => isUint64(value) && value === expected[index]);
}

function validUint64Values(values: readonly bigint[], expectedLength: number): boolean {
  return values.length === expectedLength && values.every(isUint64);
}

function validHistogram(
  histogram: HECFixedHistogram | undefined,
  upperBounds: readonly bigint[],
): boolean {
  return histogram !== undefined
    && sameUint64Values(histogram.upperBounds, upperBounds)
    && validUint64Values(histogram.bucketCounts, upperBounds.length + 1)
    && isUint64(histogram.count)
    && isUint64(histogram.sum)
    && isUint64(histogram.max);
}

function reconciliationHistograms(
  metrics: HECReconciliationOperationalMetrics,
): readonly NamedHistogram[] {
  return [
    {
      description: "Logical request batches represented by each durable write group.",
      histogram: metrics.memberBatchesPerGroup,
      name: "Member batches per group",
      unit: "batches",
      upperBounds: HEC_SHAPE_UPPER_BOUNDS,
    },
    {
      description: "Logical event rows represented by each durable write group.",
      histogram: metrics.rowsPerGroup,
      name: "Rows per group",
      unit: "rows",
      upperBounds: HEC_SHAPE_UPPER_BOUNDS,
    },
    {
      description: "Decoded payload bytes represented by each durable write group.",
      histogram: metrics.decodedBytesPerGroup,
      name: "Decoded bytes per group",
      unit: "bytes",
      upperBounds: HEC_BYTE_UPPER_BOUNDS,
    },
    {
      description: "Distinct monthly partitions represented by each durable write group.",
      histogram: metrics.monthlyPartitionsPerGroup,
      name: "Monthly partitions per group",
      unit: "partitions",
      upperBounds: HEC_SHAPE_UPPER_BOUNDS,
    },
    {
      description: "Rows included in each physical ClickHouse insert.",
      histogram: metrics.rowsPerPhysicalInsert,
      name: "Rows per physical insert",
      unit: "rows",
      upperBounds: HEC_SHAPE_UPPER_BOUNDS,
    },
  ];
}

function validSnapshotValues(snapshot: GetHECOperationalSnapshotResponse): boolean {
  if (
    snapshot.observedAt !== undefined
    && (!(snapshot.observedAt instanceof Date) || !Number.isFinite(snapshot.observedAt.valueOf()))
  ) return false;

  const request = snapshot.request;
  if (request !== undefined && (
    ![
      request.requests,
      request.acceptedRequests,
      request.events,
      request.uncompressedBytes,
      request.authenticationFailures,
      request.decodeFailures,
      request.eventPolicyFailures,
      request.rateLimitedRequests,
      request.stagingFailures,
      request.shutdownRejections,
    ].every(isUint64)
    || !isOperationalDuration(request.stagingDuration)
  )) return false;

  const durable = snapshot.durable;
  if (durable !== undefined && (
    ![
      durable.pendingOutboxReservations,
      durable.pendingOutboxBytes,
      durable.retainedRequests,
      durable.pendingMetadataBytes,
      durable.pendingUngrouped,
      durable.readyWriteGroups,
      durable.ambiguousWriteGroups,
      durable.liveWriteGroupLeases,
    ].every(isUint64)
    || !isOperationalDuration(durable.oldestPendingOutboxAge)
  )) return false;

  const reconciliation = snapshot.reconciliation;
  if (reconciliation !== undefined) {
    if (![
      reconciliation.successes,
      reconciliation.retries,
      reconciliation.ambiguities,
      reconciliation.stagedLogicalBatches,
      reconciliation.stagedLogicalRows,
      reconciliation.formedWriteGroups,
      reconciliation.physicalInsertSends,
      reconciliation.successfulWriteGroups,
      reconciliation.writeGroupMemberBatches,
      reconciliation.writeGroupRows,
      reconciliation.writeGroupDecodedBytes,
      reconciliation.writeGroupMonthlyPartitions,
      reconciliation.fillRowTarget,
      reconciliation.fillByteTarget,
      reconciliation.fillHardBoundary,
      reconciliation.fillLinger,
      reconciliation.fillDrain,
      reconciliation.fillRecovery,
      reconciliation.nativeWaiters,
      reconciliation.peakNativeWaiters,
      reconciliation.nativeWaiterWakeups,
      reconciliation.nativeWaiterCancellations,
      reconciliation.nativeTerminalLookups,
    ].every(isUint64)) return false;
  }

  const acknowledgments = snapshot.acknowledgments;
  if (acknowledgments !== undefined && ![
    acknowledgments.activeChannels,
    acknowledgments.retainedChannels,
    acknowledgments.pendingRows,
    acknowledgments.indexedRows,
    acknowledgments.expiredRows,
    acknowledgments.terminalFailedRequests,
    acknowledgments.queries,
    acknowledgments.idsQueried,
    acknowledgments.misses,
  ].every(isUint64)) return false;

  if (snapshot.protocolFailures.length > 27) return false;
  const protocolCodes = new Set<number>();
  for (const metric of snapshot.protocolFailures) {
    if (
      !Number.isInteger(metric.code)
      || metric.code < 1
      || metric.code > 27
      || protocolCodes.has(metric.code)
      || !isUint64(metric.count)
    ) return false;
    protocolCodes.add(metric.code);
  }
  return true;
}

function validReconciliationDistributions(
  reconciliation: HECReconciliationOperationalMetrics,
): boolean {
  return sameUint64Values(
    reconciliation.latencyUpperBoundsMicroseconds,
    HEC_LATENCY_UPPER_BOUNDS_MICROSECONDS,
  )
    && validUint64Values(reconciliation.sealLatencyBuckets, 8)
    && validUint64Values(reconciliation.sendLatencyBuckets, 8)
    && validUint64Values(reconciliation.commitLatencyBuckets, 8)
    && reconciliationHistograms(reconciliation).every(
      ({ histogram, upperBounds }) => validHistogram(histogram, upperBounds),
    );
}

function formatCount(value: bigint | undefined): string {
  return value === undefined ? "Not reported" : value.toLocaleString();
}

function formatAvailability(value: boolean | undefined): string {
  return value === undefined ? "Not reported" : value ? "Available" : "Unavailable";
}

function formatOperationalDuration(
  duration: { seconds: bigint; nanos: number } | undefined,
): string {
  if (duration === undefined) return "Not reported";
  if (duration.nanos === 0) return `${duration.seconds.toLocaleString()} seconds`;
  const fractional = duration.nanos.toString().padStart(9, "0").replace(/0+$/u, "");
  return `${duration.seconds.toLocaleString()}.${fractional} seconds`;
}

function MetricList({ metrics }: {
  metrics: ReadonlyArray<readonly [label: string, value: string]>;
}) {
  return (
    <dl className="backend-definition-list">
      {metrics.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}
    </dl>
  );
}

function bucketLabel(upperBounds: readonly bigint[], index: number, unit: string): string {
  const bound = upperBounds[index];
  if (bound === undefined) return `More than ${upperBounds.at(-1)?.toLocaleString()} ${unit}`;
  if (index === 0) return `0 through ${bound.toLocaleString()} ${unit}`;
  return `More than ${upperBounds[index - 1]?.toLocaleString()} through ${bound.toLocaleString()} ${unit}`;
}

function FixedHistogramTable({ description, histogram, name, unit, upperBounds }: NamedHistogram) {
  if (histogram === undefined) return null;
  return (
    <section className="suite-card settings-group" aria-label={name}>
      <header><h3>{name}</h3><p>{description}</p></header>
      <MetricList metrics={[
        ["Samples", formatCount(histogram.count)],
        ["Sum", formatCount(histogram.sum)],
        ["Maximum", formatCount(histogram.max)],
      ]} />
      <div className="table-wrap">
        <table className="table table--compact">
          <caption className="sr-only">{name}</caption>
          <thead><tr><th scope="col">Inclusive bucket</th><th scope="col">Samples</th></tr></thead>
          <tbody>{histogram.bucketCounts.map((count, index) => (
            <tr key={bucketLabel(upperBounds, index, unit)}>
              <td>{bucketLabel(upperBounds, index, unit)}</td>
              <td className="numeric-data">{formatCount(count)}</td>
            </tr>
          ))}</tbody>
        </table>
      </div>
    </section>
  );
}

function HECReconciliationPanels({ metrics }: {
  metrics: HECReconciliationOperationalMetrics | undefined;
}) {
  if (metrics === undefined) {
    return (
      <section className="suite-card settings-group" aria-labelledby="hec-coalescing-title">
        <header><h3 id="hec-coalescing-title">Write grouping and native waiters</h3><p>Coalescing metrics were not reported by this server.</p></header>
      </section>
    );
  }
  const histograms = reconciliationHistograms(metrics);
  const distributionsValid = validReconciliationDistributions(metrics);
  return (
    <>
      <section className="suite-card settings-group" aria-labelledby="hec-coalescing-title">
        <header><h3 id="hec-coalescing-title">Write grouping and native waiters</h3><p>Durable grouping, insert, fill-reason, and native backpressure totals.</p></header>
        <MetricList metrics={[
          ["Reconciliation", formatAvailability(metrics.available)],
          ["Reconciliation successes", formatCount(metrics.successes)],
          ["Reconciliation retries", formatCount(metrics.retries)],
          ["Reconciliation ambiguities", formatCount(metrics.ambiguities)],
          ["Staged logical batches", formatCount(metrics.stagedLogicalBatches)],
          ["Staged logical rows", formatCount(metrics.stagedLogicalRows)],
          ["Formed write groups", formatCount(metrics.formedWriteGroups)],
          ["Physical insert sends", formatCount(metrics.physicalInsertSends)],
          ["Successful write groups", formatCount(metrics.successfulWriteGroups)],
          ["Write-group member batches", formatCount(metrics.writeGroupMemberBatches)],
          ["Write-group rows", formatCount(metrics.writeGroupRows)],
          ["Write-group decoded bytes", formatCount(metrics.writeGroupDecodedBytes)],
          ["Write-group monthly partitions", formatCount(metrics.writeGroupMonthlyPartitions)],
          ["Row target", formatCount(metrics.fillRowTarget)],
          ["Byte target", formatCount(metrics.fillByteTarget)],
          ["Hard boundary", formatCount(metrics.fillHardBoundary)],
          ["Linger", formatCount(metrics.fillLinger)],
          ["Drain", formatCount(metrics.fillDrain)],
          ["Recovery", formatCount(metrics.fillRecovery)],
          ["Current native waiters", formatCount(metrics.nativeWaiters)],
          ["Peak native waiters", formatCount(metrics.peakNativeWaiters)],
          ["Native waiter wakeups", formatCount(metrics.nativeWaiterWakeups)],
          ["Native waiter cancellations", formatCount(metrics.nativeWaiterCancellations)],
          ["Native terminal lookups", formatCount(metrics.nativeTerminalLookups)],
        ]} />
      </section>
      {distributionsValid ? <><section className="suite-card settings-group" aria-label="Write-group latency distributions">
        <header><h3>Write-group latency</h3><p>Time from the oldest member reservation to seal, send, and commit.</p></header>
        <div className="table-wrap">
          <table className="table table--compact">
            <caption className="sr-only">Write-group seal, send, and commit latency buckets</caption>
            <thead><tr><th scope="col">Inclusive elapsed time</th><th scope="col">Seal samples</th><th scope="col">Send samples</th><th scope="col">Commit samples</th></tr></thead>
            <tbody>{metrics.sealLatencyBuckets.map((seal, index) => (
              <tr key={bucketLabel(HEC_LATENCY_UPPER_BOUNDS_MICROSECONDS, index, "microseconds")}>
                <td>{bucketLabel(HEC_LATENCY_UPPER_BOUNDS_MICROSECONDS, index, "microseconds")}</td>
                <td className="numeric-data">{formatCount(seal)}</td>
                <td className="numeric-data">{formatCount(metrics.sendLatencyBuckets[index])}</td>
                <td className="numeric-data">{formatCount(metrics.commitLatencyBuckets[index])}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      </section>
      {histograms.map((histogram) => <FixedHistogramTable {...histogram} key={histogram.name} />)}</> : (
        <section className="suite-card settings-group" role="alert">
          <header>
            <h3>HEC operational distributions are unavailable</h3>
            <p>The server returned values outside the fixed latency bounds and histogram shapes.</p>
          </header>
        </section>
      )}
    </>
  );
}

export function HECOperationalSnapshotPanel({ snapshot }: {
  snapshot: GetHECOperationalSnapshotResponse;
}) {
  if (!validSnapshotValues(snapshot)) {
    return (
      <section className="suite-card settings-group" role="alert">
        <header>
          <h3>HEC operational snapshot is unavailable</h3>
          <p>The server returned an invalid operational counter, duration, timestamp, or response code.</p>
        </header>
      </section>
    );
  }
  const request = snapshot.request;
  const durable = snapshot.durable;
  const acknowledgments = snapshot.acknowledgments;
  return (
    <>
      <section className="suite-card settings-group">
        <header><h3>HTTP Event Collector operations</h3><p>Process-wide counters observed {formatMediumDateTime(snapshot.observedAt, "Not reported")}.</p></header>
        <MetricList metrics={[
          ["Requests", formatCount(request?.requests)],
          ["Accepted requests", formatCount(request?.acceptedRequests)],
          ["Events", formatCount(request?.events)],
          ["Uncompressed bytes", formatCount(request?.uncompressedBytes)],
          ["Authentication failures", formatCount(request?.authenticationFailures)],
          ["Decode failures", formatCount(request?.decodeFailures)],
          ["Event-policy failures", formatCount(request?.eventPolicyFailures)],
          ["Rate-limited requests", formatCount(request?.rateLimitedRequests)],
          ["Staging failures", formatCount(request?.stagingFailures)],
          ["Staging duration", formatOperationalDuration(request?.stagingDuration)],
          ["Shutdown rejections", formatCount(request?.shutdownRejections)],
        ]} />
      </section>
      <section className="suite-card settings-group">
        <header><h3>Durable queue state</h3><p>Queue capacity and pending durable work.</p></header>
        <MetricList metrics={[
          ["Durable queue", formatAvailability(durable?.queueAvailable)],
          ["Request capacity", formatAvailability(durable?.requestCapacityAvailable)],
          ["Pending outbox reservations", formatCount(durable?.pendingOutboxReservations)],
          ["Pending outbox bytes", formatCount(durable?.pendingOutboxBytes)],
          ["Pending metadata bytes", formatCount(durable?.pendingMetadataBytes)],
          ["Pending ungrouped requests", formatCount(durable?.pendingUngrouped)],
          ["Ready write groups", formatCount(durable?.readyWriteGroups)],
          ["Ambiguous write groups", formatCount(durable?.ambiguousWriteGroups)],
          ["Live write-group leases", formatCount(durable?.liveWriteGroupLeases)],
          ["Oldest pending age", formatOperationalDuration(durable?.oldestPendingOutboxAge)],
          ["Retained requests", formatCount(durable?.retainedRequests)],
        ]} />
      </section>
      <HECReconciliationPanels metrics={snapshot.reconciliation} />
      <section className="suite-card settings-group">
        <header><h3>Indexer acknowledgment</h3><p>Channel retention, terminal outcomes, and query totals.</p></header>
        <MetricList metrics={[
          ["ACK service", formatAvailability(acknowledgments?.available)],
          ["Active ACK channels", formatCount(acknowledgments?.activeChannels)],
          ["Retained ACK channels", formatCount(acknowledgments?.retainedChannels)],
          ["Pending ACK rows", formatCount(acknowledgments?.pendingRows)],
          ["Indexed ACK rows", formatCount(acknowledgments?.indexedRows)],
          ["Expired ACK rows", formatCount(acknowledgments?.expiredRows)],
          ["Terminal failed requests", formatCount(acknowledgments?.terminalFailedRequests)],
          ["ACK queries", formatCount(acknowledgments?.queries)],
          ["ACK IDs queried", formatCount(acknowledgments?.idsQueried)],
          ["ACK query misses", formatCount(acknowledgments?.misses)],
        ]} />
      </section>
      <section className="suite-card settings-group">
        <header><h3>HEC protocol failures</h3><p>Bounded non-success response codes reported by the HEC compatibility layer.</p></header>
        {snapshot.protocolFailures.length === 0 ? <p className="settings-group__empty">No protocol failures have been observed.</p> : (
          <MetricList metrics={snapshot.protocolFailures.map((metric) => [
            `Response code ${metric.code}`,
            formatCount(metric.count),
          ])} />
        )}
      </section>
    </>
  );
}
