/** Converts a protobuf Duration into non-negative milliseconds. */
export function durationToMilliseconds(duration: { seconds: bigint; nanos: number } | undefined): number {
  if (duration === undefined) return 0;
  const milliseconds = Number(duration.seconds) * 1_000 + duration.nanos / 1_000_000;
  return Number.isFinite(milliseconds) && milliseconds >= 0 ? milliseconds : 0;
}

/** Formats an elapsed-millisecond count the way the activity and history surfaces do. */
export function formatDurationMilliseconds(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "0 ms";
  if (milliseconds < 1_000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1_000).toFixed(milliseconds < 10_000 ? 2 : 1)} s`;
}

/** Copies a server timestamp when it is present and parsed, otherwise reports null. */
export function validDate(value: Date | undefined): Date | null {
  return value !== undefined && !Number.isNaN(value.valueOf()) ? new Date(value) : null;
}
