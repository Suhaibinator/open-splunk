export interface AnalyticsSampleStatusProps {
  complete: boolean;
  loaded: number;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  className?: string;
}

export function analyticsSampleStatusLabel({
  complete,
  loaded,
  totalSize,
  totalSizeExact,
}: Omit<AnalyticsSampleStatusProps, "className">): string {
  if (!complete) {
    return totalSizeExact && totalSize !== null
      ? `Partial sample · ${loaded.toLocaleString()} of ${totalSize.toLocaleString()}`
      : `Partial sample · ${loaded.toLocaleString()} loaded`;
  }
  return `Live data · ${loaded.toLocaleString()} ${loaded === 1 ? "search" : "searches"}`;
}

export function AnalyticsSampleStatus(props: AnalyticsSampleStatusProps) {
  return (
    <span className={props.className} data-complete={props.complete} data-testid="analytics-sample-status">
      {analyticsSampleStatusLabel(props)}
    </span>
  );
}
