import type { StatusTone } from "@/app/_components/status";
import { SearchJobState } from "@/gen/ts/open_splunk/search";

export interface HomeSearchStatus {
  label: string;
  tone: StatusTone;
}

export function homeSearchStatus(state: SearchJobState): HomeSearchStatus {
  if (state === SearchJobState.SEARCH_JOB_STATE_COMPLETED) {
    return { label: "Completed", tone: "success" };
  }
  if (state === SearchJobState.SEARCH_JOB_STATE_CANCELED) {
    return { label: "Canceled", tone: "neutral" };
  }
  if (state === SearchJobState.SEARCH_JOB_STATE_EXPIRED) {
    return { label: "Expired", tone: "error" };
  }
  if (state === SearchJobState.SEARCH_JOB_STATE_FAILED) {
    return { label: "Failed", tone: "error" };
  }
  return { label: "Unknown", tone: "neutral" };
}

export function homeSearchFinishedAt(
  entry: { finishedAt: Date | null; createdAt: Date | null },
): Date | null {
  return entry.finishedAt ?? entry.createdAt;
}
