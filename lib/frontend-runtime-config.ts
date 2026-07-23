import "server-only";

import type { SearchDataMode } from "@/lib/search/backend-data";

export interface FrontendRuntimeConfig {
  dataMode: SearchDataMode;
  apiBaseUrl: string;
}

/**
 * Reads the server-owned frontend runtime switches.
 *
 * Keeping this in a server-only module prevents browser state from silently
 * overriding whether the UI uses demo fixtures or the connected backend.
 */
export function getFrontendRuntimeConfig(): FrontendRuntimeConfig {
  return {
    dataMode: process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo",
    apiBaseUrl: process.env.OPEN_SPLUNK_API_BASE_URL ?? "",
  };
}
