export const OPEN_SPLUNK_SOURCE_REVISION =
  process.env.NEXT_PUBLIC_OPEN_SPLUNK_SOURCE_REVISION ?? "development";

export const OPEN_SPLUNK_BUILD_LABEL = OPEN_SPLUNK_SOURCE_REVISION === "development"
  ? "development"
  : `rev ${OPEN_SPLUNK_SOURCE_REVISION.slice(0, 12)}`;
