import { SearchWorkspace } from "./search-workspace";

export default function HomePage() {
  const dataMode = process.env.OPEN_SPLUNK_DATA_MODE === "backend" ? "backend" : "demo";
  return <SearchWorkspace dataMode={dataMode} apiBaseUrl={process.env.OPEN_SPLUNK_API_BASE_URL ?? ""} />;
}
