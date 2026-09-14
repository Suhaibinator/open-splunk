import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";
import type { SearchResultView } from "@/lib/search/result-view-navigation";

import { SearchWorkspace } from "../search-workspace";

interface SearchPageContentProps {
  canonicalizeParent?: boolean;
  initialResultView: SearchResultView;
}

export function SearchPageContent({ canonicalizeParent = false, initialResultView }: SearchPageContentProps) {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <SearchWorkspace
      apiBaseUrl={apiBaseUrl}
      canonicalizeParent={canonicalizeParent}
      dataMode={dataMode}
      initialResultView={initialResultView}
    />
  );
}
