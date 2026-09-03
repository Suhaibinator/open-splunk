import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { ReportsConsole } from "./reports-console";
import type { ReportsView } from "./reports-view-state";

interface ReportsPageContentProps {
  canonicalizeParent?: boolean;
  initialView: ReportsView;
}

export function ReportsPageContent({ canonicalizeParent = false, initialView }: ReportsPageContentProps) {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="reports" apiBaseUrl={apiBaseUrl} appName="Search & Reporting" dataMode={dataMode}>
      <ReportsConsole
        apiBaseUrl={apiBaseUrl}
        canonicalizeParent={canonicalizeParent}
        dataMode={dataMode}
        initialView={initialView}
      />
    </ProductShell>
  );
}
