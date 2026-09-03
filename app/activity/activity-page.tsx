import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { ProductShell } from "../_components/product-shell";
import { ActivityConsole } from "./activity-console";
import type { ActivityView } from "./activity-navigation";

interface ActivityPageContentProps {
  canonicalizeParent?: boolean;
  initialView: ActivityView;
}

export function ActivityPageContent({ canonicalizeParent = false, initialView }: ActivityPageContentProps) {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return (
    <ProductShell activeSection="activity" apiBaseUrl={apiBaseUrl} appName="Activity" dataMode={dataMode}>
      <ActivityConsole
        canonicalizeParent={canonicalizeParent}
        dataMode={dataMode}
        initialView={initialView}
        apiBaseUrl={apiBaseUrl}
      />
    </ProductShell>
  );
}
