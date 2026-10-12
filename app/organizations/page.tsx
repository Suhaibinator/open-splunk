import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";
import { ProductShell } from "../_components/product-shell";
import { OrganizationAccess } from "./organization-access";

export default function OrganizationsPage() {
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return <ProductShell activeSection="admin" apiBaseUrl={apiBaseUrl} appName="Organization access" dataMode={dataMode}><OrganizationAccess apiBaseUrl={apiBaseUrl} /></ProductShell>;
}
