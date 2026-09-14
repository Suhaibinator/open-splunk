import type { Metadata } from "next";

import { ReportsPageContent } from "./reports-page";

export const metadata: Metadata = { title: "Reports" };

export default function ReportsPage() {
  return <ReportsPageContent canonicalizeParent initialView="saved-searches" />;
}
