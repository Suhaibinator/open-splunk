import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { ReportsPageContent } from "../reports-page";
import { isReportsView, REPORT_VIEWS } from "../reports-view-state";

export const metadata: Metadata = { title: "Reports" };
export const dynamicParams = false;

export function generateStaticParams() {
  return REPORT_VIEWS.map((view) => ({ view }));
}

export default async function ReportsViewPage({ params }: { params: Promise<{ view: string }> }) {
  const { view } = await params;
  if (!isReportsView(view)) notFound();
  return <ReportsPageContent initialView={view} />;
}
