import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { isActivityView, ACTIVITY_VIEWS } from "../activity-navigation";
import { ActivityPageContent } from "../activity-page";

export const metadata: Metadata = { title: "Activity" };
export const dynamicParams = false;

export function generateStaticParams() {
  return ACTIVITY_VIEWS.map((view) => ({ view }));
}

export default async function ActivityViewPage({ params }: { params: Promise<{ view: string }> }) {
  const { view } = await params;
  if (!isActivityView(view)) notFound();
  return <ActivityPageContent initialView={view} />;
}
