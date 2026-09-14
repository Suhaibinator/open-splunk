import type { Metadata } from "next";

import { ActivityPageContent } from "./activity-page";

export const metadata: Metadata = { title: "Activity" };

export default function ActivityPage() {
  return <ActivityPageContent canonicalizeParent initialView="jobs" />;
}
