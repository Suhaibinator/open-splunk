import type { Metadata } from "next";

import { SearchPageContent } from "./search-page";

export const metadata: Metadata = { title: "Search & Reporting" };

export default function SearchPage() {
  return <SearchPageContent canonicalizeParent initialResultView="events" />;
}
