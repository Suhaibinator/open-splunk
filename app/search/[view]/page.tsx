import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { isSearchResultView, SEARCH_RESULT_VIEWS } from "@/lib/search/result-view-navigation";

import { SearchPageContent } from "../search-page";

export const metadata: Metadata = { title: "Search & Reporting" };
export const dynamicParams = false;

export function generateStaticParams() {
  return SEARCH_RESULT_VIEWS.map((view) => ({ view }));
}

export default async function SearchResultViewPage({ params }: { params: Promise<{ view: string }> }) {
  const { view } = await params;
  if (!isSearchResultView(view)) notFound();
  return <SearchPageContent initialResultView={view} />;
}
