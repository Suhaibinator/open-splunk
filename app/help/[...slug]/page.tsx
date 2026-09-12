import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getFrontendRuntimeConfig } from "@/lib/frontend-runtime-config";

import { HelpBrowser } from "../help-browser";
import { HELP_DOCUMENTS, helpDocumentForSlug } from "../help-data";

export const dynamicParams = false;

export function generateStaticParams() {
  return HELP_DOCUMENTS
    .filter((document) => document.slug !== "")
    .map((document) => ({ slug: document.slug.split("/") }));
}

export async function generateMetadata({ params }: { params: Promise<{ slug: string[] }> }): Promise<Metadata> {
  const { slug } = await params;
  return { title: helpDocumentForSlug(slug.join("/"))?.title ?? "Documentation" };
}

export default async function HelpDocumentPage({ params }: { params: Promise<{ slug: string[] }> }) {
  const { slug } = await params;
  const documentSlug = slug.join("/");
  if (helpDocumentForSlug(documentSlug) === undefined) notFound();
  const { apiBaseUrl, dataMode } = getFrontendRuntimeConfig();
  return <HelpBrowser apiBaseUrl={apiBaseUrl} dataMode={dataMode} initialSlug={documentSlug} />;
}
