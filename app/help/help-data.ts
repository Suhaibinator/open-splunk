import generatedContent from "./help-content.generated.json";

import type { HelpContentBundle, HelpDocument } from "@/lib/help/help-content";

export const HELP_CONTENT = generatedContent as HelpContentBundle;
export const HELP_DOCUMENTS: readonly HelpDocument[] = HELP_CONTENT.documents;

export function helpDocumentForSlug(slug: string): HelpDocument | undefined {
  return HELP_DOCUMENTS.find((document) => document.slug === slug);
}
