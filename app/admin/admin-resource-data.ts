import type { AppDefinition, AppWorkspace } from "@/gen/ts/open_splunk/app";

export interface AppFormState {
  slug: string;
  displayName: string;
  description: string;
  indexNames: string;
  hasTimeRange: boolean;
  earliest: string;
  latest: string;
  timezone: string;
}

export function blankAppForm(): AppFormState {
  return {
    slug: "",
    displayName: "",
    description: "",
    indexNames: "",
    hasTimeRange: false,
    earliest: "-24h",
    latest: "now",
    timezone: "UTC",
  };
}

export function appForm(app: AppWorkspace): AppFormState {
  const definition = app.definition;
  return {
    slug: definition?.slug ?? "",
    displayName: definition?.displayName ?? "",
    description: definition?.description ?? "",
    indexNames: definition?.defaultIndexNames.join(", ") ?? "",
    hasTimeRange: definition?.defaultTimeRange !== undefined,
    earliest: definition?.defaultTimeRange?.earliest ?? "-24h",
    latest: definition?.defaultTimeRange?.latest ?? "now",
    timezone: definition?.defaultTimeRange?.timezone ?? "UTC",
  };
}

export function splitIndexNames(value: string): string[] {
  return [...new Set(value.split(",").map((item) => item.trim()).filter(Boolean))].toSorted();
}

export function definitionFromForm(form: AppFormState): AppDefinition {
  return {
    slug: form.slug.trim().toLowerCase(),
    displayName: form.displayName.trim(),
    description: form.description.trim() || undefined,
    defaultIndexNames: splitIndexNames(form.indexNames),
    defaultTimeRange: form.hasTimeRange ? {
      earliest: form.earliest.trim() || undefined,
      latest: form.latest.trim() || undefined,
      timezone: form.timezone.trim() || undefined,
    } : undefined,
  };
}

export function sameStrings(left: string[], right: string[]): boolean {
  const a = [...left].toSorted();
  const b = [...right].toSorted();
  return a.length === b.length && a.every((value, index) => value === b[index]);
}
