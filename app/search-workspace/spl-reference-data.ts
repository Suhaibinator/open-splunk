import { type CompletionKind, getQueryDiagnostic } from "@/lib/search/spl-editor";
import {
  SPL_FUNCTIONS,
  SPL_KEYWORDS,
  SPL_PIPELINE_COMMANDS,
  UNSUPPORTED_SPL_PIPELINE_COMMANDS,
} from "@/lib/search/spl-syntax";

// The SPL reference pane is generated from the completion catalog rather than
// written by hand, so the pane, the completion popup and the server agree on
// what exists. Everything here is derived once at module load and filtered on
// demand; nothing is fetched.

export type SplReferenceSectionId =
  | "commands"
  | "aggregate-functions"
  | "scalar-functions"
  | "keywords"
  | "unsupported";

export interface SplReferenceEntry {
  /** Unique across every section; the DOM id the section list points at. */
  readonly id: string;
  readonly name: string;
  readonly section: SplReferenceSectionId;
  readonly kind: CompletionKind;
  readonly detail: string;
  readonly syntax?: string;
  readonly documentation?: string;
  /** What Insert adds to the editor, or null for entries the pipeline refuses. */
  readonly insertion: string | null;
  readonly supported: boolean;
}

export interface SplReferenceSection {
  readonly id: SplReferenceSectionId;
  readonly title: string;
  readonly summary: string;
  readonly entries: readonly SplReferenceEntry[];
}

function unsupportedEntry(name: string): SplReferenceEntry {
  // The editor's own diagnostic is the one source of the "use this instead"
  // wording, so the pane and the inline problem list never disagree.
  const diagnostic = getQueryDiagnostic(`* | ${name}`);
  return {
    id: `unsupported-${name}`,
    name,
    section: "unsupported",
    kind: "command",
    detail: diagnostic?.suggestion ?? "Not supported by Open Splunk.",
    insertion: null,
    supported: false,
  };
}

export const SPL_REFERENCE_SECTIONS: readonly SplReferenceSection[] = [
  {
    id: "commands",
    title: "Commands",
    summary: "Pipeline stages, in the order the completion menu lists them.",
    entries: SPL_PIPELINE_COMMANDS.map((command) => ({
      id: `command-${command.name}`,
      name: command.name,
      section: "commands",
      kind: "command",
      detail: command.detail,
      syntax: command.syntax,
      documentation: command.documentation,
      insertion: command.insertion,
      supported: true,
    })),
  },
  {
    id: "aggregate-functions",
    title: "Aggregate functions",
    summary: "Measures for stats, eventstats, streamstats, timechart and chart.",
    entries: SPL_FUNCTIONS.filter((fn) => fn.class === "aggregate").map((fn) => ({
      id: `aggregate-${fn.name}`,
      name: fn.name,
      section: "aggregate-functions",
      kind: "function",
      detail: fn.detail,
      insertion: fn.insertion,
      supported: true,
    })),
  },
  {
    id: "scalar-functions",
    title: "Scalar functions",
    summary: "Expressions for eval and where.",
    entries: SPL_FUNCTIONS.filter((fn) => fn.class === "scalar").map((fn) => ({
      id: `scalar-${fn.name}`,
      name: fn.name,
      section: "scalar-functions",
      kind: "function",
      detail: fn.detail,
      insertion: fn.insertion,
      supported: true,
    })),
  },
  {
    id: "keywords",
    title: "Keywords and options",
    summary: "Operators, clauses and the named options commands accept.",
    entries: SPL_KEYWORDS.map((keyword) => ({
      id: `keyword-${keyword.name.replace(/=$/u, "")}`,
      name: keyword.name,
      section: "keywords",
      kind: "keyword",
      detail: keyword.detail,
      insertion: keyword.insertion,
      supported: true,
    })),
  },
  {
    id: "unsupported",
    title: "Not supported",
    summary: "Commands the editor flags; each names what to use instead.",
    entries: UNSUPPORTED_SPL_PIPELINE_COMMANDS.map(unsupportedEntry),
  },
];

function normalise(value: string): string {
  return value.toLocaleLowerCase("en-US");
}

/** Whether one entry matches a filter: its name, detail, syntax or documentation. */
export function referenceEntryMatches(entry: SplReferenceEntry, filter: string): boolean {
  const needle = normalise(filter.trim());
  if (needle.length === 0) return true;
  return [entry.name, entry.detail, entry.syntax ?? "", entry.documentation ?? ""]
    .some((text) => normalise(text).includes(needle));
}

/** The sections that still have an entry after filtering, with only those entries. */
export function filterSplReference(
  sections: readonly SplReferenceSection[],
  filter: string,
): SplReferenceSection[] {
  return sections
    .map((section) => ({ ...section, entries: section.entries.filter((entry) => referenceEntryMatches(entry, filter)) }))
    .filter((section) => section.entries.length > 0);
}
