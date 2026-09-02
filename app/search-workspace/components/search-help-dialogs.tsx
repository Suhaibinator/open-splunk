import { useId, useMemo, useState } from "react";

import { AppIcon } from "../../_components/app-icon";
import { Modal } from "../../_components/modal";

import {
  SPL_REFERENCE_SECTIONS,
  type SplReferenceEntry,
  type SplReferenceSection,
  filterSplReference,
} from "../spl-reference-data";

// The Help menu's dialogs. They read static tables (the shared completion
// catalog, the shortcut table, the example drafts) and hand one intent back to
// the workspace -- insert this, use that -- so the workspace keeps ownership
// of the editor and the draft.

export interface SplReferenceDialogProps {
  onClose: () => void;
  /** Adds one catalog entry to the editor; the workspace decides where. */
  onInsert: (entry: SplReferenceEntry) => void;
  returnFocus?: HTMLElement | null;
  sections?: readonly SplReferenceSection[];
}

const REFERENCE_FILTER_ID = "spl-reference-filter";
const REFERENCE_SECTIONS_ID = "spl-reference-sections";

function sectionDomId(section: SplReferenceSection): string {
  return `spl-reference-${section.id}`;
}

function ReferenceEntry({ entry, onInsert }: { entry: SplReferenceEntry; onInsert: (entry: SplReferenceEntry) => void }) {
  const prose = entry.documentation ?? entry.detail;
  const usage = entry.syntax ?? entry.insertion;
  return (
    <li className="workspace-dialog-reference-entry" data-supported={entry.supported ? "true" : "false"} id={`spl-reference-${entry.id}`}>
      <div className="workspace-dialog-reference-entry-head">
        <code className="workspace-dialog-reference-name">{entry.name}</code>
        {entry.supported && entry.insertion !== null
          ? <button className="button button--ghost button--compact" type="button" aria-label={`Insert ${entry.name}`} onClick={() => onInsert(entry)}>Insert</button>
          : <span className="badge badge--warning">Not supported</span>}
      </div>
      {usage !== null
        ? <pre className="workspace-dialog-reference-syntax"><code>{usage}</code></pre>
        : null}
      <p className="workspace-dialog-reference-prose">{prose}</p>
      {entry.documentation !== undefined && entry.detail !== entry.documentation
        ? <details className="workspace-dialog-reference-contract"><summary>Supported behaviour</summary><p>{entry.detail}</p></details>
        : null}
    </li>
  );
}

export function SplReferenceDialog({
  onClose,
  onInsert,
  returnFocus = null,
  sections = SPL_REFERENCE_SECTIONS,
}: SplReferenceDialogProps) {
  const [filter, setFilter] = useState("");
  const headingPrefix = useId();
  const visible = useMemo(() => filterSplReference(sections, filter), [filter, sections]);
  const entryCount = visible.reduce((total, section) => total + section.entries.length, 0);

  function scrollToSection(section: SplReferenceSection) {
    document.getElementById(sectionDomId(section))?.scrollIntoView({ block: "start" });
  }

  return (
    <Modal
      title="SPL reference"
      subtitle="Every command, function and keyword this server accepts, from the same catalog the editor completes."
      wide
      onClose={onClose}
      returnFocus={returnFocus}
      initialFocus={`#${REFERENCE_FILTER_ID}`}
    >
      <div className="library-toolbar">
        <label className="filter-input">
          <span aria-hidden="true"><AppIcon name="search" size="sm" /></span>
          <input
            id={REFERENCE_FILTER_ID}
            type="search"
            aria-controls={REFERENCE_SECTIONS_ID}
            aria-label="Filter the SPL reference"
            placeholder="Filter by name or description"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
        </label>
        <output className="workspace-dialog-reference-count" htmlFor={REFERENCE_FILTER_ID}>
          {entryCount} {entryCount === 1 ? "entry" : "entries"}
        </output>
      </div>
      <div className="workspace-dialog-reference" data-testid="spl-reference">
        <nav className="workspace-dialog-reference-nav" aria-label="Reference sections">
          <ul>
            {visible.map((section) => (
              <li key={section.id}>
                <button type="button" aria-controls={sectionDomId(section)} onClick={() => scrollToSection(section)}>
                  <span>{section.title}</span>
                  <span className="workspace-dialog-reference-nav-count">{section.entries.length}</span>
                </button>
              </li>
            ))}
          </ul>
        </nav>
        <div className="workspace-dialog-reference-sections" id={REFERENCE_SECTIONS_ID}>
          {visible.length === 0
            ? <p className="empty-state" role="status"><strong>No entries match</strong><span>Try a command name, a function, or a word from a description.</span></p>
            : visible.map((section) => (
              <section className="workspace-dialog-reference-section" id={sectionDomId(section)} aria-labelledby={`${headingPrefix}-${section.id}`} key={section.id}>
                <h3 id={`${headingPrefix}-${section.id}`}>{section.title}</h3>
                <p className="workspace-dialog-reference-summary">{section.summary}</p>
                <ul className="workspace-dialog-reference-list">
                  {section.entries.map((entry) => <ReferenceEntry entry={entry} onInsert={onInsert} key={entry.id} />)}
                </ul>
              </section>
            ))}
        </div>
      </div>
    </Modal>
  );
}
