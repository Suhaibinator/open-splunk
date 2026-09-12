"use client";

import { useId, useState } from "react";
import { Select, SelectOption } from "../../_components/select";
import {
  NEARBY_OPERATORS,
  nearbySearch,
  type NearbyComparison,
  type NearbyDraft,
  type NearbyOperator,
  type NearbyScalarKind,
} from "@/lib/search/nearby-events";

interface NearbyContextEditorProps {
  draft: NearbyDraft;
  onChange: (draft: NearbyDraft) => void;
  onApply: (draft: NearbyDraft) => void;
  onDetach: () => void;
  busy?: boolean;
}

/** Changes stay in this controlled draft until one Apply starts a new search. */
export function NearbyContextEditor({ draft, onChange, onApply, onDetach, busy = false }: NearbyContextEditorProps) {
  const id = useId();
  const [selected, setSelected] = useState(0);
  const comparison = draft.comparisons[selected];
  let validationError: string | null = null;
  try { nearbySearch(draft); } catch (error) { validationError = error instanceof Error ? error.message : "Check the context comparisons."; }
  const updateComparison = (patch: Partial<NearbyComparison>) => onChange({
    ...draft,
    comparisons: draft.comparisons.map((item, index) => index === selected ? { ...item, ...patch } : item),
  });
  return (
    <section className="nearby-context" aria-label="Nearby event context">
      <div className="nearby-context-heading">
        <strong>Nearby events</strong>
        <span>Anchor: {draft.anchorTime}</span>
        <button className="button button--ghost button--compact" type="button" onClick={onDetach}>Use full SPL editor</button>
      </div>
      <p>Started with the same index, host and source, five minutes either side of the event. Edit comparisons and apply them together.</p>
      {draft.clipped && <p className="status status--warning" role="status">The nearby interval was clipped to the supported 1900–2262 timestamp range.</p>}
      <div className="nearby-context-chips" aria-label="Context comparisons">
        {draft.comparisons.map((item, index) => (
          <button className="button button--secondary button--compact" type="button" key={item.id} aria-pressed={selected === index} data-enabled={item.enabled} onClick={() => setSelected(index)}>
            {item.enabled ? "✓ " : "+ "}{item.field || "New field"} {item.operator} {item.scalar.value || '""'}
          </button>
        ))}
        <button className="button button--ghost button--compact" type="button" onClick={() => {
          setSelected(draft.comparisons.length);
          onChange({ ...draft, comparisons: [...draft.comparisons, { id: `${id}-${draft.comparisons.length}`, field: "", operator: "=", scalar: { kind: "string", value: "" }, enabled: true }] });
        }}>Add comparison</button>
      </div>
      {comparison && <div className="nearby-context-comparison form-stack">
        <label><span>Field</span><input aria-label="Context field" value={comparison.field} onChange={(event) => updateComparison({ field: event.target.value })} /></label>
        <label htmlFor={`${id}-operator-select`}><span id={`${id}-operator`}>Comparison</span><Select id={`${id}-operator-select`} aria-labelledby={`${id}-operator`} value={comparison.operator} onValueChange={(value) => updateComparison({ operator: value as NearbyOperator })}>
          {NEARBY_OPERATORS.map((operator) => <SelectOption key={operator} value={operator} disabled={comparison.scalar.kind === "boolean" && operator !== "=" && operator !== "!="}>{operator}</SelectOption>)}
        </Select></label>
        <label htmlFor={`${id}-type-select`}><span id={`${id}-type`}>Value type</span><Select id={`${id}-type-select`} aria-labelledby={`${id}-type`} value={comparison.scalar.kind} onValueChange={(value) => updateComparison({ scalar: { ...comparison.scalar, kind: value as NearbyScalarKind } })}>
          <SelectOption value="string">Text</SelectOption><SelectOption value="number">Number</SelectOption><SelectOption value="boolean">Boolean</SelectOption>
        </Select></label>
        <label><span>Value</span><textarea rows={2} aria-label="Context value" value={comparison.scalar.value} onChange={(event) => updateComparison({ scalar: { ...comparison.scalar, value: event.target.value } })} /></label>
        <label><span>Enabled</span><input type="checkbox" aria-label="Enable context comparison" checked={comparison.enabled} onChange={(event) => updateComparison({ enabled: event.target.checked })} /></label>
      </div>}
      <div className="nearby-context-range form-stack">
        <label><span>Earliest (inclusive)</span><input aria-label="Nearby earliest time" value={draft.earliest} onChange={(event) => onChange({ ...draft, earliest: event.target.value })} /></label>
        <label><span>Latest (exclusive)</span><input aria-label="Nearby latest time" value={draft.latest} onChange={(event) => onChange({ ...draft, latest: event.target.value })} /></label>
      </div>
      {validationError && <p className="status status--warning" role="status">{validationError}</p>}
      <button className="button button--primary button--compact" type="button" disabled={busy || validationError !== null} onClick={() => onApply(draft)}>Apply context</button>
    </section>
  );
}
