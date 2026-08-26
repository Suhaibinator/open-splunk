"use client";

import { useEffect, useId, useRef, useState } from "react";

import { SharingScope } from "@/gen/ts/open_splunk/common";
import {
  CalculatedFieldDefinition,
  FieldAliasDefinition,
  FieldExtractionDefinition,
  KnowledgeObject,
  KnowledgeObjectDefinition,
  KnowledgeObjectState,
  KnowledgeOverwriteBehavior,
  KnowledgeSelector,
  KnowledgeSelectorMatchKind,
  type KnowledgeObjectDefinition as KnowledgeObjectDefinitionMessage,
} from "@/gen/ts/open_splunk/knowledge";
import {
  CreateKnowledgeObjectRequest,
  DeleteKnowledgeObjectRequest,
  KnowledgeValidationIntent,
  PrepareKnowledgeObjectQuarantineRequest,
  QuarantineKnowledgeObjectRequest,
  SetKnowledgeObjectStateRequest,
  UpdateKnowledgeObjectRequest,
  ValidateKnowledgeObjectRequest,
} from "@/gen/ts/open_splunk/knowledge_api";

import { joinedPatterns, lines } from "./knowledge-lookup-text";
import type { KnowledgeManagerAppOption } from "./knowledge-manager-feature";
import {
  createKnowledgeObject,
  deleteKnowledgeObject,
  prepareKnowledgeObjectQuarantine,
  quarantineKnowledgeObject,
  setKnowledgeObjectState,
  updateKnowledgeObject,
  validateKnowledgeObject,
  type KnowledgeMutationClient,
  type KnowledgeQuarantinePreparation,
  type KnowledgeValidationReceipt,
} from "./knowledge-manager-data";

export type KnowledgeTierOneDefinitionKind =
  | "regex-extraction"
  | "json-extraction"
  | "field-alias"
  | "calculated-field";

export type KnowledgeMutationSharingScope = "private" | "app" | "global";
export type KnowledgeMutationOverwrite = "preserve" | "replace";

export interface KnowledgeMutationDraft {
  kind: KnowledgeTierOneDefinitionKind;
  appId: string;
  name: string;
  description: string;
  sharingScope: KnowledgeMutationSharingScope;
  indexPatterns: string;
  hostPatterns: string;
  sourcePatterns: string;
  sourcetypePatterns: string;
  regexPattern: string;
  regexOutputFields: string;
  jsonPath: string;
  jsonOutputField: string;
  aliasSourceField: string;
  aliasDestinationField: string;
  calculatedDestinationField: string;
  calculatedExpression: string;
  overwrite: KnowledgeMutationOverwrite;
}

const TIER_ONE_KIND_OPTIONS = [
  { value: "regex-extraction", label: "Regex extraction" },
  { value: "json-extraction", label: "JSON extraction" },
  { value: "field-alias", label: "Field alias" },
  { value: "calculated-field", label: "Calculated field" },
] as const satisfies ReadonlyArray<{
  value: KnowledgeTierOneDefinitionKind;
  label: string;
}>;

function emptyDraft(appId: string): KnowledgeMutationDraft {
  return {
    kind: "regex-extraction",
    appId,
    name: "",
    description: "",
    sharingScope: "app",
    indexPatterns: "",
    hostPatterns: "",
    sourcePatterns: "",
    sourcetypePatterns: "",
    regexPattern: "",
    regexOutputFields: "",
    jsonPath: "",
    jsonOutputField: "",
    aliasSourceField: "",
    aliasDestinationField: "",
    calculatedDestinationField: "",
    calculatedExpression: "",
    overwrite: "preserve",
  };
}

export function createKnowledgeMutationDraft(appId: string): KnowledgeMutationDraft {
  return emptyDraft(appId);
}

function sharingScopeFromDraft(value: KnowledgeMutationSharingScope): SharingScope {
  switch (value) {
    case "private": return SharingScope.SHARING_SCOPE_PRIVATE;
    case "app": return SharingScope.SHARING_SCOPE_APP;
    case "global": return SharingScope.SHARING_SCOPE_GLOBAL;
    default: throw new TypeError("Knowledge sharing scope is outside Tier 1.");
  }
}

function sharingScopeToDraft(value: SharingScope): KnowledgeMutationSharingScope | null {
  switch (value) {
    case SharingScope.SHARING_SCOPE_PRIVATE: return "private";
    case SharingScope.SHARING_SCOPE_APP: return "app";
    case SharingScope.SHARING_SCOPE_GLOBAL: return "global";
    default: return null;
  }
}

function overwriteFromDraft(value: KnowledgeMutationOverwrite): KnowledgeOverwriteBehavior {
  switch (value) {
    case "preserve":
      return KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING;
    case "replace":
      return KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING;
    default:
      throw new TypeError("Knowledge overwrite behavior is outside Tier 1.");
  }
}

function overwriteToDraft(value: KnowledgeOverwriteBehavior): KnowledgeMutationOverwrite | null {
  switch (value) {
    case KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING:
      return "preserve";
    case KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING:
      return "replace";
    default:
      return null;
  }
}

function selectorPatterns(value: string) {
  return lines(value).map((pattern) => ({
    matchKind: selectorMatchKind(pattern),
    value: pattern,
  }));
}

function selectorMatchKind(pattern: string): KnowledgeSelectorMatchKind {
  let escaped = false;
  let wildcard = false;
  for (const character of pattern) {
    if (escaped) {
      if (character !== "*" && character !== "?" && character !== "\\") {
        return KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED;
      }
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (character === "*" || character === "?") wildcard = true;
  }
  if (escaped) return KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED;
  return wildcard
    ? KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
    : KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT;
}

export function knowledgeDefinitionFromMutationDraft(
  draft: KnowledgeMutationDraft,
): KnowledgeObjectDefinitionMessage {
  const selector = KnowledgeSelector.fromPartial({
    indexPatterns: selectorPatterns(draft.indexPatterns),
    hostPatterns: selectorPatterns(draft.hostPatterns),
    sourcePatterns: selectorPatterns(draft.sourcePatterns),
    sourcetypePatterns: selectorPatterns(draft.sourcetypePatterns),
  });
  const overwriteBehavior = overwriteFromDraft(draft.overwrite);
  let body: KnowledgeObjectDefinitionMessage["body"];
  switch (draft.kind) {
    case "regex-extraction":
      body = {
        $case: "fieldExtraction",
        value: FieldExtractionDefinition.fromPartial({
          inputField: "_raw",
          overwriteBehavior,
          extraction: {
            $case: "regex",
            value: {
              pattern: draft.regexPattern,
              outputFields: lines(draft.regexOutputFields),
            },
          },
        }),
      };
      break;
    case "json-extraction":
      body = {
        $case: "fieldExtraction",
        value: FieldExtractionDefinition.fromPartial({
          inputField: "_raw",
          overwriteBehavior,
          extraction: {
            $case: "json",
            value: {
              path: draft.jsonPath,
              outputField: draft.jsonOutputField,
            },
          },
        }),
      };
      break;
    case "field-alias":
      body = {
        $case: "fieldAlias",
        value: FieldAliasDefinition.fromPartial({
          sourceField: draft.aliasSourceField,
          destinationField: draft.aliasDestinationField,
          overwriteBehavior,
        }),
      };
      break;
    case "calculated-field":
      body = {
        $case: "calculatedField",
        value: CalculatedFieldDefinition.fromPartial({
          destinationField: draft.calculatedDestinationField,
          expression: draft.calculatedExpression,
          overwriteBehavior,
        }),
      };
      break;
    default:
      throw new TypeError("Knowledge definition kind is outside Tier 1.");
  }
  return KnowledgeObjectDefinition.fromPartial({
    appId: draft.appId,
    name: draft.name,
    description: draft.description === "" ? undefined : draft.description,
    sharingScope: sharingScopeFromDraft(draft.sharingScope),
    selector,
    body,
  });
}

export function knowledgeMutationDraftFromObject(
  object: KnowledgeObject,
): KnowledgeMutationDraft | null {
  const definition = object.definition;
  const sharingScope = definition === undefined
    ? null
    : sharingScopeToDraft(definition.sharingScope);
  if (definition === undefined || sharingScope === null) return null;
  const selector = definition.selector;
  const common = {
    appId: definition.appId,
    name: definition.name,
    description: definition.description ?? "",
    sharingScope,
    indexPatterns: joinedPatterns(selector?.indexPatterns ?? []),
    hostPatterns: joinedPatterns(selector?.hostPatterns ?? []),
    sourcePatterns: joinedPatterns(selector?.sourcePatterns ?? []),
    sourcetypePatterns: joinedPatterns(selector?.sourcetypePatterns ?? []),
  };
  switch (definition.body?.$case) {
    case "fieldExtraction": {
      const extraction = definition.body.value;
      const overwrite = overwriteToDraft(extraction.overwriteBehavior);
      if (overwrite === null || extraction.inputField !== "_raw") return null;
      if (extraction.extraction?.$case === "regex") {
        return {
          ...emptyDraft(definition.appId),
          ...common,
          kind: "regex-extraction",
          regexPattern: extraction.extraction.value.pattern,
          regexOutputFields: extraction.extraction.value.outputFields.join("\n"),
          overwrite,
        };
      }
      if (extraction.extraction?.$case === "json") {
        return {
          ...emptyDraft(definition.appId),
          ...common,
          kind: "json-extraction",
          jsonPath: extraction.extraction.value.path,
          jsonOutputField: extraction.extraction.value.outputField,
          overwrite,
        };
      }
      return null;
    }
    case "fieldAlias": {
      const alias = definition.body.value;
      const overwrite = overwriteToDraft(alias.overwriteBehavior);
      if (overwrite === null) return null;
      return {
        ...emptyDraft(definition.appId),
        ...common,
        kind: "field-alias",
        aliasSourceField: alias.sourceField,
        aliasDestinationField: alias.destinationField,
        overwrite,
      };
    }
    case "calculatedField": {
      const calculated = definition.body.value;
      const overwrite = overwriteToDraft(calculated.overwriteBehavior);
      if (overwrite === null) return null;
      return {
        ...emptyDraft(definition.appId),
        ...common,
        kind: "calculated-field",
        calculatedDestinationField: calculated.destinationField,
        calculatedExpression: calculated.expression,
        overwrite,
      };
    }
    default:
      return null;
  }
}

function sameWire<T>(
  codec: { encode(message: T): { finish(): Uint8Array } },
  left: T,
  right: T,
): boolean {
  const leftBytes = codec.encode(left).finish();
  const rightBytes = codec.encode(right).finish();
  if (leftBytes.byteLength !== rightBytes.byteLength) return false;
  return leftBytes.every((value, index) => value === rightBytes[index]);
}

/** Returns the exact sorted top-level mask accepted by the Writer. */
export function knowledgeDefinitionUpdateMask(
  current: KnowledgeObjectDefinitionMessage,
  candidate: KnowledgeObjectDefinitionMessage,
): string[] {
  const paths: string[] = [];
  if (current.appId !== candidate.appId) paths.push("app_id");
  if (current.name !== candidate.name) paths.push("name");
  if (current.description !== candidate.description) paths.push("description");
  if (current.sharingScope !== candidate.sharingScope) paths.push("sharing_scope");
  const currentSelector = current.selector ?? KnowledgeSelector.fromPartial({});
  const candidateSelector = candidate.selector ?? KnowledgeSelector.fromPartial({});
  if (!sameWire(KnowledgeSelector, currentSelector, candidateSelector)) paths.push("selector");
  if (current.body?.$case !== candidate.body?.$case) {
    throw new TypeError("Knowledge definition kind cannot change during edit.");
  }
  switch (current.body?.$case) {
    case "fieldExtraction":
      if (
        candidate.body?.$case !== "fieldExtraction"
        || !sameWire(FieldExtractionDefinition, current.body.value, candidate.body.value)
      ) paths.push("field_extraction");
      break;
    case "fieldAlias":
      if (
        candidate.body?.$case !== "fieldAlias"
        || !sameWire(FieldAliasDefinition, current.body.value, candidate.body.value)
      ) paths.push("field_alias");
      break;
    case "calculatedField":
      if (
        candidate.body?.$case !== "calculatedField"
        || !sameWire(CalculatedFieldDefinition, current.body.value, candidate.body.value)
      ) paths.push("calculated_field");
      break;
    default:
      throw new TypeError("Knowledge definition kind is outside Tier 1.");
  }
  return paths.toSorted();
}

export function knowledgeBrowserClientRequestId(
  suppliedBytes?: Uint8Array,
): string {
  const bytes = suppliedBytes === undefined
    ? globalThis.crypto.getRandomValues(new Uint8Array(16))
    : Uint8Array.from(suppliedBytes);
  if (bytes.byteLength !== 16) {
    throw new TypeError("Knowledge request identity requires 16 random bytes.");
  }
  return `browser-${Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("")}`;
}

type EditorState =
  | { state: "idle" }
  | { state: "validating" }
  | { state: "valid"; receipt: KnowledgeValidationReceipt }
  | { state: "invalid"; receipt: KnowledgeValidationReceipt }
  | { state: "saving" }
  | { state: "unavailable"; message: string };

export interface KnowledgeMutationEditorProps {
  client: KnowledgeMutationClient;
  apps: readonly KnowledgeManagerAppOption[];
  initialDraft: KnowledgeMutationDraft;
  currentKnowledgeObject?: KnowledgeObject;
  onCancel: () => void;
  onCommitted: () => void;
}

export function KnowledgeMutationEditor({
  client,
  apps,
  initialDraft,
  currentKnowledgeObject,
  onCancel,
  onCommitted,
}: KnowledgeMutationEditorProps) {
  const editing = currentKnowledgeObject !== undefined;
  const id = useId().replaceAll(":", "");
  const formRef = useRef<HTMLFormElement>(null);
  const requestRef = useRef<AbortController | null>(null);
  const [draft, setDraft] = useState(initialDraft);
  const [editorState, setEditorState] = useState<EditorState>({ state: "idle" });

  useEffect(() => () => requestRef.current?.abort(), []);

  function changeDraft(change: Partial<KnowledgeMutationDraft>): void {
    requestRef.current?.abort();
    requestRef.current = null;
    setDraft((current) => ({ ...current, ...change }));
    setEditorState({ state: "idle" });
  }

  function exactCandidate(): {
    definition: KnowledgeObjectDefinitionMessage;
    updateMask: string[] | undefined;
  } {
    const definition = knowledgeDefinitionFromMutationDraft(draft);
    if (!editing) return { definition, updateMask: undefined };
    if (currentKnowledgeObject.definition === undefined) {
      throw new TypeError("Knowledge edit authority omitted its definition.");
    }
    return {
      definition,
      updateMask: knowledgeDefinitionUpdateMask(
        currentKnowledgeObject.definition,
        definition,
      ),
    };
  }

  async function validate(): Promise<void> {
    if (!formRef.current?.reportValidity()) return;
    let candidate: ReturnType<typeof exactCandidate>;
    try {
      candidate = exactCandidate();
    } catch {
      setEditorState({ state: "unavailable", message: "This definition cannot be edited safely." });
      return;
    }
    if (editing && candidate.updateMask?.length === 0) {
      setEditorState({ state: "unavailable", message: "Change at least one definition field before validating." });
      return;
    }
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    setEditorState({ state: "validating" });
    try {
      const request = ValidateKnowledgeObjectRequest.fromPartial({
        definition: candidate.definition,
        knowledgeObjectId: currentKnowledgeObject?.knowledgeObjectId,
        expectedVersion: currentKnowledgeObject?.version,
        updateMask: candidate.updateMask,
        intent: currentKnowledgeObject?.state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
          ? KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION
          : KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
      });
      const receipt = await validateKnowledgeObject(client, request, {
        signal: controller.signal,
        currentKnowledgeObject,
      });
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setEditorState(receipt.result.valid
        ? { state: "valid", receipt }
        : { state: "invalid", receipt });
    } catch {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setEditorState({
        state: "unavailable",
        message: "Validation is unavailable. No definition details were accepted.",
      });
    }
  }

  async function save(): Promise<void> {
    if (editorState.state !== "valid" || !formRef.current?.reportValidity()) return;
    let candidate: ReturnType<typeof exactCandidate>;
    try {
      candidate = exactCandidate();
    } catch {
      setEditorState({ state: "unavailable", message: "This definition cannot be saved safely." });
      return;
    }
    if (editing && candidate.updateMask?.length === 0) return;
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    setEditorState({ state: "saving" });
    try {
      if (currentKnowledgeObject === undefined) {
        await createKnowledgeObject(client, CreateKnowledgeObjectRequest.fromPartial({
          definition: candidate.definition,
          initialState: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
          clientRequestId: knowledgeBrowserClientRequestId(),
        }), { signal: controller.signal });
      } else {
        await updateKnowledgeObject(client, UpdateKnowledgeObjectRequest.fromPartial({
          knowledgeObjectId: currentKnowledgeObject.knowledgeObjectId,
          expectedVersion: currentKnowledgeObject.version,
          definition: candidate.definition,
          updateMask: candidate.updateMask,
          clientRequestId: knowledgeBrowserClientRequestId(),
        }), {
          signal: controller.signal,
          currentKnowledgeObject,
        });
      }
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      onCommitted();
    } catch {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setEditorState({
        state: "unavailable",
        message: editing
          ? "The update was not accepted. Reload the object before retrying."
          : "The draft was not created. Reload the catalog before retrying.",
      });
    }
  }

  const busy = editorState.state === "validating" || editorState.state === "saving";
  const saving = editorState.state === "saving";
  return (
    <form
      className="knowledge-manager__mutation-form"
      ref={formRef}
      onSubmit={(event) => { event.preventDefault(); void validate(); }}
      aria-labelledby={`${id}-title`}
    >
      <header>
        <div>
          <span className="knowledge-manager__eyebrow">TIER-1 DEFINITION</span>
          <h3 id={`${id}-title`}>{editing ? "Edit knowledge object" : "Create knowledge object"}</h3>
          <p>{editing
            ? `Editing exact version ${currentKnowledgeObject.version.toLocaleString()}.`
            : "New objects are created as drafts and activated explicitly."}</p>
        </div>
        <button type="button" onClick={onCancel} disabled={saving}>Cancel</button>
      </header>

      <div className="knowledge-manager__mutation-grid">
        <label htmlFor={`${id}-kind`}><span>Definition type</span>
          <select
            id={`${id}-kind`}
            value={draft.kind}
            onChange={(event) => changeDraft({
              kind: event.currentTarget.value as KnowledgeTierOneDefinitionKind,
            })}
            disabled={editing || saving}
          >
            {TIER_ONE_KIND_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>
        </label>
        <label htmlFor={`${id}-app`}><span>App</span>
          <select
            id={`${id}-app`}
            value={draft.appId}
            onChange={(event) => changeDraft({ appId: event.currentTarget.value })}
            disabled={saving}
            required
          >
            {apps.map((app) => <option key={app.appId} value={app.appId}>{app.label}</option>)}
          </select>
        </label>
        <label htmlFor={`${id}-name`}><span>Name</span>
          <input
            id={`${id}-name`}
            value={draft.name}
            onChange={(event) => changeDraft({ name: event.currentTarget.value })}
            maxLength={255}
            autoComplete="off"
            required
            disabled={saving}
          />
        </label>
        <label htmlFor={`${id}-sharing`}><span>Sharing</span>
          <select
            id={`${id}-sharing`}
            value={draft.sharingScope}
            onChange={(event) => changeDraft({
              sharingScope: event.currentTarget.value as KnowledgeMutationSharingScope,
            })}
            disabled={saving}
          >
            <option value="private">Private</option>
            <option value="app">App</option>
            <option value="global">Global</option>
          </select>
        </label>
        <label className="knowledge-manager__mutation-wide" htmlFor={`${id}-description`}>
          <span>Description <small>(optional)</small></span>
          <textarea
            id={`${id}-description`}
            value={draft.description}
            onChange={(event) => changeDraft({ description: event.currentTarget.value })}
            maxLength={16_384}
            disabled={saving}
          />
        </label>
      </div>

      <fieldset>
        <legend>Selector builder <small>one pattern per line; dimensions are combined with AND</small></legend>
        <div className="knowledge-manager__mutation-grid knowledge-manager__mutation-grid--selectors">
          {([
            ["indexPatterns", "Index patterns"],
            ["hostPatterns", "Host patterns"],
            ["sourcePatterns", "Source patterns"],
            ["sourcetypePatterns", "Sourcetype patterns"],
          ] as const).map(([field, label]) => (
            <label key={field} htmlFor={`${id}-${field}`}><span>{label}</span>
              <textarea
                id={`${id}-${field}`}
                value={draft[field]}
                onChange={(event) => changeDraft({ [field]: event.currentTarget.value })}
                maxLength={4_096}
                disabled={saving}
              />
            </label>
          ))}
        </div>
      </fieldset>

      <fieldset>
        <legend>{TIER_ONE_KIND_OPTIONS.find((option) => option.value === draft.kind)?.label}</legend>
        <KnowledgeMutationBodyFields id={id} draft={draft} busy={saving} changeDraft={changeDraft} />
      </fieldset>

      {editorState.state === "validating" ? (
        <output className="knowledge-manager__mutation-result" aria-live="polite">Validating exact candidate…</output>
      ) : null}
      {editorState.state === "saving" ? (
        <output className="knowledge-manager__mutation-result" aria-live="polite">
          {editing ? "Saving exact update…" : "Creating draft…"}
        </output>
      ) : null}
      {editorState.state === "valid" || editorState.state === "invalid" ? (
        <KnowledgeValidationResultView receipt={editorState.receipt} />
      ) : null}
      {editorState.state === "unavailable" ? (
        <div className="knowledge-manager__mutation-error" role="alert">{editorState.message}</div>
      ) : null}

      <footer className="knowledge-manager__mutation-actions">
        <button type="submit" disabled={busy}>{editing ? "Validate changes" : "Validate draft"}</button>
        <button
          className="suite-button suite-button--primary"
          type="button"
          onClick={() => void save()}
          disabled={busy || editorState.state !== "valid"}
        >{editing ? "Save changes" : "Create draft"}</button>
      </footer>
    </form>
  );
}

function KnowledgeMutationBodyFields({
  id,
  draft,
  busy,
  changeDraft,
}: {
  id: string;
  draft: KnowledgeMutationDraft;
  busy: boolean;
  changeDraft: (change: Partial<KnowledgeMutationDraft>) => void;
}) {
  let fields: React.ReactNode;
  switch (draft.kind) {
    case "regex-extraction":
      fields = <>
        <label className="knowledge-manager__mutation-wide" htmlFor={`${id}-regex-pattern`}>
          <span>Regex pattern</span>
          <textarea id={`${id}-regex-pattern`} value={draft.regexPattern} onChange={(event) => changeDraft({ regexPattern: event.currentTarget.value })} maxLength={4_096} required disabled={busy} />
        </label>
        <label htmlFor={`${id}-regex-outputs`}><span>Output fields <small>one per line</small></span>
          <textarea id={`${id}-regex-outputs`} value={draft.regexOutputFields} onChange={(event) => changeDraft({ regexOutputFields: event.currentTarget.value })} maxLength={4_096} required disabled={busy} />
        </label>
      </>;
      break;
    case "json-extraction":
      fields = <>
        <label className="knowledge-manager__mutation-wide" htmlFor={`${id}-json-path`}><span>JSON path</span>
          <textarea id={`${id}-json-path`} value={draft.jsonPath} onChange={(event) => changeDraft({ jsonPath: event.currentTarget.value })} maxLength={4_096} required disabled={busy} />
        </label>
        <label htmlFor={`${id}-json-output`}><span>Output field</span>
          <input id={`${id}-json-output`} value={draft.jsonOutputField} onChange={(event) => changeDraft({ jsonOutputField: event.currentTarget.value })} maxLength={255} required disabled={busy} />
        </label>
      </>;
      break;
    case "field-alias":
      fields = <>
        <label htmlFor={`${id}-alias-source`}><span>Source field</span>
          <input id={`${id}-alias-source`} value={draft.aliasSourceField} onChange={(event) => changeDraft({ aliasSourceField: event.currentTarget.value })} maxLength={255} required disabled={busy} />
        </label>
        <label htmlFor={`${id}-alias-destination`}><span>Destination field</span>
          <input id={`${id}-alias-destination`} value={draft.aliasDestinationField} onChange={(event) => changeDraft({ aliasDestinationField: event.currentTarget.value })} maxLength={255} required disabled={busy} />
        </label>
      </>;
      break;
    case "calculated-field":
      fields = <>
        <label htmlFor={`${id}-calculated-destination`}><span>Destination field</span>
          <input id={`${id}-calculated-destination`} value={draft.calculatedDestinationField} onChange={(event) => changeDraft({ calculatedDestinationField: event.currentTarget.value })} maxLength={255} required disabled={busy} />
        </label>
        <label className="knowledge-manager__mutation-wide" htmlFor={`${id}-calculated-expression`}><span>Expression</span>
          <textarea id={`${id}-calculated-expression`} value={draft.calculatedExpression} onChange={(event) => changeDraft({ calculatedExpression: event.currentTarget.value })} maxLength={16_384} required disabled={busy} />
        </label>
      </>;
      break;
  }
  return <div className="knowledge-manager__mutation-grid">
    {fields}
    <label htmlFor={`${id}-overwrite`}><span>Existing destination</span>
      <select id={`${id}-overwrite`} value={draft.overwrite} onChange={(event) => changeDraft({ overwrite: event.currentTarget.value as KnowledgeMutationOverwrite })} disabled={busy}>
        <option value="preserve">Preserve existing value</option>
        <option value="replace">Replace existing value</option>
      </select>
    </label>
  </div>;
}

function KnowledgeValidationResultView({ receipt }: { receipt: KnowledgeValidationReceipt }) {
  const { result } = receipt;
  return (
    <section
      className={`knowledge-manager__mutation-result knowledge-manager__mutation-result--${result.valid ? "valid" : "invalid"}`}
      aria-live="polite"
    >
      <strong>{result.valid ? "Validation passed" : "Validation found issues"}</strong>
      <span>Catalog revision {receipt.tenantCatalogRevision.toLocaleString()}</span>
      {result.fieldViolations.length === 0 ? null : (
        <ul>
          {result.fieldViolations.map((issue) => (
            <li key={`${issue.fieldPath}\0${issue.code}\0${issue.message}`}>
              <code>{issue.fieldPath}</code> — {issue.message}
            </li>
          ))}
        </ul>
      )}
      {result.diagnostics.length === 0 ? null : (
        <ul>
          {result.diagnostics.map((issue) => (
            <li key={`${issue.fieldPath}\0${issue.diagnostic?.code ?? ""}\0${issue.diagnostic?.message ?? ""}`}>
              <code>{issue.fieldPath}</code> — {issue.diagnostic?.message}
              {issue.diagnostic?.sourceRange?.start === undefined ? null : (
                <small> line {issue.diagnostic.sourceRange.start.line}, column {issue.diagnostic.sourceRange.start.column}</small>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export function KnowledgeCreateControl({
  client,
  apps,
  initialAppId,
  onCommitted,
}: {
  client: KnowledgeMutationClient;
  apps: readonly KnowledgeManagerAppOption[];
  initialAppId: string | null;
  onCommitted: () => void;
}) {
  const [open, setOpen] = useState(false);
  const appId = apps.some((app) => app.appId === initialAppId)
    ? initialAppId as string
    : apps[0]?.appId ?? "";
  if (!open) {
    return <button
      className="suite-button suite-button--primary"
      type="button"
      onClick={() => setOpen(true)}
      disabled={appId === ""}
    >＋ Create knowledge object</button>;
  }
  return <KnowledgeMutationEditor
    client={client}
    apps={apps}
    initialDraft={createKnowledgeMutationDraft(appId)}
    onCancel={() => setOpen(false)}
    onCommitted={() => { setOpen(false); onCommitted(); }}
  />;
}

type ObjectActionState = "idle" | "working" | "unavailable";

export function KnowledgeObjectMutationControls({
  client,
  apps,
  currentKnowledgeObject,
  onCommitted,
}: {
  client: KnowledgeMutationClient;
  apps: readonly KnowledgeManagerAppOption[];
  currentKnowledgeObject: KnowledgeObject;
  onCommitted: () => void;
}) {
  const [mode, setMode] = useState<"actions" | "edit" | "delete">("actions");
  const [confirmation, setConfirmation] = useState("");
  const [actionState, setActionState] = useState<ObjectActionState>("idle");
  const requestRef = useRef<AbortController | null>(null);
  const draft = knowledgeMutationDraftFromObject(currentKnowledgeObject);
  useEffect(() => () => requestRef.current?.abort(), []);

  if (draft === null) return null;
  if (mode === "edit") {
    return <KnowledgeMutationEditor
      client={client}
      apps={apps}
      initialDraft={draft}
      currentKnowledgeObject={currentKnowledgeObject}
      onCancel={() => setMode("actions")}
      onCommitted={onCommitted}
    />;
  }

  const state = currentKnowledgeObject.state;
  const mayEdit = state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
    || state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
    || state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED;
  const targetState = state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
    || state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
    ? KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
    : state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
      ? KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
      : null;
  const mayDelete = mayEdit;

  async function setState(): Promise<void> {
    if (targetState === null || actionState === "working") return;
    const controller = new AbortController();
    requestRef.current?.abort();
    requestRef.current = controller;
    setActionState("working");
    try {
      await setKnowledgeObjectState(client, SetKnowledgeObjectStateRequest.fromPartial({
        knowledgeObjectId: currentKnowledgeObject.knowledgeObjectId,
        expectedVersion: currentKnowledgeObject.version,
        state: targetState,
        clientRequestId: knowledgeBrowserClientRequestId(),
      }), { signal: controller.signal, currentKnowledgeObject });
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      onCommitted();
    } catch {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setActionState("unavailable");
    }
  }

  async function remove(): Promise<void> {
    if (
      !mayDelete
      || confirmation !== currentKnowledgeObject.name
      || actionState === "working"
    ) return;
    const controller = new AbortController();
    requestRef.current?.abort();
    requestRef.current = controller;
    setActionState("working");
    try {
      await deleteKnowledgeObject(client, DeleteKnowledgeObjectRequest.fromPartial({
        knowledgeObjectId: currentKnowledgeObject.knowledgeObjectId,
        expectedVersion: currentKnowledgeObject.version,
        clientRequestId: knowledgeBrowserClientRequestId(),
      }), { signal: controller.signal });
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      onCommitted();
    } catch {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setActionState("unavailable");
    }
  }

  if (mode === "delete") {
    return <section className="knowledge-manager__delete-confirmation" aria-labelledby="knowledge-delete-confirmation-title">
      <h4 id="knowledge-delete-confirmation-title">Confirm delete</h4>
      <p>Deleting appends a terminal tombstone. Type <strong>{currentKnowledgeObject.name}</strong> to confirm.</p>
      <label htmlFor="knowledge-delete-confirmation"><span>Object name</span>
        <input
          id="knowledge-delete-confirmation"
          value={confirmation}
          onChange={(event) => setConfirmation(event.currentTarget.value)}
          autoComplete="off"
          disabled={actionState === "working"}
        />
      </label>
      {actionState === "unavailable" ? (
        <div role="alert">Delete was not accepted. Reload the object before retrying.</div>
      ) : null}
      <div className="knowledge-manager__mutation-actions">
        <button type="button" onClick={() => { setMode("actions"); setConfirmation(""); setActionState("idle"); }} disabled={actionState === "working"}>Cancel</button>
        <button className="button danger" type="button" onClick={() => void remove()} disabled={confirmation !== currentKnowledgeObject.name || actionState === "working"}>
          {actionState === "working" ? "Deleting…" : "Delete knowledge object"}
        </button>
      </div>
    </section>;
  }

  return <section className="knowledge-manager__object-actions" aria-label="Knowledge object actions">
    <div>
      <button type="button" onClick={() => setMode("edit")} disabled={!mayEdit || actionState === "working"}>Edit</button>
      {targetState === null ? null : (
        <button type="button" onClick={() => void setState()} disabled={actionState === "working"}>
          {actionState === "working"
            ? "Updating state…"
            : targetState === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
              ? "Activate"
              : "Disable"}
        </button>
      )}
      <button type="button" onClick={() => { setMode("delete"); setActionState("idle"); }} disabled={!mayDelete || actionState === "working"}>Delete</button>
    </div>
    {actionState === "unavailable" ? (
      <div role="alert">The state change was not accepted. Reload the object before retrying.</div>
    ) : null}
  </section>;
}

type KnowledgeQuarantineControlState =
  | "idle"
  | "preparing"
  | "prepared"
  | "quarantining"
  | "unavailable";

/**
 * Definition-free emergency recovery control. It deliberately needs only the
 * already-disclosed list identity, so a corrupt definition cannot prevent an
 * administrator from preparing a quarantine plan.
 */
export function KnowledgeQuarantineControl({
  client,
  knowledgeObjectId,
  name,
  state,
  onCommitted,
}: {
  client: KnowledgeMutationClient;
  knowledgeObjectId: string;
  name: string;
  state: KnowledgeObjectState;
  onCommitted: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [confirmation, setConfirmation] = useState("");
  const [controlState, setControlState] = useState<KnowledgeQuarantineControlState>("idle");
  const [preparation, setPreparation] = useState<KnowledgeQuarantinePreparation | null>(null);
  const requestRef = useRef<AbortController | null>(null);
  useEffect(() => () => requestRef.current?.abort(), []);
  useEffect(() => {
    if (preparation === null) return;
    const remainingMilliseconds = preparation.expiresAt.valueOf() - Date.now();
    if (remainingMilliseconds <= 0) {
      setControlState("unavailable");
      return;
    }
    const timeout = window.setTimeout(
      () => setControlState("unavailable"),
      Math.min(remainingMilliseconds, 2_147_483_647),
    );
    return () => window.clearTimeout(timeout);
  }, [preparation]);

  const eligible = state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
    || state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
    || state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED;
  if (!eligible) return null;

  function close(): void {
    requestRef.current?.abort();
    requestRef.current = null;
    setOpen(false);
    setConfirmation("");
    setPreparation(null);
    setControlState("idle");
  }

  async function prepare(): Promise<void> {
    if (controlState === "preparing" || controlState === "quarantining") return;
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    setConfirmation("");
    setPreparation(null);
    setControlState("preparing");
    try {
      const result = await prepareKnowledgeObjectQuarantine(
        client,
        PrepareKnowledgeObjectQuarantineRequest.fromPartial({ knowledgeObjectId }),
        { signal: controller.signal },
      );
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      if (result.expiresAt.valueOf() <= Date.now()) {
        setControlState("unavailable");
        return;
      }
      setPreparation(result);
      setControlState("prepared");
    } catch {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setControlState("unavailable");
    }
  }

  async function quarantine(): Promise<void> {
    if (
      preparation === null
      || confirmation !== name
      || controlState !== "prepared"
    ) return;
    if (preparation.expiresAt.valueOf() <= Date.now()) {
      setControlState("unavailable");
      return;
    }
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    setControlState("quarantining");
    try {
      await quarantineKnowledgeObject(
        client,
        QuarantineKnowledgeObjectRequest.fromPartial({
          recoveryToken: preparation.recoveryToken,
          clientRequestId: knowledgeBrowserClientRequestId(),
        }),
        { signal: controller.signal, preparation },
      );
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      onCommitted();
    } catch {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      setPreparation(null);
      setConfirmation("");
      setControlState("unavailable");
    }
  }

  if (!open) {
    return <section className="knowledge-manager__object-actions" aria-label="Knowledge recovery actions">
      <div>
        <button className="button danger" type="button" onClick={() => setOpen(true)}>
          Quarantine
        </button>
      </div>
    </section>;
  }

  const expired = preparation !== null && controlState === "unavailable";
  return <section
    className="knowledge-manager__delete-confirmation"
    aria-labelledby="knowledge-quarantine-confirmation-title"
  >
    <h4 id="knowledge-quarantine-confirmation-title">Emergency quarantine</h4>
    <p>
      Quarantine is terminal and removes the current definition from normal access.
      The integrity scan also identifies active objects that must be quarantined first.
    </p>
    {preparation === null ? (
      <div className="knowledge-manager__mutation-actions">
        <button type="button" onClick={close} disabled={controlState === "preparing"}>Cancel</button>
        <button
          type="button"
          onClick={() => void prepare()}
          disabled={controlState === "preparing"}
        >{controlState === "preparing" ? "Scanning impact…" : "Scan quarantine impact"}</button>
      </div>
    ) : (
      <>
        <dl className="knowledge-manager__metadata">
          <div><dt>Active dependents</dt><dd>{preparation.dependentCount.toLocaleString()}</dd></div>
          <div><dt>Catalog revision</dt><dd>{preparation.tenantCatalogRevision.toLocaleString()}</dd></div>
          <div><dt>Plan expires</dt><dd>{preparation.expiresAt.toLocaleTimeString()}</dd></div>
        </dl>
        <p>
          Type <strong>{name}</strong> to quarantine this object
          {preparation.dependentCount === 0
            ? "."
            : ` and ${preparation.dependentCount.toLocaleString()} active dependent${preparation.dependentCount === 1 ? "" : "s"}.`}
        </p>
        <label htmlFor="knowledge-quarantine-confirmation"><span>Object name</span>
          <input
            id="knowledge-quarantine-confirmation"
            value={confirmation}
            onChange={(event) => setConfirmation(event.currentTarget.value)}
            autoComplete="off"
            disabled={controlState === "quarantining" || expired}
          />
        </label>
        {expired ? <div role="alert">This recovery plan expired. Scan again before continuing.</div> : null}
        <div className="knowledge-manager__mutation-actions">
          <button type="button" onClick={close} disabled={controlState === "quarantining"}>Cancel</button>
          <button type="button" onClick={() => void prepare()} disabled={controlState === "quarantining"}>Scan again</button>
          <button
            className="button danger"
            type="button"
            onClick={() => void quarantine()}
            disabled={confirmation !== name || controlState === "quarantining" || expired}
          >{controlState === "quarantining" ? "Quarantining…" : "Quarantine object and dependents"}</button>
        </div>
      </>
    )}
    {controlState === "unavailable" ? (
      <div role="alert">The recovery plan was not accepted. Reload the catalog and scan again.</div>
    ) : null}
  </section>;
}
