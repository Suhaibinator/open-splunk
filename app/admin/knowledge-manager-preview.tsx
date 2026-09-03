"use client";

import { useEffect, useRef, useState } from "react";

import type {
  KnowledgeObject,
  KnowledgeObjectDefinition,
} from "@/gen/ts/open_splunk/knowledge";
import { PreviewKnowledgeObjectRequest } from "@/gen/ts/open_splunk/knowledge_api";
import type { ResultColumn } from "@/gen/ts/open_splunk/result";

import {
  KNOWLEDGE_PREVIEW_DEFAULT_MAXIMUM_ROWS,
  KNOWLEDGE_PREVIEW_MAXIMUM_ROWS,
  knowledgePreviewValueText,
  type KnowledgePreviewClient,
  type KnowledgePreviewProjection,
  type KnowledgePreviewReceipt,
} from "./knowledge-manager-preview-data";

type PreviewState = "idle" | "loading" | "available" | "invalid" | "unavailable";

export const KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP = 16_384;

export interface KnowledgePreviewRowWindow {
  readonly page: number;
  readonly pageCount: number;
  readonly rowsPerPage: number;
  readonly start: number;
  readonly end: number;
  readonly totalRows: number;
}

// The schema headers consume cells too. Reserving their combined width before
// deriving one shared row window keeps both tables under one strict DOM bound.
export function knowledgePreviewRowWindow(
  beforeColumns: number,
  afterColumns: number,
  beforeRows: number,
  afterRows: number,
  requestedPage: number,
): KnowledgePreviewRowWindow {
  const dimensions = [beforeColumns, afterColumns, beforeRows, afterRows, requestedPage];
  if (
    dimensions.some((value) => !Number.isSafeInteger(value) || value < 0)
    || beforeColumns === 0
    || afterColumns === 0
  ) {
    throw new TypeError("Knowledge Preview window dimensions are invalid.");
  }
  const combinedColumns = beforeColumns + afterColumns;
  const emptyProjectionCells = Number(beforeRows === 0) + Number(afterRows === 0);
  const bodyBudget = KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP
    - combinedColumns
    - emptyProjectionCells;
  if (bodyBudget < combinedColumns) {
    throw new TypeError("Knowledge Preview schemas exceed the rendered-cell contract.");
  }
  const rowsPerPage = Math.floor(bodyBudget / combinedColumns);
  const totalRows = Math.max(beforeRows, afterRows);
  const pageCount = Math.max(1, Math.ceil(totalRows / rowsPerPage));
  const page = Math.min(requestedPage, pageCount - 1);
  const start = page * rowsPerPage;
  return {
    page,
    pageCount,
    rowsPerPage,
    start,
    end: Math.min(totalRows, start + rowsPerPage),
    totalRows,
  };
}

export interface KnowledgeManagerPreviewProps {
  readonly client: KnowledgePreviewClient;
  readonly currentKnowledgeObject: KnowledgeObject;
}

export interface ActivePreviewRequest {
  readonly generation: number;
  readonly controller: AbortController;
}

export function knowledgePreviewRequestIsCurrent(
  active: ActivePreviewRequest | null,
  controller: AbortController,
  generation: number,
): boolean {
  return !controller.signal.aborted
    && active?.controller === controller
    && active.generation === generation;
}

export function KnowledgeManagerPreview({
  client,
  currentKnowledgeObject,
}: KnowledgeManagerPreviewProps) {
  const [retainedJobID, setRetainedJobID] = useState("");
  const [maximumRows, setMaximumRows] = useState(
    KNOWLEDGE_PREVIEW_DEFAULT_MAXIMUM_ROWS.toString(),
  );
  const [state, setState] = useState<PreviewState>("idle");
  const [receipt, setReceipt] = useState<KnowledgePreviewReceipt | null>(null);
  const active = useRef<ActivePreviewRequest | null>(null);
  const nextGeneration = useRef(0);

  useEffect(() => () => {
    nextGeneration.current += 1;
    active.current?.controller.abort();
    active.current = null;
  }, []);

  async function submit(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    const definition = currentKnowledgeObject.definition;
    const parsedMaximumRows = Number(maximumRows);
    const updateMask = definition === undefined
      ? null
      : knowledgePreviewUpdateMask(definition);
    if (
      definition === undefined
      || updateMask === null
      || !Number.isInteger(parsedMaximumRows)
      || parsedMaximumRows < 1
      || parsedMaximumRows > KNOWLEDGE_PREVIEW_MAXIMUM_ROWS
    ) {
      active.current?.controller.abort();
      active.current = null;
      setReceipt(null);
      setState("unavailable");
      return;
    }

    active.current?.controller.abort();
    const generation = nextGeneration.current + 1;
    nextGeneration.current = generation;
    const controller = new AbortController();
    active.current = { generation, controller };
    setReceipt(null);
    setState("loading");
    try {
      const result = await client.preview(
        PreviewKnowledgeObjectRequest.fromPartial({
          retainedSearchJobId: retainedJobID,
          definition,
          knowledgeObjectId: currentKnowledgeObject.knowledgeObjectId,
          expectedVersion: currentKnowledgeObject.version,
          updateMask,
          maximumRows: parsedMaximumRows,
        }),
        {
          signal: controller.signal,
          currentKnowledgeObject,
        },
      );
      if (!knowledgePreviewRequestIsCurrent(active.current, controller, generation)) return;
      active.current = null;
      setReceipt(result);
      setState(result.validation.valid ? "available" : "invalid");
    } catch {
      if (!knowledgePreviewRequestIsCurrent(active.current, controller, generation)) return;
      active.current = null;
      setReceipt(null);
      setState("unavailable");
    }
  }

  return (
    <section className="knowledge-preview" aria-labelledby="knowledge-preview-title">
      <h4 id="knowledge-preview-title">Preview on retained search</h4>
      <p className="knowledge-preview__intro">
        Compare this exact object version against an immutable retained search result scope.
        Preview does not publish or reserve the candidate.
      </p>
      <form className="knowledge-preview__form" onSubmit={(event) => void submit(event)} autoComplete="off" noValidate>
        <label className="knowledge-preview__field" htmlFor="knowledge-preview-job-id">
          <span>Retained search job ID</span>
          <input
            id="knowledge-preview-job-id"
            value={retainedJobID}
            maxLength={256}
            required
            autoComplete="off"
            onChange={(event) => setRetainedJobID(event.currentTarget.value)}
          />
        </label>
        <label className="knowledge-preview__field" htmlFor="knowledge-preview-maximum-rows">
          <span>Maximum rows per side</span>
          <input
            id="knowledge-preview-maximum-rows"
            type="number"
            min={1}
            max={KNOWLEDGE_PREVIEW_MAXIMUM_ROWS}
            step={1}
            value={maximumRows}
            onChange={(event) => setMaximumRows(event.currentTarget.value)}
          />
        </label>
        <button className="knowledge-preview__submit" type="submit" disabled={state === "loading"}>
          {state === "loading" ? "Comparing…" : "Compare before and after"}
        </button>
      </form>
      <div className="knowledge-preview__status" aria-live="polite">
        {state === "idle" ? <p>No Preview request has been sent.</p> : null}
        {state === "loading" ? <p>Executing both bounded projections…</p> : null}
        {state === "unavailable" ? (
          <p role="alert">Preview is unavailable or returned an invalid response. No rows were retained.</p>
        ) : null}
        {state === "invalid" && receipt !== null ? (
          <KnowledgePreviewValidation receipt={receipt} />
        ) : null}
        {state === "available" && receipt !== null
        && receipt.before !== null && receipt.after !== null ? (
          <KnowledgePreviewComparison receipt={receipt} />
        ) : null}
      </div>
    </section>
  );
}

export function KnowledgePreviewComparison({
  receipt,
}: {
  readonly receipt: KnowledgePreviewReceipt;
}) {
  const [requestedPage, setRequestedPage] = useState(0);
  const [activeReceipt, setActiveReceipt] = useState(receipt);
  if (activeReceipt !== receipt) {
    setActiveReceipt(receipt);
    setRequestedPage(0);
  }
  if (receipt.before === null || receipt.after === null) return null;
  const rowWindow = knowledgePreviewRowWindow(
    receipt.before.schema.columns.length,
    receipt.after.schema.columns.length,
    receipt.before.rows.length,
    receipt.after.rows.length,
    requestedPage,
  );
  const firstVisibleRow = rowWindow.totalRows === 0 ? 0 : rowWindow.start + 1;
  return (
    <>
      <p>
        Catalog revision {receipt.tenantCatalogRevision.toString()}
        {receipt.truncated ? " · row bound reached" : " · complete within row bound"}
      </p>
      <nav className="knowledge-preview__pagination" aria-label="Knowledge Preview row pages">
        <button
          type="button"
          aria-label="Previous Knowledge Preview rows"
          disabled={rowWindow.page === 0}
          onClick={() => setRequestedPage((page) => Math.max(0, page - 1))}
        >
          Previous
        </button>
        <output aria-live="polite" aria-label="Knowledge Preview row status">
          Rows {firstVisibleRow.toLocaleString()}–{rowWindow.end.toLocaleString()}
          {" of "}{rowWindow.totalRows.toLocaleString()}
          {" · page "}{(rowWindow.page + 1).toLocaleString()}
          {" of "}{rowWindow.pageCount.toLocaleString()}
        </output>
        <button
          type="button"
          aria-label="Next Knowledge Preview rows"
          disabled={rowWindow.page + 1 >= rowWindow.pageCount}
          onClick={() => setRequestedPage((page) => Math.min(rowWindow.pageCount - 1, page + 1))}
        >
          Next
        </button>
      </nav>
      <div className="knowledge-preview__comparison">
        <KnowledgePreviewTable label="Before" projection={receipt.before} rowWindow={rowWindow} />
        <KnowledgePreviewTable label="After" projection={receipt.after} rowWindow={rowWindow} />
      </div>
    </>
  );
}

function KnowledgePreviewValidation({ receipt }: { receipt: KnowledgePreviewReceipt }) {
  const fieldViolations = receipt.validation.fieldViolations;
  const diagnostics = receipt.validation.diagnostics;
  return (
    <div role="alert">
      <strong>Candidate is not valid for active publication.</strong>
      <ul>
        {fieldViolations.map((issue) => (
          <li key={`field:${issue.fieldPath}:${issue.code}`}>
            {issue.fieldPath}: {issue.message}
          </li>
        ))}
        {diagnostics.map((issue) => (
          <li key={`diagnostic:${issue.fieldPath}:${issue.diagnostic?.code ?? "missing"}`}>
            {issue.fieldPath}: {issue.diagnostic?.message ?? "Invalid diagnostic"}
          </li>
        ))}
      </ul>
    </div>
  );
}

function KnowledgePreviewTable({
  label,
  projection,
  rowWindow,
}: {
  readonly label: "Before" | "After";
  readonly projection: KnowledgePreviewProjection;
  readonly rowWindow: KnowledgePreviewRowWindow;
}) {
  const visibleRows = projection.rows.slice(rowWindow.start, rowWindow.end);
  return (
    <section className="knowledge-preview__projection" aria-label={`${label} projection`}>
      <h5>{label}</h5>
      <p>
        Schema {projection.schema.schemaId} · revision {projection.schema.revision.toString()}
        {" · "}{projection.schema.columns.length.toLocaleString()} columns
        {" · "}{projection.rows.length.toLocaleString()} rows
      </p>
      <section className="knowledge-preview__table-scroll" aria-label={`${label} projection table`}>
        <table className="table knowledge-preview__table">
          <caption className="sr-only">{label} retained-search projection</caption>
          <thead>
            <tr>
              {projection.schema.columns.map((column) => (
                <th key={column.fieldName} scope="col">
                  <span>{column.fieldName}</span>
                  <small>{columnContract(column)}</small>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {projection.rows.length === 0 ? (
              <tr><td colSpan={projection.schema.columns.length}>No rows</td></tr>
            ) : visibleRows.map((row) => (
              <tr key={row.rowId} data-row-id={row.rowId}>
                {row.cells.map((cell, index) => (
                  <td key={`${row.rowId}:${projection.schema.columns[index]!.fieldName}`}>
                    <code>{knowledgePreviewValueText(cell)}</code>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </section>
  );
}

function columnContract(column: ResultColumn): string {
  return [
    `type ${column.valueType}`,
    `semantic ${column.semanticType}`,
    column.nullable ? "nullable" : "required",
    column.multivalue ? "multivalue" : "scalar",
  ].join(" · ");
}

export function knowledgePreviewUpdateMask(
  definition: KnowledgeObjectDefinition,
): string[] | null {
  if (definition.body === undefined) return null;
  const bodyPath = definition.body.$case === "fieldExtraction"
    ? "field_extraction"
    : definition.body.$case === "fieldAlias"
      ? "field_alias"
      : definition.body.$case === "calculatedField"
        ? "calculated_field"
        : null;
  return bodyPath === null
    ? null
    : ["app_id", "description", bodyPath, "name", "selector", "sharing_scope"];
}
