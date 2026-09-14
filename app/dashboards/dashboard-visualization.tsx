"use client";

import { type KeyboardEvent, useEffect, useId, useMemo, useRef, useState } from "react";

import {
  VisualizationSpec,
  VisualizationStackMode,
  VisualizationType,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/result";

import { Select, SelectOption } from "../_components/select";
import { linearTickScale, projectScaleValue } from "../search-workspace/charts/chart-scale";
import { stackChartRows, stackedChartDomain } from "../search-workspace/charts/chart-stacking";
import {
  dashboardCellText,
  dashboardDurationFromInput,
  dashboardDurationInput,
  dashboardDurationIsValid,
  type DashboardCartesianVisualization,
  type DashboardField,
  type DashboardPieVisualization,
  type DashboardVisualizationError,
  type DashboardVisualizationModel,
  dashboardVisualizationTypeLabel,
  projectDashboardVisualization,
} from "./dashboard-visualization-model";

const VIEWBOX_WIDTH = 1_000;
const VIEWBOX_HEIGHT = 320;
const PLOT_LEFT = 48;
const PLOT_RIGHT = 984;
const PLOT_TOP = 16;
const PLOT_BOTTOM = 280;

const VISUALIZATION_TYPES = [
  VisualizationType.VISUALIZATION_TYPE_TABLE,
  VisualizationType.VISUALIZATION_TYPE_LINE,
  VisualizationType.VISUALIZATION_TYPE_AREA,
  VisualizationType.VISUALIZATION_TYPE_COLUMN,
  VisualizationType.VISUALIZATION_TYPE_BAR,
  VisualizationType.VISUALIZATION_TYPE_PIE,
  VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE,
  VisualizationType.VISUALIZATION_TYPE_SCATTER,
] as const;

interface DashboardVisualizationEditorProps {
  disabled: boolean;
  fields: readonly DashboardField[];
  onChange: (value: VisualizationSpec) => void;
  onValidityChange?: (valid: boolean) => void;
  value: VisualizationSpec | undefined;
}

function uniqueFields(fields: readonly DashboardField[], value: VisualizationSpec | undefined): DashboardField[] {
  const byName = new Map<string, DashboardField>();
  for (const field of fields) if (field.fieldName) byName.set(field.fieldName, field);
  for (const fieldName of [value?.xField, value?.seriesField, ...(value?.yFields ?? [])]) {
    if (fieldName && !byName.has(fieldName)) byName.set(fieldName, { fieldName, label: fieldName });
  }
  return [...byName.values()];
}

function createVisualizationSpec(type: VisualizationType): VisualizationSpec {
  return VisualizationSpec.fromPartial({
    showLegend: true,
    stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE,
    type,
  });
}

export function DashboardVisualizationEditor({ disabled, fields, onChange, onValidityChange, value }: DashboardVisualizationEditorProps) {
  const [addedFields, setAddedFields] = useState<DashboardField[]>([]);
  const storedDuration = dashboardDurationInput(value?.timeBucketWidth);
  const [durationEdit, setDurationEdit] = useState({ base: storedDuration, draft: storedDuration });
  const [fieldDraft, setFieldDraft] = useState("");
  const durationErrorId = useId();
  const durationDraft = durationEdit.base === storedDuration ? durationEdit.draft : storedDuration;
  const candidates = uniqueFields([...fields, ...addedFields], value);
  const spec = value ?? createVisualizationSpec(VisualizationType.VISUALIZATION_TYPE_TABLE);
  const durationValid = dashboardDurationIsValid(value?.timeBucketWidth)
    && (durationDraft === "" || dashboardDurationFromInput(durationDraft) !== null);
  const typeValid = VISUALIZATION_TYPES.some((type) => type === spec.type);
  const stackValid = [
    VisualizationStackMode.VISUALIZATION_STACK_MODE_UNSPECIFIED,
    VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE,
    VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED,
    VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED_100_PERCENT,
  ].some((mode) => mode === spec.stackMode);
  const editorValid = durationValid && typeValid && stackValid;
  const update = (changes: Partial<VisualizationSpec>) => onChange(VisualizationSpec.fromPartial({ ...spec, ...changes }));

  useEffect(() => onValidityChange?.(editorValid), [editorValid, onValidityChange]);

  function addField() {
    const fieldName = fieldDraft.trim();
    if (!fieldName || candidates.some((field) => field.fieldName === fieldName)) return;
    setAddedFields((current) => [...current, { fieldName, label: fieldName }]);
    setFieldDraft("");
  }

  function selectYField(fieldName: string, selected: boolean) {
    update({ yFields: selected ? [...spec.yFields, fieldName] : spec.yFields.filter((candidate) => candidate !== fieldName) });
  }

  function moveYField(fieldName: string, direction: -1 | 1) {
    const index = spec.yFields.indexOf(fieldName);
    const destination = index + direction;
    if (index < 0 || destination < 0 || destination >= spec.yFields.length) return;
    const yFields = [...spec.yFields];
    [yFields[index], yFields[destination]] = [yFields[destination], yFields[index]];
    update({ yFields });
  }

  return (
    <fieldset className="operations-visualization-editor" disabled={disabled}>
      <legend>Visualization settings</legend>
      <div className="operations-visualization-field"><span>Visualization</span><Select aria-label="Visualization" value={String(value?.type ?? VisualizationType.VISUALIZATION_TYPE_TABLE)} onValueChange={(next) => update({ type: Number(next) as VisualizationType })}>
        {VISUALIZATION_TYPES.map((type) => <SelectOption key={type} value={String(type)}>{dashboardVisualizationTypeLabel(type)}</SelectOption>)}
      </Select></div>
      <label><span>Visualization title</span><input value={spec.title ?? ""} onChange={(event) => update({ title: event.target.value || undefined })} /></label>
      <div className="operations-visualization-field"><span>X field</span><Select aria-label="X field" placeholder="Choose an X field" value={spec.xField ?? ""} onValueChange={(xField) => update({ xField: xField || undefined })}>
        <SelectOption value="">No X field</SelectOption>
        {candidates.map((field) => <SelectOption key={field.fieldName} value={field.fieldName}>{field.label}</SelectOption>)}
      </Select></div>
      <div className="operations-visualization-field"><span>Series field</span><Select aria-label="Series field" placeholder="No series field" value={spec.seriesField ?? ""} onValueChange={(seriesField) => update({ seriesField: seriesField || undefined })}>
        <SelectOption value="">No series field</SelectOption>
        {candidates.map((field) => <SelectOption key={field.fieldName} value={field.fieldName}>{field.label}</SelectOption>)}
      </Select></div>
      <div className="operations-visualization-custom-field">
        <label><span>Visualization field name</span><input value={fieldDraft} onChange={(event) => setFieldDraft(event.target.value)} /></label>
        <button className="button button--secondary button--compact" disabled={!fieldDraft.trim()} type="button" onClick={addField}>Add visualization field</button>
      </div>
      <fieldset aria-label="Y fields" className="operations-visualization-y-fields">
        <legend>Y fields</legend>
        {candidates.length === 0 ? <p>Add a field name to configure chart axes before running the search.</p> : candidates.map((field) => {
          const index = spec.yFields.indexOf(field.fieldName);
          return <div key={field.fieldName}>
            <label><input checked={index >= 0} type="checkbox" onChange={(event) => selectYField(field.fieldName, event.target.checked)} /><span>{field.label}</span></label>
            {index >= 0 ? <span className="operations-visualization-y-actions">
              <button aria-label={`Move ${field.label} up`} className="button button--ghost button--compact" disabled={index === 0} type="button" onClick={() => moveYField(field.fieldName, -1)}>↑</button>
              <button aria-label={`Move ${field.label} down`} className="button button--ghost button--compact" disabled={index === spec.yFields.length - 1} type="button" onClick={() => moveYField(field.fieldName, 1)}>↓</button>
            </span> : null}
          </div>;
        })}
      </fieldset>
      {spec.yFields.length > 0 ? <ol aria-label="Selected Y field order" className="operations-visualization-y-order">{spec.yFields.map((fieldName) => <li key={fieldName}>{candidates.find((field) => field.fieldName === fieldName)?.label ?? fieldName}</li>)}</ol> : null}
      <div className="operations-visualization-field"><span>Stacking</span><Select aria-label="Stacking" value={String(spec.stackMode === VisualizationStackMode.VISUALIZATION_STACK_MODE_UNSPECIFIED ? VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE : spec.stackMode)} onValueChange={(next) => update({ stackMode: Number(next) as VisualizationStackMode })}>
        <SelectOption value={String(VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE)}>None</SelectOption>
        <SelectOption value={String(VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED)}>Stacked</SelectOption>
        <SelectOption value={String(VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED_100_PERCENT)}>Stacked 100%</SelectOption>
      </Select></div>
      <label><span>Time bucket width (seconds)</span><input aria-describedby={!durationValid ? durationErrorId : undefined} aria-invalid={!durationValid} inputMode="decimal" value={durationDraft} onChange={(event) => {
        const input = event.target.value;
        setDurationEdit({ base: storedDuration, draft: input });
        if (input === "") update({ timeBucketWidth: undefined });
        else {
          const duration = dashboardDurationFromInput(input);
          if (duration !== null) update({ timeBucketWidth: duration });
        }
      }} /></label>
      {!durationValid ? <p id={durationErrorId} role="alert">Enter a positive duration in seconds with at most nine decimal places.</p> : null}
      {!typeValid ? <p role="alert">The saved visualization type is unsupported.</p> : null}
      {!stackValid ? <p role="alert">The saved stacking mode is unsupported.</p> : null}
      <label className="operations-visualization-check"><input checked={spec.showLegend} type="checkbox" onChange={(event) => update({ showLegend: event.target.checked })} /><span>Show legend</span></label>
      <label className="operations-visualization-check"><input checked={spec.showDataLabels} type="checkbox" onChange={(event) => update({ showDataLabels: event.target.checked })} /><span>Show data labels</span></label>
    </fieldset>
  );
}

export interface DashboardVisualizationProps {
  capped?: boolean;
  complete?: boolean;
  onNextPage?: () => void;
  onPreviousPage?: () => void;
  pageNumber?: number;
  panelTitle: string;
  retainedTruncated?: boolean;
  rows: ResultRow[];
  schema: ResultSchema;
  spec: VisualizationSpec | undefined;
}

function keyedColumns(schema: ResultSchema) {
  const occurrences = new Map<string, number>();
  return schema.columns.map((column, index) => {
    const identity = `${column.fieldName}\u0000${column.displayName}\u0000${column.valueType}`;
    const occurrence = occurrences.get(identity) ?? 0;
    occurrences.set(identity, occurrence + 1);
    return { column, index, key: `${identity}\u0000${occurrence}` };
  });
}

function ResultTable({ caption, rows, schema }: { caption: string; rows: ResultRow[]; schema: ResultSchema }) {
  const columns = keyedColumns(schema);
  return <div className="table-wrap"><table className="table">
    <caption>{caption}</caption>
    <thead><tr>{columns.map(({ column, key }) => <th key={key} scope="col">{column.displayName || column.fieldName}</th>)}</tr></thead>
    <tbody>{rows.map((row) => <tr key={row.rowId || row.ordinal.toString()}>{columns.map(({ index, key }) => <td key={key}>{dashboardCellText(row.cells[index])}</td>)}</tr>)}</tbody>
  </table></div>;
}

function ErrorVisualization({ model }: { model: DashboardVisualizationError }) {
  return <div className="operations-visualization-error">
    <div aria-label="Visualization compatibility" role="alert"><strong>This result cannot be rendered with the saved visualization.</strong><ul>{model.issues.map((issue) => <li key={issue}>{issue}</li>)}</ul></div>
    <details><summary>Inspect rows</summary><ResultTable caption="Rows that could not be plotted" rows={model.rows} schema={model.schema} /></details>
  </div>;
}

function numericDomain(values: readonly number[]): { maximum: number; minimum: number } {
  let maximum = Number.NEGATIVE_INFINITY;
  let minimum = Number.POSITIVE_INFINITY;
  for (const value of values) {
    if (!Number.isFinite(value)) continue;
    maximum = Math.max(maximum, value);
    minimum = Math.min(minimum, value);
  }
  return minimum === Number.POSITIVE_INFINITY ? { maximum: 1, minimum: 0 } : { maximum, minimum };
}

function xCoordinates(model: DashboardCartesianVisualization): number[] {
  if (model.style === "column") {
    const rowWidth = (PLOT_RIGHT - PLOT_LEFT) / Math.max(1, model.rows.length);
    return model.rows.map((_row, index) => PLOT_LEFT + (index + 0.5) * rowWidth);
  }
  const numeric = model.rows.map((row) => row.x.coordinate);
  if (numeric.every((value): value is number => value !== null)) {
    const domain = numericDomain(numeric);
    if (domain.minimum === domain.maximum) return numeric.map(() => (PLOT_LEFT + PLOT_RIGHT) / 2);
    return numeric.map((value) => PLOT_LEFT + projectScaleValue(value, { ...domain, ticks: [] }) * (PLOT_RIGHT - PLOT_LEFT));
  }
  if (model.rows.length <= 1) return model.rows.map(() => (PLOT_LEFT + PLOT_RIGHT) / 2);
  return model.rows.map((_row, index) => PLOT_LEFT + (index / (model.rows.length - 1)) * (PLOT_RIGHT - PLOT_LEFT));
}

function markColorIndex(index: number): number {
  return (index % 12) + 1;
}

interface DashboardChartPath {
  area: string;
  color: number;
  key: string;
  line: string;
}

function stackedCoordinatesAreApproximate(
  model: DashboardCartesianVisualization,
  stacked: ReturnType<typeof stackChartRows>,
): boolean {
  if (model.stackMode === "none") return false;
  return stacked.some((row) => row.some((value) =>
    value.raw !== null && !Number.isFinite(value.start + value.raw),
  ));
}

function chartMarkKey(rowId: string, fieldName: string, suffix: string): string {
  return `${rowId}\u0000${fieldName}\u0000${suffix}`;
}

function CartesianChart({ model }: { model: DashboardCartesianVisualization }) {
  const [activeIndex, setActiveIndex] = useState(0);
  const inspection = useRef<HTMLDivElement>(null);
  const x = useMemo(() => xCoordinates(model), [model]);
  const stacked = useMemo(() => stackChartRows(model.rows.map((row) => row.values.map((value) => value.coordinate)), model.stackMode), [model]);
  const yScale = useMemo(() => linearTickScale(stackedChartDomain(stacked)), [stacked]);
  const y = (coordinate: number) => PLOT_BOTTOM - projectScaleValue(coordinate, yScale) * (PLOT_BOTTOM - PLOT_TOP);
  const groups = [...new Map(model.rows.map((row) => [row.group?.identity ?? "ungrouped", row.group?.label])).entries()];
  const groupIndex = new Map(groups.map(([identity], index) => [identity, index]));
  const safeActiveIndex = Math.min(activeIndex, Math.max(0, model.rows.length - 1));
  const positionApproximate = model.approximate || stackedCoordinatesAreApproximate(model, stacked);

  function inspectKey(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    let next = index;
    if (event.key === "ArrowRight" || event.key === "ArrowDown") next = Math.min(model.rows.length - 1, index + 1);
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp") next = Math.max(0, index - 1);
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = model.rows.length - 1;
    else return;
    event.preventDefault();
    setActiveIndex(next);
    inspection.current?.querySelector<HTMLButtonElement>(`[data-row-index="${next}"]`)?.focus({ preventScroll: true });
  }

  const paths: DashboardChartPath[] = [];
  for (const [renderedGroupIndex, [identity]] of groups.entries()) {
    for (const [seriesIndex] of model.series.entries()) {
      let segment: Array<{ end: number; rowIndex: number; start: number }> = [];
      const flush = () => {
        if (segment.length === 0) return;
        const firstRowId = model.rows[segment[0].rowIndex].id;
        const line = segment.map((point, index) => `${index === 0 ? "M" : "L"}${x[point.rowIndex].toFixed(2)},${y(point.end).toFixed(2)}`).join(" ");
        const baseline = segment.toReversed().map((point) => `L${x[point.rowIndex].toFixed(2)},${y(point.start).toFixed(2)}`).join(" ");
        paths.push({
          area: `${line} ${baseline} Z`,
          color: markColorIndex(renderedGroupIndex * model.series.length + seriesIndex),
          key: `${identity}\u0000${model.series[seriesIndex].fieldName}\u0000${firstRowId}`,
          line,
        });
        segment = [];
      };
      for (const [rowIndex, row] of model.rows.entries()) {
        if ((row.group?.identity ?? "ungrouped") !== identity) continue;
        const value = stacked[rowIndex]?.[seriesIndex];
        if (row.gapBefore || value === undefined || value.raw === null) flush();
        if (value !== undefined && value.raw !== null) segment.push({ end: value.end, rowIndex, start: value.start });
      }
      flush();
    }
  }
  const active = model.rows[safeActiveIndex];
  const chartLabel = `${model.title || "Untitled visualization"}, ${model.style} chart`;
  const axisDescription = `${model.xField.label} axis. ${model.series.map((series) => series.label).join(", ")} value axis from ${String(yScale.minimum)} to ${String(yScale.maximum)}.`;

  return <figure className="operations-dashboard-chart" data-chart-style={model.style}>
    <svg aria-label={chartLabel} preserveAspectRatio="none" role="img" viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`}>
      <desc>{axisDescription}</desc>
      <g aria-hidden="true" className="operations-dashboard-chart-grid">{yScale.ticks.map((tick) => model.style === "bar"
        ? <g key={tick}><line x1={PLOT_LEFT + projectScaleValue(tick, yScale) * (PLOT_RIGHT - PLOT_LEFT)} x2={PLOT_LEFT + projectScaleValue(tick, yScale) * (PLOT_RIGHT - PLOT_LEFT)} y1={PLOT_TOP} y2={PLOT_BOTTOM} /><text className="operations-dashboard-chart-tick" x={PLOT_LEFT + projectScaleValue(tick, yScale) * (PLOT_RIGHT - PLOT_LEFT)} y={PLOT_BOTTOM + 18}>{String(tick)}</text></g>
        : <g key={tick}><line x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={y(tick)} y2={y(tick)} /><text className="operations-dashboard-chart-tick operations-dashboard-chart-tick--y" x={PLOT_LEFT - 6} y={y(tick)}>{String(tick)}</text></g>)}</g>
      <text aria-hidden="true" className="operations-dashboard-chart-axis-title" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={VIEWBOX_HEIGHT - 4}>{model.style === "bar" ? model.series.map((series) => series.label).join(", ") : model.xField.label}</text>
      {model.style === "line" ? paths.map((item) => <path className="operations-dashboard-chart-line time-series-chart__series" data-series-color={item.color} d={item.line} key={item.key} />) : null}
      {model.style === "area" ? paths.map((item) => <path className="operations-dashboard-chart-area time-series-chart__series" data-series-color={item.color} d={item.area} key={item.key} />) : null}
      {model.rows.flatMap((row, rowIndex) => row.values.flatMap((value, seriesIndex) => {
        const stackedValue = stacked[rowIndex]?.[seriesIndex];
        if (value.coordinate === null || stackedValue === undefined || stackedValue.raw === null) return [];
        const color = markColorIndex(((groupIndex.get(row.group?.identity ?? "ungrouped") ?? 0) * model.series.length) + seriesIndex);
        const activate = () => setActiveIndex(rowIndex);
        if (model.style === "scatter" || model.style === "line" || model.style === "area") return [<circle className="operations-dashboard-chart-point time-series-chart__series" data-series-color={color} cx={x[rowIndex]} cy={y(stackedValue.end)} key={chartMarkKey(row.id, model.series[seriesIndex].fieldName, "point")} onClick={activate} onPointerEnter={activate} r="4" />];
        if (model.style === "column") {
          const stackedColumns = model.stackMode !== "none";
          const rowWidth = (PLOT_RIGHT - PLOT_LEFT) / Math.max(1, model.rows.length);
          const renderedSeries = stackedColumns ? 1 : model.series.length;
          const width = Math.max(2, (rowWidth * 0.72) / Math.max(1, renderedSeries));
          const offset = stackedColumns ? 0 : (seriesIndex - (model.series.length - 1) / 2) * width;
          const baseline = y(stackedValue.start);
          return [<rect className="operations-dashboard-chart-column time-series-chart__series" data-series-color={color} height={Math.max(1, Math.abs(y(stackedValue.end) - baseline))} key={chartMarkKey(row.id, model.series[seriesIndex].fieldName, "column")} onClick={activate} onPointerEnter={activate} width={width} x={x[rowIndex] + offset - width / 2} y={Math.min(y(stackedValue.end), baseline)} />];
        }
        if (model.style === "bar") {
          const rowHeight = (PLOT_BOTTOM - PLOT_TOP) / Math.max(1, model.rows.length);
          const stackedBars = model.stackMode !== "none";
          const renderedSeries = stackedBars ? 1 : model.series.length;
          const start = projectScaleValue(stackedValue.start, yScale);
          const end = projectScaleValue(stackedValue.end, yScale);
          return [<rect className="operations-dashboard-chart-bar time-series-chart__series" data-series-color={color} height={Math.max(1, rowHeight / renderedSeries - 2)} key={chartMarkKey(row.id, model.series[seriesIndex].fieldName, "bar")} onClick={activate} onPointerEnter={activate} width={Math.max(1, Math.abs(end - start) * (PLOT_RIGHT - PLOT_LEFT))} x={PLOT_LEFT + Math.min(start, end) * (PLOT_RIGHT - PLOT_LEFT)} y={PLOT_TOP + rowIndex * rowHeight + (stackedBars ? 0 : seriesIndex * rowHeight / renderedSeries)} />];
        }
        return [];
      }))}
      {model.showDataLabels ? model.rows.flatMap((row, rowIndex) => row.values.flatMap((value, seriesIndex) => {
        const stackedValue = stacked[rowIndex]?.[seriesIndex];
        if (value.coordinate === null || stackedValue === undefined || stackedValue.raw === null) return [];
        let labelX = x[rowIndex];
        let labelY = Math.max(PLOT_TOP, y(stackedValue.end) - 5);
        if (model.style === "column" && model.stackMode === "none") {
          const rowWidth = (PLOT_RIGHT - PLOT_LEFT) / Math.max(1, model.rows.length);
          const width = Math.max(2, (rowWidth * 0.72) / Math.max(1, model.series.length));
          labelX += (seriesIndex - (model.series.length - 1) / 2) * width;
        } else if (model.style === "bar") {
          const rowHeight = (PLOT_BOTTOM - PLOT_TOP) / Math.max(1, model.rows.length);
          const stackedBars = model.stackMode !== "none";
          const renderedSeries = stackedBars ? 1 : model.series.length;
          labelX = PLOT_LEFT + projectScaleValue(stackedValue.end, yScale) * (PLOT_RIGHT - PLOT_LEFT);
          labelY = PLOT_TOP + rowIndex * rowHeight + (stackedBars ? rowHeight / 2 : (seriesIndex + 0.5) * rowHeight / renderedSeries);
        }
        return [<text className="operations-dashboard-chart-label" key={chartMarkKey(row.id, model.series[seriesIndex].fieldName, "label")} x={labelX} y={labelY}>{value.exact}</text>];
      })) : null}
    </svg>
    <figcaption>{chartLabel}{positionApproximate ? ". Chart positions are approximate; displayed values are exact." : ". Displayed values are exact."}</figcaption>
    {model.showLegend ? <ul className="operations-dashboard-chart-legend">{groups.flatMap(([identity, group], renderedGroupIndex) => model.series.map((series, seriesIndex) => <li className="time-series-chart__series" data-series-color={markColorIndex(renderedGroupIndex * model.series.length + seriesIndex)} key={`${identity}-${series.fieldName}`}>{group ? `${group} · ` : ""}{series.label}</li>))}</ul> : null}
    <div aria-label="Chart row inspection" className="operations-dashboard-chart-inspection" ref={inspection}>{model.rows.map((row, index) => <button aria-label={`${row.x.label}; inspect chart values`} className={index === safeActiveIndex ? "button button--secondary button--compact operations-dashboard-chart-inspection-button" : "sr-only"} data-row-index={index} key={row.id} onClick={() => setActiveIndex(index)} onFocus={() => setActiveIndex(index)} onKeyDown={(event) => inspectKey(event, index)} tabIndex={index === safeActiveIndex ? 0 : -1} type="button">Inspect row {index + 1}: {row.x.label}</button>)}</div>
    {active ? <div aria-label={`Chart values for ${active.x.label}`} className="operations-dashboard-chart-values" role="group"><strong>{active.x.label}{active.group ? ` · ${active.group.label}` : ""}</strong><dl><div><dt>{model.xField.label}</dt><dd>{active.x.label}{active.x.approximate ? " (chart position approximate)" : ""}</dd></div>{model.series.map((series, index) => <div key={series.fieldName}><dt>{series.label}</dt><dd>{active.values[index].label}{active.values[index].approximate ? " (chart position approximate)" : ""}</dd></div>)}</dl></div> : null}
  </figure>;
}

function piePoint(centerX: number, centerY: number, radius: number, fraction: number): [number, number] {
  const angle = fraction * Math.PI * 2 - Math.PI / 2;
  return [centerX + Math.cos(angle) * radius, centerY + Math.sin(angle) * radius];
}

function cumulativeFractions(fractions: readonly number[]): number[] {
  const boundaries = [0];
  let cursor = 0;
  for (const fraction of fractions) {
    cursor += fraction;
    boundaries.push(cursor);
  }
  return boundaries;
}

function PieChart({ model }: { model: DashboardPieVisualization }) {
  const maximum = Math.max(...model.slices.map((slice) => slice.value.coordinate ?? 0));
  const normalized = model.slices.map((slice) => maximum === 0 ? 0 : (slice.value.coordinate ?? 0) / maximum);
  const total = normalized.reduce((sum, value) => sum + value, 0);
  const fractions = normalized.map((value) => value / total);
  const boundaries = cumulativeFractions(fractions);
  const chartLabel = `${model.title || "Untitled visualization"}, pie chart`;
  return <figure className="operations-dashboard-chart">
    {model.zeroTotal || total === 0 ? <p>No positive area is available to draw. Every source row remains listed below.</p> : <svg aria-label={chartLabel} role="img" viewBox="0 0 320 320">{model.slices.map((slice, index) => {
      const start = boundaries[index];
      const end = boundaries[index + 1];
      const [startX, startY] = piePoint(160, 160, 140, start);
      const [endX, endY] = piePoint(160, 160, 140, end);
      const path = end - start >= 0.999999 ? "M160,20 A140,140 0 1 1 159.99,20 Z" : `M160,160 L${startX},${startY} A140,140 0 ${end - start > 0.5 ? 1 : 0} 1 ${endX},${endY} Z`;
      const [labelX, labelY] = piePoint(160, 160, 92, start + (end - start) / 2);
      return <g className="time-series-chart__series" data-series-color={markColorIndex(index)} key={slice.id}><path className="operations-dashboard-chart-slice" d={path} />{model.showDataLabels ? <text className="operations-dashboard-chart-label" x={labelX} y={labelY}>{slice.value.exact}</text> : null}</g>;
    })}</svg>}
    <figcaption>{chartLabel}{model.approximate ? ". Chart positions are approximate; displayed values are exact." : ". Displayed values are exact."}</figcaption>
    {model.showLegend ? <ul className="operations-dashboard-chart-legend">{model.slices.map((slice, index) => <li className="time-series-chart__series" data-series-color={markColorIndex(index)} key={slice.id}>{slice.category.label}</li>)}</ul> : null}
    <details className="operations-dashboard-pie-values"><summary>Inspect pie values</summary><dl>{model.slices.map((slice) => <div key={slice.id}><dt>{slice.category.label}</dt><dd>{slice.value.exact}{slice.value.approximate ? " (chart position approximate)" : ""}</dd></div>)}</dl></details>
  </figure>;
}

function renderModel(model: DashboardVisualizationModel, props: DashboardVisualizationProps) {
  switch (model.kind) {
    case "error": return <ErrorVisualization model={model} />;
    case "single": return <div className="operations-dashboard-single-value"><span>{model.title || model.field.label}</span><strong>{model.value.exact}</strong>{model.value.approximate ? <small>Chart coordinate would be approximate; the displayed value is exact.</small> : null}</div>;
    case "pie": return <PieChart model={model} />;
    case "cartesian": return <CartesianChart model={model} />;
    case "table": return <><ResultTable caption={model.title || "Dashboard search results"} rows={model.rows} schema={model.schema} /><nav aria-label="Dashboard result pages" className="operations-dashboard-result-pages"><button className="button button--secondary button--compact" disabled={(props.pageNumber ?? 1) <= 1} type="button" onClick={props.onPreviousPage}>Previous results page</button><span>Page {props.pageNumber ?? 1}</span><button className="button button--secondary button--compact" disabled={props.onNextPage === undefined} type="button" onClick={props.onNextPage}>Next results page</button></nav></>;
  }
}

export function DashboardVisualization(props: DashboardVisualizationProps) {
  const model = useMemo(() => projectDashboardVisualization(props.spec, props.schema, props.rows), [props.rows, props.schema, props.spec]);
  const chartResult = model.kind !== "table";
  return <section aria-label={`${props.panelTitle || "Untitled panel"} visualization`} className="operations-dashboard-visualization">
    {props.capped ? <p className="operations-result-status">Showing the first 10,000 rows from one result snapshot. More rows are available.</p> : null}
    {props.complete === false && !props.capped ? <p className="operations-result-status">The retained snapshot reports incomplete row coverage.</p> : null}
    {chartResult && props.complete === true && !props.capped ? <p className="operations-result-status">Showing all {props.rows.length.toLocaleString()} retained rows from one complete result snapshot.</p> : null}
    {props.retainedTruncated ? <p className="operations-result-status">Server-retained results are truncated; additional query matches may be unavailable.</p> : null}
    {renderModel(model, props)}
  </section>;
}
