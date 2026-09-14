import type { Duration } from "@/gen/ts/google/protobuf/duration";
import {
  type ResultColumn,
  type ResultRow,
  type ResultSchema,
  VisualizationStackMode,
  VisualizationType,
  type VisualizationSpec,
} from "@/gen/ts/open_splunk/result";
import type { TypedValue } from "@/gen/ts/open_splunk/value";
import {
  chartNumericValue,
  type ChartNumericValue,
} from "@/lib/search/backend-data";

export type DashboardCartesianStyle = "area" | "bar" | "column" | "line" | "scatter";
export type DashboardStackMode = "none" | "stacked" | "stacked100";

export interface DashboardField {
  fieldName: string;
  label: string;
}

export interface DashboardDisplayValue {
  approximate: boolean;
  coordinate: number | null;
  exact: string;
  identity: string;
  label: string;
}

export interface DashboardProjectedRow {
  gapBefore: boolean;
  group?: DashboardDisplayValue;
  id: string;
  source: ResultRow;
  values: DashboardDisplayValue[];
  x: DashboardDisplayValue;
}

export interface DashboardTableVisualization {
  columns: DashboardField[];
  kind: "table";
  rows: ResultRow[];
  schema: ResultSchema;
  title?: string;
}

export interface DashboardCartesianVisualization {
  approximate: boolean;
  kind: "cartesian";
  rows: DashboardProjectedRow[];
  series: DashboardField[];
  showDataLabels: boolean;
  showLegend: boolean;
  stackMode: DashboardStackMode;
  style: DashboardCartesianStyle;
  title?: string;
  xField: DashboardField;
}

export interface DashboardPieSlice {
  category: DashboardDisplayValue;
  id: string;
  source: ResultRow;
  value: DashboardDisplayValue;
}

export interface DashboardPieVisualization {
  approximate: boolean;
  kind: "pie";
  series: DashboardField;
  showDataLabels: boolean;
  showLegend: boolean;
  slices: DashboardPieSlice[];
  title?: string;
  xField: DashboardField;
  zeroTotal: boolean;
}

export interface DashboardSingleValueVisualization {
  field: DashboardField;
  kind: "single";
  source: ResultRow;
  title?: string;
  value: DashboardDisplayValue;
}

export interface DashboardVisualizationError {
  columns: DashboardField[];
  issues: string[];
  kind: "error";
  rows: ResultRow[];
  schema: ResultSchema;
  title?: string;
}

export type DashboardVisualizationModel =
  | DashboardTableVisualization
  | DashboardCartesianVisualization
  | DashboardPieVisualization
  | DashboardSingleValueVisualization
  | DashboardVisualizationError;

interface ResolvedField extends DashboardField {
  column: ResultColumn;
  index: number;
}

interface ProjectedNumericValue extends DashboardDisplayValue {
  coordinate: number;
  timeCoordinate?: ExactTimeCoordinate;
}

interface ExactTimeCoordinate {
  denominator: bigint;
  numerator: bigint;
}

function fieldLabel(column: ResultColumn): string {
  return column.displayName || column.fieldName;
}

function fieldsFromSchema(schema: ResultSchema): DashboardField[] {
  return schema.columns.map((column) => ({
    fieldName: column.fieldName,
    label: fieldLabel(column),
  }));
}

function formatDuration(duration: Duration): string {
  const totalNanos = duration.seconds * 1_000_000_000n + BigInt(duration.nanos);
  const negative = totalNanos < 0n;
  const absolute = negative ? -totalNanos : totalNanos;
  const seconds = absolute / 1_000_000_000n;
  const nanos = absolute % 1_000_000_000n;
  return `${negative ? "-" : ""}${seconds.toString()}.${nanos.toString().padStart(9, "0")}s`;
}

function formatTypedValue(value: TypedValue | undefined): string {
  switch (value?.kind?.$case) {
    case "nullValue": return "null";
    case "missingValue": return "missing";
    case "stringValue": return value.kind.value;
    case "sint64Value":
    case "uint64Value": return value.kind.value.toString();
    case "doubleValue": return Number.isFinite(value.kind.value) ? String(value.kind.value) : "non-finite number";
    case "boolValue": return value.kind.value ? "true" : "false";
    case "timestampValue": return Number.isFinite(value.kind.value.getTime()) ? value.kind.value.toISOString() : "invalid timestamp";
    case "durationValue": return formatDuration(value.kind.value);
    case "decimalValue": return value.kind.value.value;
    case "bytesValue": return `0x${Array.from(value.kind.value, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
    case "listValue": return `[${value.kind.value.values.map(formatTypedValue).join(", ")}]`;
    case "objectValue": return JSON.stringify(Object.fromEntries(
      value.kind.value.fields.map((field) => [field.name, formatTypedValue(field.value)]),
    ));
    default: return "unset";
  }
}

function scalarValue(value: TypedValue | undefined): DashboardDisplayValue | null {
  switch (value?.kind?.$case) {
    case "stringValue": return { approximate: false, coordinate: null, exact: value.kind.value, identity: `string:${value.kind.value}`, label: value.kind.value };
    case "sint64Value": return { approximate: false, coordinate: null, exact: value.kind.value.toString(), identity: `sint64:${value.kind.value.toString()}`, label: value.kind.value.toString() };
    case "uint64Value": return { approximate: false, coordinate: null, exact: value.kind.value.toString(), identity: `uint64:${value.kind.value.toString()}`, label: value.kind.value.toString() };
    case "doubleValue":
      if (!Number.isFinite(value.kind.value)) return null;
      return {
        approximate: false,
        coordinate: null,
        exact: formatTypedValue(value),
        identity: `double:${Object.is(value.kind.value, -0) ? "-0" : String(value.kind.value)}`,
        label: formatTypedValue(value),
      };
    case "boolValue": return { approximate: false, coordinate: null, exact: formatTypedValue(value), identity: `bool:${value.kind.value}`, label: formatTypedValue(value) };
    case "timestampValue": {
      const milliseconds = value.kind.value.getTime();
      if (!Number.isFinite(milliseconds)) return null;
      const exact = value.kind.value.toISOString();
      return { approximate: false, coordinate: null, exact, identity: `timestamp:${exact}`, label: exact };
    }
    case "durationValue": {
      if (!validProtoDuration(value.kind.value)) return null;
      const exact = formatDuration(value.kind.value);
      return { approximate: false, coordinate: null, exact, identity: `duration:${exact}`, label: exact };
    }
    case "decimalValue": {
      if (chartNumericValue(value) === null) return null;
      return { approximate: false, coordinate: null, exact: value.kind.value.value, identity: `decimal:${value.kind.value.value}`, label: value.kind.value.value };
    }
    case "bytesValue": {
      const exact = formatTypedValue(value);
      return { approximate: false, coordinate: null, exact, identity: `bytes:${exact}`, label: exact };
    }
    default:
      return null;
  }
}

function numericKind(value: TypedValue | undefined): boolean {
  const kind = value?.kind?.$case;
  return kind === "sint64Value"
    || kind === "uint64Value"
    || kind === "doubleValue"
    || kind === "decimalValue";
}

function exactNumericText(value: TypedValue): string {
  switch (value.kind?.$case) {
    case "sint64Value":
    case "uint64Value": return value.kind.value.toString();
    case "doubleValue": return String(value.kind.value);
    case "decimalValue": return value.kind.value.value;
    default: return formatTypedValue(value);
  }
}

function numericValue(value: TypedValue | undefined): ProjectedNumericValue | null {
  if (!numericKind(value) || value === undefined) return null;
  const numeric: ChartNumericValue | null = chartNumericValue(value);
  if (numeric === null) return null;
  const exact = exactNumericText(value);
  return {
    approximate: numeric.approximate,
    coordinate: numeric.coordinate,
    exact,
    identity: `${value.kind?.$case}:${exact}`,
    label: exact,
    timeCoordinate: numericTimeCoordinate(value),
  };
}

function decimalTimeCoordinate(source: string): ExactTimeCoordinate | undefined {
  const match = /^([+-]?)(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/iu.exec(source.trim());
  if (match === null) return undefined;
  const digitsText = `${match[2]}${match[3] ?? ""}`;
  if (digitsText.length > 768) return undefined;
  const exponent = Number(match[4] ?? "0");
  if (!Number.isSafeInteger(exponent)) return undefined;
  const scale = (match[3]?.length ?? 0) - exponent;
  if (Math.abs(scale) > 768) return undefined;
  const signedDigits = (match[1] === "-" ? -1n : 1n) * BigInt(digitsText);
  return scale >= 0
    ? { denominator: 10n ** BigInt(scale), numerator: signedDigits }
    : { denominator: 1n, numerator: signedDigits * (10n ** BigInt(-scale)) };
}

function numericTimeCoordinate(value: TypedValue): ExactTimeCoordinate | undefined {
  switch (value.kind?.$case) {
    case "sint64Value":
    case "uint64Value": return { denominator: 1n, numerator: value.kind.value };
    case "doubleValue": return decimalTimeCoordinate(String(value.kind.value));
    case "decimalValue": return decimalTimeCoordinate(value.kind.value.value);
    default: return undefined;
  }
}

function timestampValue(value: TypedValue | undefined): ProjectedNumericValue | null {
  if (value?.kind?.$case !== "timestampValue") return null;
  const milliseconds = value.kind.value.getTime();
  if (!Number.isFinite(milliseconds)) return null;
  const exact = value.kind.value.toISOString();
  return {
    approximate: false,
    coordinate: milliseconds,
    exact,
    identity: `timestamp:${exact}`,
    label: exact,
    timeCoordinate: { denominator: 1_000n, numerator: BigInt(milliseconds) },
  };
}

function nullGapValue(value: TypedValue | undefined): DashboardDisplayValue | null {
  if (value?.kind?.$case !== "nullValue" && value?.kind?.$case !== "missingValue") return null;
  const exact = value.kind.$case === "nullValue" ? "null" : "missing";
  return { approximate: false, coordinate: null, exact, identity: value.kind.$case, label: exact };
}

const MAXIMUM_PROTO_DURATION_SECONDS = 315_576_000_000n;

function validProtoDuration(duration: Duration): boolean {
  return duration.seconds >= -MAXIMUM_PROTO_DURATION_SECONDS
    && duration.seconds <= MAXIMUM_PROTO_DURATION_SECONDS
    && Number.isSafeInteger(duration.nanos)
    && Math.abs(duration.nanos) < 1_000_000_000
    && (duration.seconds === 0n || duration.nanos === 0 || Math.sign(duration.nanos) === (duration.seconds < 0n ? -1 : 1));
}

function durationNanoseconds(duration: Duration | undefined): bigint | null {
  if (duration === undefined || !validProtoDuration(duration)) return null;
  const nanoseconds = duration.seconds * 1_000_000_000n + BigInt(duration.nanos);
  return nanoseconds > 0n ? nanoseconds : null;
}

export function dashboardDurationIsValid(duration: Duration | undefined): boolean {
  return duration === undefined || durationNanoseconds(duration) !== null;
}

export function dashboardDurationInput(duration: Duration | undefined): string {
  if (duration === undefined) return "";
  const nanoseconds = duration.seconds * 1_000_000_000n + BigInt(duration.nanos);
  const negative = nanoseconds < 0n;
  const absolute = negative ? -nanoseconds : nanoseconds;
  const whole = absolute / 1_000_000_000n;
  const fraction = (absolute % 1_000_000_000n).toString().padStart(9, "0").replace(/0+$/u, "");
  return `${negative ? "-" : ""}${whole.toString()}${fraction ? `.${fraction}` : ""}`;
}

export function dashboardDurationFromInput(input: string): Duration | null {
  const match = /^(\d+)(?:\.(\d{1,9}))?$/u.exec(input.trim());
  if (match === null) return null;
  const seconds = BigInt(match[1]);
  const nanos = Number((match[2] ?? "").padEnd(9, "0"));
  const duration = { seconds, nanos };
  return durationNanoseconds(duration) === null ? null : duration;
}

function resolveField(schema: ResultSchema, fieldName: string | undefined, role: string, issues: string[]): ResolvedField | null {
  if (fieldName === undefined || fieldName.length === 0) {
    issues.push(`${role} is required.`);
    return null;
  }
  const matches = schema.columns.flatMap((column, index) =>
    column.fieldName === fieldName ? [{ column, index }] : [],
  );
  if (matches.length === 0) {
    issues.push(`${role} “${fieldName}” is not present in these results.`);
    return null;
  }
  if (matches.length > 1) {
    issues.push(`${role} “${fieldName}” is ambiguous because the result schema repeats that field.`);
    return null;
  }
  return { ...matches[0], fieldName, label: fieldLabel(matches[0].column) };
}

function resolveYFields(schema: ResultSchema, names: readonly string[], issues: string[]): ResolvedField[] {
  if (names.length === 0) {
    issues.push("At least one Y field is required.");
    return [];
  }
  const seen = new Set<string>();
  const fields: ResolvedField[] = [];
  for (const name of names) {
    if (seen.has(name)) {
      issues.push(`Y field “${name}” is configured more than once.`);
      continue;
    }
    seen.add(name);
    const field = resolveField(schema, name, "Y field", issues);
    if (field !== null) fields.push(field);
  }
  return fields;
}

function errorModel(spec: VisualizationSpec, schema: ResultSchema, rows: ResultRow[], issues: string[]): DashboardVisualizationError {
  return {
    columns: fieldsFromSchema(schema),
    issues: [...new Set(issues)],
    kind: "error",
    rows,
    schema,
    title: spec.title,
  };
}

function stackMode(spec: VisualizationSpec, issues: string[]): DashboardStackMode {
  switch (spec.stackMode) {
    case VisualizationStackMode.VISUALIZATION_STACK_MODE_UNSPECIFIED:
    case VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE: return "none";
    case VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED: return "stacked";
    case VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED_100_PERCENT: return "stacked100";
    default:
      issues.push("The visualization uses an unsupported stacking mode.");
      return "none";
  }
}

function validatePresentationOptions(spec: VisualizationSpec, allowsStacking: boolean, allowsTimeBucket: boolean, issues: string[]): DashboardStackMode {
  const rendered = stackMode(spec, issues);
  if (!allowsStacking && rendered !== "none") issues.push("This visualization type does not support stacking.");
  if (spec.timeBucketWidth !== undefined) {
    if (durationNanoseconds(spec.timeBucketWidth) === null) {
      issues.push("Time bucket width must be greater than zero with nanosecond precision.");
    } else if (!allowsTimeBucket) {
      issues.push("This visualization type does not support time bucket width.");
    }
  }
  return rendered;
}

function cartesianStyle(type: VisualizationType): DashboardCartesianStyle | null {
  switch (type) {
    case VisualizationType.VISUALIZATION_TYPE_LINE: return "line";
    case VisualizationType.VISUALIZATION_TYPE_AREA: return "area";
    case VisualizationType.VISUALIZATION_TYPE_COLUMN: return "column";
    case VisualizationType.VISUALIZATION_TYPE_BAR: return "bar";
    case VisualizationType.VISUALIZATION_TYPE_SCATTER: return "scatter";
    default: return null;
  }
}

function projectCartesian(spec: VisualizationSpec, schema: ResultSchema, rows: ResultRow[], style: DashboardCartesianStyle): DashboardVisualizationModel {
  const issues: string[] = [];
  const xField = resolveField(schema, spec.xField, "X field", issues);
  const yFields = resolveYFields(schema, spec.yFields, issues);
  const groupField = spec.seriesField === undefined ? null : resolveField(schema, spec.seriesField, "Series field", issues);
  const renderedStackMode = validatePresentationOptions(spec, style !== "scatter", style !== "scatter", issues);
  const bucketNanoseconds = spec.timeBucketWidth === undefined ? undefined : durationNanoseconds(spec.timeBucketWidth);
  if (issues.length > 0 || xField === null || yFields.length !== spec.yFields.length) return errorModel(spec, schema, rows, issues);

  const projected: DashboardProjectedRow[] = [];
  const previousTimeCoordinates = new Map<string, ExactTimeCoordinate>();
  for (const [rowIndex, row] of rows.entries()) {
    const rawX = row.cells[xField.index];
    const numericX = numericValue(rawX);
    const timestampX = timestampValue(rawX);
    const x = style === "scatter" ? numericX : timestampX ?? numericX ?? scalarValue(rawX);
    if (x === null) {
      issues.push(`Row ${rowIndex + 1} has a non-${style === "scatter" ? "numeric" : "scalar"} X field “${xField.fieldName}”.`);
      continue;
    }
    let timeCoordinate: ExactTimeCoordinate | undefined;
    if (bucketNanoseconds !== undefined) {
      const timeValue = timestampX ?? numericX;
      if (timeValue === null || timeValue.timeCoordinate === undefined) {
        issues.push(`Row ${rowIndex + 1} must have a timestamp or numeric-seconds X field when time bucket width is set.`);
        continue;
      }
      timeCoordinate = timeValue.timeCoordinate;
    }
    const group = groupField === null ? undefined : scalarValue(row.cells[groupField.index]);
    if (groupField !== null && group === null) {
      issues.push(`Row ${rowIndex + 1} has a non-scalar Series field “${groupField.fieldName}”.`);
      continue;
    }
    const values: DashboardDisplayValue[] = [];
    for (const yField of yFields) {
      const cell = row.cells[yField.index];
      const gap = nullGapValue(cell);
      const numeric = numericValue(cell);
      if (gap === null && numeric === null) {
        issues.push(`Row ${rowIndex + 1} has a non-numeric Y field “${yField.fieldName}”.`);
        continue;
      }
      values.push(gap ?? numeric!);
    }
    if (values.length !== yFields.length) continue;
    const groupIdentity = group?.identity ?? "ungrouped";
    const previousTimeCoordinate = previousTimeCoordinates.get(groupIdentity);
    const gapBefore = bucketNanoseconds != null
      && previousTimeCoordinate !== undefined
      && timeCoordinate !== undefined
      && exactTimeGapExceeds(previousTimeCoordinate, timeCoordinate, bucketNanoseconds);
    projected.push({ gapBefore, group: group ?? undefined, id: row.rowId || `row-${row.ordinal.toString()}`, source: row, values, x });
    if (timeCoordinate !== undefined) previousTimeCoordinates.set(groupIdentity, timeCoordinate);
  }
  if (issues.length > 0 || projected.length !== rows.length) return errorModel(spec, schema, rows, issues);
  return {
    approximate: projected.some((row) => row.x.approximate || row.group?.approximate === true || row.values.some((value) => value.approximate)),
    kind: "cartesian",
    rows: projected,
    series: yFields.map(({ fieldName, label }) => ({ fieldName, label })),
    showDataLabels: spec.showDataLabels,
    showLegend: spec.showLegend,
    stackMode: renderedStackMode,
    style,
    title: spec.title,
    xField: { fieldName: xField.fieldName, label: xField.label },
  };
}

function exactTimeGapExceeds(previous: ExactTimeCoordinate, current: ExactTimeCoordinate, bucketNanoseconds: bigint): boolean {
  const differenceNumerator = current.numerator * previous.denominator
    - previous.numerator * current.denominator;
  const differenceDenominator = current.denominator * previous.denominator;
  return differenceNumerator * 1_000_000_000n > bucketNanoseconds * differenceDenominator;
}

function projectPie(spec: VisualizationSpec, schema: ResultSchema, rows: ResultRow[]): DashboardVisualizationModel {
  const issues: string[] = [];
  validatePresentationOptions(spec, false, false, issues);
  const xField = resolveField(schema, spec.xField, "X field", issues);
  const yFields = resolveYFields(schema, spec.yFields, issues);
  if (spec.yFields.length !== 1) issues.push("Pie visualizations require exactly one Y field.");
  if (issues.length > 0 || xField === null || yFields.length !== 1) return errorModel(spec, schema, rows, issues);
  const yField = yFields[0];
  const slices: DashboardPieSlice[] = [];
  for (const [rowIndex, row] of rows.entries()) {
    const category = scalarValue(row.cells[xField.index]);
    const value = numericValue(row.cells[yField.index]);
    if (category === null) issues.push(`Row ${rowIndex + 1} has a non-scalar X field “${xField.fieldName}”.`);
    if (value === null) issues.push(`Row ${rowIndex + 1} has a non-numeric Y field “${yField.fieldName}”.`);
    else if (numericValueIsNegative(row.cells[yField.index])) issues.push(`Row ${rowIndex + 1} has a negative pie value in “${yField.fieldName}”.`);
    if (category !== null && value !== null && !numericValueIsNegative(row.cells[yField.index])) {
      slices.push({ category, id: row.rowId || `row-${row.ordinal.toString()}`, source: row, value });
    }
  }
  if (issues.length > 0 || slices.length !== rows.length) return errorModel(spec, schema, rows, issues);
  return {
    approximate: slices.some((slice) => slice.value.approximate),
    kind: "pie",
    series: { fieldName: yField.fieldName, label: yField.label },
    showDataLabels: spec.showDataLabels,
    showLegend: spec.showLegend,
    slices,
    title: spec.title,
    xField: { fieldName: xField.fieldName, label: xField.label },
    zeroTotal: slices.every((slice) => slice.value.coordinate === 0),
  };
}

function numericValueIsNegative(value: TypedValue | undefined): boolean {
  switch (value?.kind?.$case) {
    case "sint64Value": return value.kind.value < 0n;
    case "doubleValue": return value.kind.value < 0;
    case "decimalValue": {
      const text = value.kind.value.value.trim();
      return text.startsWith("-") && /[1-9]/u.test(text.replace(/[eE][+-]?\d+$/u, ""));
    }
    default: return false;
  }
}

function projectSingle(spec: VisualizationSpec, schema: ResultSchema, rows: ResultRow[]): DashboardVisualizationModel {
  const issues: string[] = [];
  validatePresentationOptions(spec, false, false, issues);
  const yFields = resolveYFields(schema, spec.yFields, issues);
  if (spec.yFields.length !== 1) issues.push("Single value visualizations require exactly one Y field.");
  if (rows.length !== 1) issues.push("Single value visualizations require exactly one result row.");
  if (issues.length > 0 || yFields.length !== 1 || rows.length !== 1) return errorModel(spec, schema, rows, issues);
  const field = yFields[0];
  const value = scalarValue(rows[0].cells[field.index]);
  if (value === null) {
    issues.push(`The single result row has a non-scalar Y field “${field.fieldName}”.`);
    return errorModel(spec, schema, rows, issues);
  }
  return {
    field: { fieldName: field.fieldName, label: field.label },
    kind: "single",
    source: rows[0],
    title: spec.title,
    value: numericValue(rows[0].cells[field.index]) ?? value,
  };
}

export function projectDashboardVisualization(spec: VisualizationSpec | undefined, schema: ResultSchema, rows: ResultRow[]): DashboardVisualizationModel {
  if (spec === undefined) {
    return { columns: fieldsFromSchema(schema), kind: "table", rows, schema, title: undefined };
  }
  if (spec.type === VisualizationType.VISUALIZATION_TYPE_TABLE) {
    const issues: string[] = [];
    validatePresentationOptions(spec, false, false, issues);
    return issues.length === 0
      ? { columns: fieldsFromSchema(schema), kind: "table", rows, schema, title: spec.title }
      : errorModel(spec, schema, rows, issues);
  }
  const style = cartesianStyle(spec.type);
  if (style !== null) return projectCartesian(spec, schema, rows, style);
  if (spec.type === VisualizationType.VISUALIZATION_TYPE_PIE) return projectPie(spec, schema, rows);
  if (spec.type === VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE) return projectSingle(spec, schema, rows);
  return errorModel(spec, schema, rows, ["The visualization type is unspecified or unsupported."]);
}

export function dashboardVisualizationTypeLabel(type: VisualizationType | undefined): string {
  switch (type) {
    case undefined:
    case VisualizationType.VISUALIZATION_TYPE_TABLE: return "Table";
    case VisualizationType.VISUALIZATION_TYPE_LINE: return "Line";
    case VisualizationType.VISUALIZATION_TYPE_AREA: return "Area";
    case VisualizationType.VISUALIZATION_TYPE_COLUMN: return "Column";
    case VisualizationType.VISUALIZATION_TYPE_BAR: return "Bar";
    case VisualizationType.VISUALIZATION_TYPE_PIE: return "Pie";
    case VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE: return "Single value";
    case VisualizationType.VISUALIZATION_TYPE_SCATTER: return "Scatter";
    default: return "Unsupported";
  }
}

export function dashboardCellText(value: TypedValue | undefined): string {
  return formatTypedValue(value);
}
