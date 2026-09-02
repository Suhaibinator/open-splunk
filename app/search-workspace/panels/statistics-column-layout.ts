export interface StatisticsColumnDefinition {
  defaultWidth: number | null;
  id: string;
  maximumWidth: number | null;
  minimumWidth: number | null;
}

export interface StatisticsColumnLayoutItem {
  id: string;
  maximumWidth: number | null;
  minimumWidth: number | null;
  visible: boolean;
  width: number | null;
}

export type StatisticsColumnLayout = readonly StatisticsColumnLayoutItem[];
export type StatisticsColumnLayoutStore = Map<string, StatisticsColumnLayout>;

function normalizedWidth(
  width: number | null,
  minimumWidth: number | null,
  maximumWidth: number | null,
): number | null {
  if (
    width === null
    || minimumWidth === null
    || maximumWidth === null
    || !Number.isFinite(width)
    || !Number.isFinite(minimumWidth)
    || !Number.isFinite(maximumWidth)
  ) return null;
  return Math.min(maximumWidth, Math.max(minimumWidth, Math.round(width)));
}

export function createColumnLayout(
  columns: readonly StatisticsColumnDefinition[],
): StatisticsColumnLayoutItem[] {
  const seen = new Set<string>();
  return columns.flatMap((column) => {
    if (seen.has(column.id)) return [];
    seen.add(column.id);
    return [{
      id: column.id,
      maximumWidth: column.maximumWidth,
      minimumWidth: column.minimumWidth,
      visible: true,
      width: normalizedWidth(
        column.defaultWidth,
        column.minimumWidth,
        column.maximumWidth,
      ),
    }];
  });
}

export function reconcileColumnLayout(
  layout: StatisticsColumnLayout,
  columns: readonly StatisticsColumnDefinition[],
): StatisticsColumnLayoutItem[] {
  const existingById = new Map(layout.map((column) => [column.id, column]));
  const reconciled = createColumnLayout(columns).map((column) => {
    const existing = existingById.get(column.id);
    return existing === undefined
      ? column
      : {
        id: column.id,
        maximumWidth: column.maximumWidth,
        minimumWidth: column.minimumWidth,
        visible: existing.visible,
        width: normalizedWidth(
          existing.width ?? column.width,
          column.minimumWidth,
          column.maximumWidth,
        ),
      };
  });
  if (reconciled.length > 0 && !reconciled.some((column) => column.visible)) {
    reconciled[0] = { ...reconciled[0], visible: true };
  }
  return reconciled;
}

export function resizeColumn(
  layout: StatisticsColumnLayout,
  id: string,
  deltaPx: number,
): StatisticsColumnLayoutItem[] {
  if (!Number.isFinite(deltaPx) || deltaPx === 0) return [...layout];
  return layout.map((column) => {
    if (column.id !== id || column.width === null) return column;
    return {
      ...column,
      width: normalizedWidth(
        column.width + deltaPx,
        column.minimumWidth,
        column.maximumWidth,
      ),
    };
  });
}

export function toggleColumn(
  layout: StatisticsColumnLayout,
  id: string,
): StatisticsColumnLayoutItem[] {
  const target = layout.find((column) => column.id === id);
  if (target?.visible === true && visibleColumns(layout).length === 1) {
    return [...layout];
  }
  return layout.map((column) => column.id === id
    ? { ...column, visible: !column.visible }
    : { ...column });
}

export function visibleColumns(
  layout: StatisticsColumnLayout,
): StatisticsColumnLayoutItem[] {
  return layout.filter((column) => column.visible);
}

export function visibleColumnWidth(layout: StatisticsColumnLayout): number | null {
  let total = 0;
  for (const column of visibleColumns(layout)) {
    if (column.width === null) return null;
    total += column.width;
  }
  return total;
}
