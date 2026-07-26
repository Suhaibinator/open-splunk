export const VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS = 100;
export const VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_CELLS = 2_048;
export const VIRTUAL_TABLE_OVERSCAN_ROWS = 6;
export const VIRTUAL_TABLE_VIEWPORT_HEIGHT = 520;

export interface VirtualTableWindowInput {
  columnCount: number;
  rowCount: number;
  rowHeight: number;
  scrollTop: number;
  viewportHeight: number;
  overscan?: number;
}

export interface VirtualTableWindow {
  virtualized: boolean;
  startIndex: number;
  endIndex: number;
  paddingTop: number;
  paddingBottom: number;
}

function normalizedInteger(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

export function shouldVirtualizeTable(rowCount: number, columnCount: number): boolean {
  const normalizedRowCount = normalizedInteger(rowCount);
  const normalizedColumnCount = Math.max(1, normalizedInteger(columnCount));
  return normalizedRowCount > VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_ROWS
    || normalizedRowCount * normalizedColumnCount
      > VIRTUAL_TABLE_MAXIMUM_UNVIRTUALIZED_CELLS;
}

export function maximumVirtualTableScrollTop({
  columnCount,
  rowCount,
  rowHeight,
  viewportHeight,
}: Pick<
  VirtualTableWindowInput,
  "columnCount" | "rowCount" | "rowHeight" | "viewportHeight"
>): number {
  const normalizedRowCount = normalizedInteger(rowCount);
  if (!shouldVirtualizeTable(normalizedRowCount, columnCount)) return 0;
  const normalizedRowHeight = Number.isFinite(rowHeight) && rowHeight > 0 ? rowHeight : 1;
  const normalizedViewportHeight = Number.isFinite(viewportHeight)
    ? Math.max(0, viewportHeight)
    : 0;
  return Math.max(
    0,
    normalizedRowCount * normalizedRowHeight - normalizedViewportHeight,
  );
}

export function calculateVirtualTableWindow({
  columnCount,
  rowCount,
  rowHeight,
  scrollTop,
  viewportHeight,
  overscan = VIRTUAL_TABLE_OVERSCAN_ROWS,
}: VirtualTableWindowInput): VirtualTableWindow {
  const normalizedRowCount = normalizedInteger(rowCount);
  if (!shouldVirtualizeTable(normalizedRowCount, columnCount)) {
    return {
      virtualized: false,
      startIndex: 0,
      endIndex: normalizedRowCount,
      paddingTop: 0,
      paddingBottom: 0,
    };
  }

  const normalizedRowHeight = Number.isFinite(rowHeight) && rowHeight > 0 ? rowHeight : 1;
  const normalizedViewportHeight = Number.isFinite(viewportHeight)
    ? Math.max(0, viewportHeight)
    : 0;
  const normalizedOverscan = normalizedInteger(overscan);
  const maximumScrollTop = maximumVirtualTableScrollTop({
    columnCount,
    rowCount: normalizedRowCount,
    rowHeight: normalizedRowHeight,
    viewportHeight: normalizedViewportHeight,
  });
  const requestedScrollTop = Number.isNaN(scrollTop) ? 0 : Math.max(0, scrollTop);
  const normalizedScrollTop = Math.min(requestedScrollTop, maximumScrollTop);
  const firstVisibleRow = Math.floor(normalizedScrollTop / normalizedRowHeight);
  const firstExcludedVisibleRow = Math.max(
    firstVisibleRow + 1,
    Math.ceil((normalizedScrollTop + normalizedViewportHeight) / normalizedRowHeight),
  );
  const startIndex = Math.max(0, firstVisibleRow - normalizedOverscan);
  const endIndex = Math.min(
    normalizedRowCount,
    firstExcludedVisibleRow + normalizedOverscan,
  );

  return {
    virtualized: true,
    startIndex,
    endIndex,
    paddingTop: startIndex * normalizedRowHeight,
    paddingBottom: (normalizedRowCount - endIndex) * normalizedRowHeight,
  };
}
