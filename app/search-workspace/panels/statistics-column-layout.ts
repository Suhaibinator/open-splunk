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

export interface StatisticsColumnLayoutDomain {
  byId: ReadonlyMap<string, StatisticsColumnLayoutItem>;
  layout: StatisticsColumnLayout;
  visible: StatisticsColumnLayout;
}

export interface StatisticsColumnWindow<Column extends { id: string }> {
  columns: readonly Column[];
  layout: StatisticsColumnLayout;
}

interface StoredColumnOverride {
  id: string;
  visible: boolean;
  width: number | null;
}

interface StoredColumnLayout {
  bytes: number;
  overrides: readonly StoredColumnOverride[];
}

interface StatisticsColumnLayoutStoreOptions {
  maximumBytes?: number;
  maximumEntries?: number;
}

const DEFAULT_MAXIMUM_LAYOUT_BYTES = 256 * 1_024;
const DEFAULT_MAXIMUM_LAYOUT_ENTRIES = 16;
const STORED_LAYOUT_BASE_BYTES = 32;
const STORED_OVERRIDE_BASE_BYTES = 24;

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

export function createColumnLayoutDomain(
  layout: StatisticsColumnLayout,
  columns: readonly StatisticsColumnDefinition[],
): StatisticsColumnLayoutDomain {
  const reconciled = reconcileColumnLayout(layout, columns);
  const byId = new Map(reconciled.map((column) => [column.id, column]));
  return {
    byId,
    layout: reconciled,
    visible: reconciled.filter((column) => column.visible),
  };
}

/** Derives a render window with one indexed layout lookup per candidate column. */
export function selectColumnLayoutWindow<Column extends { id: string }>(
  columns: readonly Column[],
  layoutById: ReadonlyMap<string, StatisticsColumnLayoutItem>,
  offset: number,
  windowSize: number,
): StatisticsColumnWindow<Column> {
  const end = Math.min(columns.length, offset + windowSize);
  const windowColumns: Column[] = [];
  const visibleLayout: StatisticsColumnLayoutItem[] = [];
  for (let index = offset; index < end; index += 1) {
    const column = columns[index];
    if (column === undefined) continue;
    const layoutItem = layoutById.get(column.id);
    if (layoutItem?.visible !== true) continue;
    windowColumns.push(column);
    visibleLayout.push(layoutItem);
  }
  return { columns: windowColumns, layout: visibleLayout };
}

/**
 * Retains only user overrides, bounded by whole-query LRU entries and metadata
 * bytes. An oversized query remains intact in the mounted panel but is not
 * partially persisted.
 */
export class StatisticsColumnLayoutStore {
  private readonly layouts = new Map<string, StoredColumnLayout>();
  private readonly maximumBytes: number;
  private readonly maximumEntries: number;
  private totalBytes = 0;

  constructor(options: StatisticsColumnLayoutStoreOptions = {}) {
    const maximumBytes = options.maximumBytes ?? DEFAULT_MAXIMUM_LAYOUT_BYTES;
    const maximumEntries = options.maximumEntries ?? DEFAULT_MAXIMUM_LAYOUT_ENTRIES;
    this.maximumBytes = Number.isFinite(maximumBytes)
      ? Math.max(0, Math.floor(maximumBytes))
      : DEFAULT_MAXIMUM_LAYOUT_BYTES;
    this.maximumEntries = Number.isFinite(maximumEntries)
      ? Math.max(0, Math.floor(maximumEntries))
      : DEFAULT_MAXIMUM_LAYOUT_ENTRIES;
  }

  get size(): number {
    return this.layouts.size;
  }

  get retainedBytes(): number {
    return this.totalBytes;
  }

  get(
    query: string,
    columns: readonly StatisticsColumnDefinition[],
  ): StatisticsColumnLayout | undefined {
    const stored = this.layouts.get(query);
    if (stored === undefined) return undefined;
    this.layouts.delete(query);
    this.layouts.set(query, stored);
    return reconcileColumnLayout(stored.overrides.map((column) => ({
      id: column.id,
      maximumWidth: null,
      minimumWidth: null,
      visible: column.visible,
      width: column.width,
    })), columns);
  }

  set(
    query: string,
    layout: StatisticsColumnLayout,
    columns: readonly StatisticsColumnDefinition[],
  ): void {
    const defaultsById = new Map(createColumnLayout(columns).map((column) => [column.id, column]));
    const overrides = layout.flatMap((column): StoredColumnOverride[] => {
      const defaultColumn = defaultsById.get(column.id);
      if (
        defaultColumn === undefined
        || (column.visible === defaultColumn.visible && column.width === defaultColumn.width)
      ) return [];
      return [{ id: column.id, visible: column.visible, width: column.width }];
    });
    const previous = this.layouts.get(query);
    if (previous !== undefined) {
      this.layouts.delete(query);
      this.totalBytes -= previous.bytes;
    }
    if (overrides.length === 0 || this.maximumBytes === 0 || this.maximumEntries === 0) return;
    const bytes = STORED_LAYOUT_BASE_BYTES + (query.length * 2) + overrides.reduce(
      (total, column) => total + STORED_OVERRIDE_BASE_BYTES + (column.id.length * 2),
      0,
    );
    if (bytes > this.maximumBytes) return;
    this.layouts.set(query, { bytes, overrides });
    this.totalBytes += bytes;
    while (
      this.layouts.size > this.maximumEntries
      || this.totalBytes > this.maximumBytes
    ) {
      const oldestQuery = this.layouts.keys().next().value;
      if (oldestQuery === undefined) break;
      const oldest = this.layouts.get(oldestQuery);
      this.layouts.delete(oldestQuery);
      this.totalBytes -= oldest?.bytes ?? 0;
    }
  }
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
  for (const column of layout) {
    if (!column.visible) continue;
    if (column.width === null) return null;
    total += column.width;
  }
  return total;
}
