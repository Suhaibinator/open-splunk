import type { DemoEvent, DemoField, DemoScalar } from "@/lib/demo/search-data";

export interface EventsTableColumn {
  id: string;
  label: string;
}

export function eventsTableColumns(fields: readonly DemoField[]): EventsTableColumn[] {
  const seen = new Set(["_time"]);
  return [
    { id: "_time", label: "_time" },
    ...fields.flatMap((field) => {
      if (!field.selected || seen.has(field.name)) return [];
      seen.add(field.name);
      return [{ id: field.name, label: field.name }];
    }),
  ];
}

export function eventsTableValue(event: DemoEvent, columnId: string): DemoScalar {
  if (columnId === "_time") return event.fields["_time"] ?? event.time;
  return event.fields[columnId] ?? null;
}
