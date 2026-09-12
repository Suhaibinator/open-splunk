import type { ProtobufRequestOptions } from "./protobuf-transport";

export interface BrowserCreateRequestOptions extends ProtobufRequestOptions {
  clientRequestId?: string;
}

export function browserClientRequestId(): string {
  return crypto.randomUUID();
}

// This key belongs to one form/action, never a global cache of similar requests.
// Keep the action alive after an ambiguous failure; complete it after acceptance.
export class BrowserCreateAction {
  private intent: string | undefined;
  private id: string | undefined;

  requestId(intent: unknown): string {
    const canonical = JSON.stringify(encodeIntent(intent));
    if (this.id === undefined || this.intent !== canonical) {
      this.intent = canonical;
      this.id = browserClientRequestId();
    }
    return this.id;
  }

  complete(expectedId?: string): void {
    if (expectedId !== undefined && this.id !== expectedId) return;
    this.intent = undefined;
    this.id = undefined;
  }
}

function encodeIntent(value: unknown): unknown {
  if (value === null) return ["null"];
  if (value instanceof Date) return ["date", value.toISOString()];
  if (value instanceof Uint8Array) return ["bytes", Array.from(value)];
  if (Array.isArray(value)) return ["array", value.map(encodeIntent)];
  if (typeof value === "object") {
    return ["object", Object.entries(value).filter(([, entry]) => entry !== undefined)
      .toSorted(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
      .map(([key, entry]) => [key, encodeIntent(entry)])];
  }
  return [typeof value, typeof value === "bigint" ? value.toString() : value];
}
