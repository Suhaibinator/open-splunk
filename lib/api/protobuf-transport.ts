import type { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";

export const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/** The small codec surface emitted by ts-proto for every generated message. */
export interface ProtobufCodec<T> {
  encode(message: T, writer?: BinaryWriter): BinaryWriter;
  decode(input: BinaryReader | Uint8Array, length?: number): T;
}

export interface ProtobufRoute<TRequest, TResponse> {
  readonly path: `/${string}`;
  readonly request: ProtobufCodec<TRequest>;
  readonly response: ProtobufCodec<TResponse>;
}

export interface ProtobufRequestOptions {
  /** Overrides the transport timeout for this request. */
  readonly timeoutMs?: number;
  /** Lets a caller cancel work independently of the request timeout. */
  readonly signal?: AbortSignal;
  readonly headers?: Readonly<Record<string, string>>;
}

export interface ProtobufTransportOptions {
  /**
   * Empty by default so the embedded static export calls its serving origin.
   * Test builds may supply another base. The production Go API itself enforces
   * same-origin browser traffic and advertises root-absolute WebSocket/export
   * paths, so cross-origin or arbitrary-prefix development needs a compatible
   * trusted test double.
   */
  readonly baseUrl?: string;
  readonly timeoutMs?: number;
  readonly fetch?: typeof globalThis.fetch;
  readonly headers?: Readonly<Record<string, string>>;
}

interface TransportErrorBody {
  readonly error?: string | { readonly message?: string };
  readonly message?: string;
}

/** HTTP failure that keeps structured transport metadata available to callers. */
export class HttpError extends Error {
  public readonly status: number;
  public readonly statusText: string;
  public readonly url: string;
  public readonly responseBody?: string;

  public constructor(options: {
    status: number;
    statusText?: string;
    message: string;
    url: string;
    responseBody?: string;
    cause?: unknown;
  }) {
    super(options.message, { cause: options.cause });
    this.name = "HttpError";
    this.status = options.status;
    this.statusText = options.statusText ?? "";
    this.url = options.url;
    this.responseBody = options.responseBody;
  }
}

/** A successful HTTP response that violates the protobuf transport contract. */
export class ProtobufProtocolError extends Error {
  public readonly url: string;
  public readonly contentType: string | null;

  public constructor(url: string, contentType: string | null) {
    const received = contentType ? `"${contentType}"` : "no Content-Type";
    super(`Expected ${PROTOBUF_CONTENT_TYPE} from ${url}, received ${received}`);
    this.name = "ProtobufProtocolError";
    this.url = url;
    this.contentType = contentType;
  }
}

export function isHttpError(error: unknown): error is HttpError {
  return error instanceof HttpError;
}

export function isHttpStatus(error: unknown, status: number): error is HttpError {
  return isHttpError(error) && error.status === status;
}

export function defineProtobufRoute<TRequest, TResponse>(
  path: `/${string}`,
  request: ProtobufCodec<TRequest>,
  response: ProtobufCodec<TResponse>,
): ProtobufRoute<TRequest, TResponse> {
  return { path, request, response };
}

function trimTrailingSlashes(value: string): string {
  return value.replace(/\/+$/, "");
}

function parseErrorMessage(status: number, statusText: string, body: string): string {
  if (body.length > 0) {
    try {
      const parsed = JSON.parse(body) as TransportErrorBody;
      if (typeof parsed.error === "string" && parsed.error.length > 0) {
        return parsed.error;
      }
      if (typeof parsed.error === "object" && typeof parsed.error?.message === "string") {
        return parsed.error.message;
      }
      if (typeof parsed.message === "string" && parsed.message.length > 0) {
        return parsed.message;
      }
    } catch {
      // SRouter may return a plain-text error. Preserve it verbatim below.
    }
    if (/^\s*<!doctype html|^\s*<html/i.test(body)) {
      return `HTTP ${status}${statusText.length > 0 ? `: ${statusText}` : ""}`;
    }
    return body;
  }

  return `HTTP ${status}${statusText.length > 0 ? `: ${statusText}` : ""}`;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError");
}

/**
 * Same-origin protobuf-over-HTTP transport for the statically exported client.
 * It intentionally has no dependency on Next.js server APIs.
 */
export class ProtobufTransport {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImplementation: typeof globalThis.fetch;
  private readonly defaultHeaders: Readonly<Record<string, string>>;

  public constructor(options: ProtobufTransportOptions = {}) {
    this.baseUrl = trimTrailingSlashes(options.baseUrl ?? "");
    this.timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    const fetchImplementation = options.fetch ?? globalThis.fetch;
    this.fetchImplementation = options.fetch ?? fetchImplementation?.bind(globalThis);
    this.defaultHeaders = options.headers ?? {};

    if (!Number.isFinite(this.timeoutMs) || this.timeoutMs <= 0) {
      throw new RangeError("Protobuf transport timeout must be a positive number of milliseconds");
    }
    if (typeof this.fetchImplementation !== "function") {
      throw new Error("Fetch is unavailable in this runtime; provide a fetch implementation");
    }
  }

  public async post<TRequest, TResponse>(
    route: ProtobufRoute<TRequest, TResponse>,
    request: TRequest,
    options: ProtobufRequestOptions = {},
  ): Promise<TResponse> {
    const timeoutMs = options.timeoutMs ?? this.timeoutMs;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new RangeError("Request timeout must be a positive number of milliseconds");
    }

    const url = `${this.baseUrl}${route.path}`;
    const controller = new AbortController();
    let didTimeout = false;
    const timeout = globalThis.setTimeout(() => {
      didTimeout = true;
      controller.abort();
    }, timeoutMs);
    const abortFromCaller = () => controller.abort(options.signal?.reason);
    options.signal?.addEventListener("abort", abortFromCaller, { once: true });

    try {
      if (options.signal?.aborted) {
        controller.abort(options.signal.reason);
      }

      const requestBytes = route.request.encode(request).finish();
      const response = await this.fetchImplementation(url, {
        method: "POST",
        headers: {
          ...this.defaultHeaders,
          ...options.headers,
          Accept: PROTOBUF_CONTENT_TYPE,
          "Content-Type": PROTOBUF_CONTENT_TYPE,
        },
        body: requestBytes,
        cache: "no-store",
        credentials: "include",
        signal: controller.signal,
      });

      if (!response.ok) {
        let responseBody = "";
        try {
          responseBody = await response.text();
        } catch (error) {
          if (controller.signal.aborted) {
            throw error;
          }
          // Keep the status-derived fallback when the body is unreadable.
        }
        if (didTimeout) {
          throw new HttpError({
            status: 504,
            statusText: "Gateway Timeout",
            message: `Request to ${route.path} timed out after ${timeoutMs}ms`,
            url,
          });
        }

        throw new HttpError({
          status: response.status,
          statusText: response.statusText,
          message: parseErrorMessage(response.status, response.statusText, responseBody),
          url,
          responseBody: responseBody || undefined,
        });
      }

      const contentType = response.headers.get("Content-Type");
      const mediaType = contentType?.split(";", 1)[0]?.trim().toLowerCase();
      if (mediaType !== PROTOBUF_CONTENT_TYPE) {
        throw new ProtobufProtocolError(url, contentType);
      }

      const responseBytes = new Uint8Array(await response.arrayBuffer());
      return route.response.decode(responseBytes);
    } catch (error) {
      if (didTimeout && isAbortError(error)) {
        throw new HttpError({
          status: 504,
          statusText: "Gateway Timeout",
          message: `Request to ${route.path} timed out after ${timeoutMs}ms`,
          url,
          cause: error,
        });
      }
      throw error;
    } finally {
      globalThis.clearTimeout(timeout);
      options.signal?.removeEventListener("abort", abortFromCaller);
    }
  }
}
