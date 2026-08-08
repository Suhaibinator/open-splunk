import type { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";

import {
  getAdministratorBearerToken,
  isAdministratorRoutePath,
} from "./administrator-session";

export const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
export const MAXIMUM_ERROR_RESPONSE_BYTES = 64 << 10;

/** The small codec surface emitted by ts-proto for every generated message. */
export interface ProtobufCodec<T> {
  encode(message: T, writer?: BinaryWriter): BinaryWriter;
  decode(input: BinaryReader | Uint8Array, length?: number): T;
}

export interface ProtobufRoute<TRequest, TResponse> {
  readonly path: `/${string}`;
  readonly request: ProtobufCodec<TRequest>;
  readonly response: ProtobufCodec<TResponse>;
  /** Administrator routes receive the current in-memory bearer credential. */
  readonly authorization: "none" | "administrator";
  /** Optional wire-size ceiling enforced before this route's decoder runs. */
  readonly maximumResponseBytes?: number;
}

export interface ProtobufRouteOptions {
  readonly maximumResponseBytes?: number;
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

/** A response whose wire body exceeds a route's pre-decode browser limit. */
export class ProtobufResponseTooLargeError extends Error {
  public readonly url: string;
  public readonly maximumBytes: number;
  public readonly declaredBytes: number | null;

  public constructor(
    url: string,
    maximumBytes: number,
    declaredBytes: number | null = null,
  ) {
    super(`Response from ${url} exceeds the ${maximumBytes}-byte transport limit`);
    this.name = "ProtobufResponseTooLargeError";
    this.url = url;
    this.maximumBytes = maximumBytes;
    this.declaredBytes = declaredBytes;
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
  options: ProtobufRouteOptions = {},
): ProtobufRoute<TRequest, TResponse> {
  if (
    options.maximumResponseBytes !== undefined
    && (!Number.isSafeInteger(options.maximumResponseBytes)
      || options.maximumResponseBytes < 0)
  ) {
    throw new RangeError("Protobuf response limit must be a non-negative safe integer");
  }
  return {
    path,
    request,
    response,
    authorization: isAdministratorRoutePath(path) ? "administrator" : "none",
    ...(options.maximumResponseBytes === undefined
      ? {}
      : { maximumResponseBytes: options.maximumResponseBytes }),
  };
}

function trimTrailingSlashes(value: string): string {
  return value.replace(/\/+$/, "");
}

function withoutAuthorization(
  headers: Readonly<Record<string, string>>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(headers).filter(([name]) => name.toLowerCase() !== "authorization"),
  );
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

function declaredContentLength(response: Response): number | null {
  const raw = response.headers.get("Content-Length")?.trim();
  if (raw === undefined || raw.length === 0 || !/^(?:0|[1-9][0-9]*)$/.test(raw)) {
    return null;
  }
  const value = Number(raw);
  return Number.isSafeInteger(value) ? value : Number.POSITIVE_INFINITY;
}

function cancelReader(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  reason: unknown,
): void {
  try {
    void reader.cancel(reason).catch(() => {
      // The size violation remains the useful failure when cancellation races.
    });
  } catch {
    // The size violation remains the useful failure when cancellation races.
  }
}

function cancelStream(stream: ReadableStream<Uint8Array> | null, reason: unknown): void {
  if (stream === null) return;
  try {
    void stream.cancel(reason).catch(() => {
      // The size violation remains the useful failure when cancellation races.
    });
  } catch {
    // The size violation remains the useful failure when cancellation races.
  }
}

function grownBufferCapacity(
  currentCapacity: number,
  requiredCapacity: number,
  maximumCapacity: number,
): number {
  if (requiredCapacity > maximumCapacity) return maximumCapacity;
  let capacity = Math.max(1, currentCapacity);
  while (capacity < requiredCapacity) {
    capacity = Math.min(maximumCapacity, Math.max(requiredCapacity, capacity * 2));
  }
  return capacity;
}

async function readBoundedResponseBytes(
  response: Response,
  maximumBytes: number,
  url: string,
): Promise<Uint8Array> {
  const declaredBytes = declaredContentLength(response);
  if (declaredBytes !== null && declaredBytes > maximumBytes) {
    const error = new ProtobufResponseTooLargeError(url, maximumBytes, declaredBytes);
    cancelStream(response.body, error);
    throw error;
  }

  const stream = response.body;
  if (stream === null) return new Uint8Array();
  const reader = stream.getReader();
  const initialCapacity = Math.min(maximumBytes, declaredBytes ?? (64 << 10));
  let bytes = new Uint8Array(initialCapacity);
  let length = 0;
  try {
    while (true) {
      // Stream chunks must be consumed in wire order and cannot be read concurrently.
      // eslint-disable-next-line no-await-in-loop
      const result = await reader.read();
      if (result.done) break;
      const chunk = result.value;
      if (chunk.byteLength > maximumBytes - length) {
        const error = new ProtobufResponseTooLargeError(url, maximumBytes, declaredBytes);
        // Cancellation is best-effort: an adversarial stream must not delay the
        // size failure by returning a promise that never settles.
        cancelReader(reader, error);
        throw error;
      }
      if (chunk.byteLength > 0) {
        const requiredCapacity = length + chunk.byteLength;
        if (requiredCapacity > bytes.byteLength) {
          const grown = new Uint8Array(grownBufferCapacity(
            bytes.byteLength,
            requiredCapacity,
            maximumBytes,
          ));
          grown.set(bytes.subarray(0, length));
          bytes = grown;
        }
        bytes.set(chunk, length);
        length += chunk.byteLength;
      }
    }
  } finally {
    reader.releaseLock();
  }

  return length === bytes.byteLength ? bytes : bytes.slice(0, length);
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
      const administratorToken = route.authorization === "administrator"
        ? getAdministratorBearerToken()
        : null;
      const defaultHeaders = administratorToken === null
        ? this.defaultHeaders
        : withoutAuthorization(this.defaultHeaders);
      const requestHeaders = administratorToken === null
        ? (options.headers ?? {})
        : withoutAuthorization(options.headers ?? {});
      const response = await this.fetchImplementation(url, {
        method: "POST",
        headers: {
          ...defaultHeaders,
          ...requestHeaders,
          ...(administratorToken === null
            ? {}
            : { Authorization: `Bearer ${administratorToken}` }),
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
          const responseBytes = await readBoundedResponseBytes(
            response,
            MAXIMUM_ERROR_RESPONSE_BYTES,
            url,
          );
          responseBody = new TextDecoder().decode(responseBytes);
        } catch (error) {
          if (controller.signal.aborted) {
            throw error;
          }
          // Keep the status-derived fallback when the body is unreadable or
          // larger than the fixed global error envelope.
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

      const responseBytes = route.maximumResponseBytes === undefined
        ? new Uint8Array(await response.arrayBuffer())
        : await readBoundedResponseBytes(response, route.maximumResponseBytes, url);
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
