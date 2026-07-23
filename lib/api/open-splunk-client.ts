import {
  ProtobufTransport,
  type ProtobufRequestOptions,
  type ProtobufTransportOptions,
} from "./protobuf-transport";
import {
  exportRoutes,
  historyRoutes,
  indexRoutes,
  ingestionTokenRoutes,
  savedSearchRoutes,
  searchRoutes,
  systemRoutes,
  type RouteRequest,
} from "./routes";

/** Typed wrappers around every search-workspace SRouter endpoint. */
export class OpenSplunkApiClient {
  public readonly system = {
    bootstrap: (
      request: RouteRequest<typeof systemRoutes.bootstrap>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(systemRoutes.bootstrap, request, options),
  };

  public readonly indexes = {
    create: (
      request: RouteRequest<typeof indexRoutes.create>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.create, request, options),
    get: (
      request: RouteRequest<typeof indexRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.get, request, options),
    list: (
      request: RouteRequest<typeof indexRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.list, request, options),
    update: (
      request: RouteRequest<typeof indexRoutes.update>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.update, request, options),
    setState: (
      request: RouteRequest<typeof indexRoutes.setState>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.setState, request, options),
  };

  public readonly ingestionTokens = {
    create: (
      request: RouteRequest<typeof ingestionTokenRoutes.create>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(ingestionTokenRoutes.create, request, options),
    get: (
      request: RouteRequest<typeof ingestionTokenRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(ingestionTokenRoutes.get, request, options),
    list: (
      request: RouteRequest<typeof ingestionTokenRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(ingestionTokenRoutes.list, request, options),
    update: (
      request: RouteRequest<typeof ingestionTokenRoutes.update>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(ingestionTokenRoutes.update, request, options),
    revoke: (
      request: RouteRequest<typeof ingestionTokenRoutes.revoke>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(ingestionTokenRoutes.revoke, request, options),
  };

  public readonly search = {
    create: (
      request: RouteRequest<typeof searchRoutes.create>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.create, request, options),
    get: (
      request: RouteRequest<typeof searchRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.get, request, options),
    list: (
      request: RouteRequest<typeof searchRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.list, request, options),
    results: (
      request: RouteRequest<typeof searchRoutes.results>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.results, request, options),
    fields: (
      request: RouteRequest<typeof searchRoutes.fields>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.fields, request, options),
    fieldSummary: (
      request: RouteRequest<typeof searchRoutes.fieldSummary>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.fieldSummary, request, options),
    timeline: (
      request: RouteRequest<typeof searchRoutes.timeline>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.timeline, request, options),
    cancel: (
      request: RouteRequest<typeof searchRoutes.cancel>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.cancel, request, options),
  };

  public readonly savedSearches = {
    create: (
      request: RouteRequest<typeof savedSearchRoutes.create>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(savedSearchRoutes.create, request, options),
    get: (
      request: RouteRequest<typeof savedSearchRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(savedSearchRoutes.get, request, options),
    list: (
      request: RouteRequest<typeof savedSearchRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(savedSearchRoutes.list, request, options),
    update: (
      request: RouteRequest<typeof savedSearchRoutes.update>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(savedSearchRoutes.update, request, options),
    duplicate: (
      request: RouteRequest<typeof savedSearchRoutes.duplicate>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(savedSearchRoutes.duplicate, request, options),
    delete: (
      request: RouteRequest<typeof savedSearchRoutes.delete>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(savedSearchRoutes.delete, request, options),
  };

  public readonly history = {
    get: (
      request: RouteRequest<typeof historyRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(historyRoutes.get, request, options),
    list: (
      request: RouteRequest<typeof historyRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(historyRoutes.list, request, options),
    delete: (
      request: RouteRequest<typeof historyRoutes.delete>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(historyRoutes.delete, request, options),
    clear: (
      request: RouteRequest<typeof historyRoutes.clear>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(historyRoutes.clear, request, options),
  };

  public readonly exports = {
    create: (
      request: RouteRequest<typeof exportRoutes.create>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(exportRoutes.create, request, options),
    get: (
      request: RouteRequest<typeof exportRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(exportRoutes.get, request, options),
    cancel: (
      request: RouteRequest<typeof exportRoutes.cancel>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(exportRoutes.cancel, request, options),
  };

  public constructor(public readonly transport: ProtobufTransport = new ProtobufTransport()) {}
}

export function createOpenSplunkApiClient(options: ProtobufTransportOptions = {}): OpenSplunkApiClient {
  return new OpenSplunkApiClient(new ProtobufTransport(options));
}
