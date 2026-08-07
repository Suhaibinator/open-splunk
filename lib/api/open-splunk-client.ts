import {
  ProtobufTransport,
  type ProtobufRequestOptions,
  type ProtobufTransportOptions,
} from "./protobuf-transport";
import {
  appRoutes,
  auditEventRoutes,
  collectorRoutes,
  exportRoutes,
  historyRoutes,
  indexRoutes,
  ingestionTokenRoutes,
  savedSearchRoutes,
  searchAttemptAuditRoutes,
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

  public readonly apps = {
    create: (
      request: RouteRequest<typeof appRoutes.create>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(appRoutes.create, request, options),
    get: (
      request: RouteRequest<typeof appRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(appRoutes.get, request, options),
    list: (
      request: RouteRequest<typeof appRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(appRoutes.list, request, options),
    update: (
      request: RouteRequest<typeof appRoutes.update>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(appRoutes.update, request, options),
    setState: (
      request: RouteRequest<typeof appRoutes.setState>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(appRoutes.setState, request, options),
    delete: (
      request: RouteRequest<typeof appRoutes.delete>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(appRoutes.delete, request, options),
  };

  public readonly collectors = {
    list: (
      request: RouteRequest<typeof collectorRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(collectorRoutes.list, request, options),
    get: (
      request: RouteRequest<typeof collectorRoutes.get>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(collectorRoutes.get, request, options),
    update: (
      request: RouteRequest<typeof collectorRoutes.update>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(collectorRoutes.update, request, options),
    setState: (
      request: RouteRequest<typeof collectorRoutes.setState>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(collectorRoutes.setState, request, options),
  };

  public readonly auditEvents = {
    list: (
      request: RouteRequest<typeof auditEventRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(auditEventRoutes.list, request, options),
  };

  public readonly searchAttemptAudit = {
    list: (
      request: RouteRequest<typeof searchAttemptAuditRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchAttemptAuditRoutes.list, request, options),
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
    fields: (
      request: RouteRequest<typeof indexRoutes.fields>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.fields, request, options),
    update: (
      request: RouteRequest<typeof indexRoutes.update>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.update, request, options),
    setState: (
      request: RouteRequest<typeof indexRoutes.setState>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.setState, request, options),
    delete: (
      request: RouteRequest<typeof indexRoutes.delete>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.delete, request, options),
    stats: (
      request: RouteRequest<typeof indexRoutes.stats>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(indexRoutes.stats, request, options),
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
    validate: (
      request: RouteRequest<typeof searchRoutes.validate>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.validate, request, options),
    suggestions: (
      request: RouteRequest<typeof searchRoutes.suggestions>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.suggestions, request, options),
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
    inspect: (
      request: RouteRequest<typeof searchRoutes.inspect>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(searchRoutes.inspect, request, options),
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
    list: (
      request: RouteRequest<typeof exportRoutes.list>,
      options?: ProtobufRequestOptions,
    ) => this.transport.post(exportRoutes.list, request, options),
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
