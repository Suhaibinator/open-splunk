import {
  ProtobufTransport,
  type ProtobufRequestOptions,
  type ProtobufRoute,
  type ProtobufTransportOptions,
} from "./protobuf-transport";
import {
  appRoutes,
  auditEventRoutes,
  collectorRoutes,
  dashboardRoutes,
  exportRoutes,
  hecOperationsRoutes,
  historyRoutes,
  indexRoutes,
  ingestionTokenRoutes,
  savedSearchRoutes,
  searchAttemptAuditRoutes,
  searchRoutes,
  systemRoutes,
} from "./routes";

/** Typed wrappers around every search-workspace SRouter endpoint. */
export class OpenSplunkApiClient {
  public readonly system = {
    bootstrap: this.route(systemRoutes.bootstrap),
  };

  public readonly apps = {
    create: this.route(appRoutes.create),
    get: this.route(appRoutes.get),
    list: this.route(appRoutes.list),
    update: this.route(appRoutes.update),
    setState: this.route(appRoutes.setState),
    delete: this.route(appRoutes.delete),
  };

  public readonly collectors = {
    list: this.route(collectorRoutes.list),
    get: this.route(collectorRoutes.get),
    update: this.route(collectorRoutes.update),
    setState: this.route(collectorRoutes.setState),
  };

  public readonly auditEvents = {
    list: this.route(auditEventRoutes.list),
  };

  public readonly searchAttemptAudit = {
    list: this.route(searchAttemptAuditRoutes.list),
  };

  public readonly indexes = {
    create: this.route(indexRoutes.create),
    get: this.route(indexRoutes.get),
    list: this.route(indexRoutes.list),
    fields: this.route(indexRoutes.fields),
    update: this.route(indexRoutes.update),
    setState: this.route(indexRoutes.setState),
    delete: this.route(indexRoutes.delete),
    stats: this.route(indexRoutes.stats),
  };

  public readonly ingestionTokens = {
    create: this.route(ingestionTokenRoutes.create),
    get: this.route(ingestionTokenRoutes.get),
    list: this.route(ingestionTokenRoutes.list),
    update: this.route(ingestionTokenRoutes.update),
    setState: this.route(ingestionTokenRoutes.setState),
    revoke: this.route(ingestionTokenRoutes.revoke),
  };

  public readonly hec = {
    getOperationalSnapshot: this.route(hecOperationsRoutes.get),
  };

  public readonly search = {
    validate: this.route(searchRoutes.validate),
    suggestions: this.route(searchRoutes.suggestions),
    create: this.route(searchRoutes.create),
    get: this.route(searchRoutes.get),
    list: this.route(searchRoutes.list),
    results: this.route(searchRoutes.results),
    fields: this.route(searchRoutes.fields),
    fieldSummary: this.route(searchRoutes.fieldSummary),
    timeline: this.route(searchRoutes.timeline),
    cancel: this.route(searchRoutes.cancel),
    inspect: this.route(searchRoutes.inspect),
  };

  public readonly savedSearches = {
    create: this.route(savedSearchRoutes.create),
    get: this.route(savedSearchRoutes.get),
    list: this.route(savedSearchRoutes.list),
    update: this.route(savedSearchRoutes.update),
    duplicate: this.route(savedSearchRoutes.duplicate),
    delete: this.route(savedSearchRoutes.delete),
  };

  public readonly dashboards = {
    create: this.route(dashboardRoutes.create),
    get: this.route(dashboardRoutes.get),
    list: this.route(dashboardRoutes.list),
    update: this.route(dashboardRoutes.update),
    delete: this.route(dashboardRoutes.delete),
    runPanel: this.route(dashboardRoutes.runPanel),
  };

  public readonly history = {
    get: this.route(historyRoutes.get),
    list: this.route(historyRoutes.list),
    delete: this.route(historyRoutes.delete),
    clear: this.route(historyRoutes.clear),
  };

  public readonly exports = {
    create: this.route(exportRoutes.create),
    get: this.route(exportRoutes.get),
    list: this.route(exportRoutes.list),
    cancel: this.route(exportRoutes.cancel),
  };

  public constructor(public readonly transport: ProtobufTransport = new ProtobufTransport()) {}

  /** Binds one route to the transport, preserving its request/response types. */
  private route<TRequest, TResponse>(
    route: ProtobufRoute<TRequest, TResponse>,
  ): (request: TRequest, options?: ProtobufRequestOptions) => Promise<TResponse> {
    return (request, options) => this.transport.post(route, request, options);
  }
}

export function createOpenSplunkApiClient(options: ProtobufTransportOptions = {}): OpenSplunkApiClient {
  return new OpenSplunkApiClient(new ProtobufTransport(options));
}
