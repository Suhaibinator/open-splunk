package server

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
)

type handlerIndexServices struct {
	catalog            IndexCatalog
	administration     IndexAdministration
	statistics         IndexStatistics
	statisticsSnapshot IndexStatisticsSnapshotter
	fields             IndexFields
	deletionAdmission  IndexDataDeletionAdmission
	deletionWaker      IndexDataDeletionWaker
}

func normalizeHandlerIndexServices(config Config) (handlerIndexServices, error) {
	if isNilDependency(config.Indexes) {
		return handlerIndexServices{}, errors.New("create server handler: index catalog is required")
	}
	result := handlerIndexServices{catalog: config.Indexes, administration: config.IndexAdmin}
	if isNilDependency(result.administration) {
		if inferred, ok := config.Indexes.(IndexAdministration); ok && !isNilDependency(inferred) {
			result.administration = inferred
		} else {
			result.administration = nil
		}
	}
	result.statistics = config.IndexStatistics
	if isNilDependency(result.statistics) {
		result.statistics = nil
	}
	result.statisticsSnapshot = config.IndexStatisticsSnapshotter
	if isNilDependency(result.statisticsSnapshot) {
		result.statisticsSnapshot = nil
	}
	if (result.statistics == nil) != (result.statisticsSnapshot == nil) {
		return handlerIndexServices{}, errors.New("create server handler: index statistics and snapshotter must be configured together")
	}
	if result.statistics != nil && result.administration == nil {
		return handlerIndexServices{}, errors.New("create server handler: index statistics requires index administration")
	}
	result.fields = config.IndexFields
	if isNilDependency(result.fields) {
		result.fields = nil
	}
	if result.fields != nil && result.administration == nil {
		return handlerIndexServices{}, errors.New("create server handler: index fields require index administration")
	}
	result.deletionAdmission = config.IndexDataDeletionAdmission
	if isNilDependency(result.deletionAdmission) {
		result.deletionAdmission = nil
	}
	result.deletionWaker = config.IndexDataDeletionWaker
	if isNilDependency(result.deletionWaker) {
		result.deletionWaker = nil
	}
	if (result.deletionAdmission == nil) != (result.deletionWaker == nil) {
		return handlerIndexServices{}, errors.New("create server handler: index data deletion admission and waker must be configured together")
	}
	if result.deletionAdmission != nil && result.administration == nil {
		return handlerIndexServices{}, errors.New("create server handler: index data deletion requires index administration")
	}
	return result, nil
}

func (services handlerIndexServices) limits() (uint32, uint32, error) {
	if services.fields == nil {
		return 0, 0, nil
	}
	maximumFields := services.fields.MaximumFields()
	maximumPageSize := services.fields.MaximumPageSize()
	if maximumFields == 0 || maximumFields > clickhouse.MaximumFieldCatalogFields {
		return 0, 0, fmt.Errorf("create server handler: index field catalog maximum fields must be between 1 and %d", clickhouse.MaximumFieldCatalogFields)
	}
	if maximumPageSize == 0 || maximumPageSize > maximumFields || maximumPageSize > maximumSearchFieldPageSize {
		return 0, 0, fmt.Errorf("create server handler: index field catalog maximum page size must be between 1 and %d and cannot exceed maximum fields", maximumSearchFieldPageSize)
	}
	return maximumFields, maximumPageSize, nil
}

type handlerKnowledgeServices struct {
	appCatalog      AppCatalog
	catalog         KnowledgeCatalog
	writer          KnowledgeWriter
	apps            KnowledgeAppCatalog
	attempts        KnowledgeAttemptJournal
	preview         *knowledgepreview.Service
	admission       bool
	configuredCount int
}

func normalizeHandlerKnowledgeServices(config Config) (handlerKnowledgeServices, error) {
	result := handlerKnowledgeServices{admission: knowledgeSearchAdmissionEnabled(config.SearchJobs), appCatalog: config.AppCatalog}
	if isNilDependency(result.appCatalog) {
		result.appCatalog = nil
	}
	if result.admission && result.appCatalog == nil {
		return handlerKnowledgeServices{}, errors.New("create server handler: knowledge-aware search admission requires a live app catalog")
	}
	if result.appCatalog != nil && len(config.Bootstrap.Apps) != 0 {
		return handlerKnowledgeServices{}, errors.New("create server handler: live app catalog and static bootstrap apps cannot both be configured")
	}
	result.catalog = config.KnowledgeCatalog
	if isNilDependency(result.catalog) {
		result.catalog = nil
	}
	result.writer = config.KnowledgeWriter
	if isNilDependency(result.writer) {
		result.writer = nil
	}
	result.apps = config.KnowledgeApps
	if isNilDependency(result.apps) {
		result.apps = nil
	}
	result.attempts = config.KnowledgeAttempts
	if isNilDependency(result.attempts) {
		result.attempts = nil
	}
	for _, configured := range []bool{result.catalog != nil, result.writer != nil, result.apps != nil, result.attempts != nil} {
		if configured {
			result.configuredCount++
		}
	}
	if result.configuredCount != 0 && result.configuredCount != 4 {
		return handlerKnowledgeServices{}, errors.New("create server handler: knowledge management dependencies must be configured together")
	}
	if result.configuredCount == 4 && !replaysUnavailableActiveMutations(result.writer) {
		return handlerKnowledgeServices{}, errors.New("create server handler: knowledge management requires the concrete catalog writer")
	}
	result.preview = config.KnowledgePreview
	if result.preview != nil && (result.configuredCount != 4 || !result.preview.Ready()) {
		return handlerKnowledgeServices{}, errors.New("create server handler: knowledge preview requires the complete ready knowledge management family")
	}
	return result, nil
}

func (services handlerKnowledgeServices) complete(inspection SearchInspections, history SearchHistory, exports Exports, timelines SearchTimelines, fields SearchFields, suggestions SearchSuggestions) bool {
	return services.configuredCount == 4 && services.preview != nil && services.preview.Ready() && services.admission &&
		inspection != nil && history != nil && exports != nil && timelines != nil && fields != nil && suggestions != nil
}

type handlerLookupServices struct {
	management LookupManagement
	admission  bool
}

func normalizeHandlerLookupServices(config Config) (handlerLookupServices, error) {
	result := handlerLookupServices{management: config.LookupManagement, admission: lookupSearchAdmissionEnabled(config.SearchJobs)}
	if isNilDependency(result.management) {
		result.management = nil
	} else if !result.management.Ready() {
		return handlerLookupServices{}, errors.New("create server handler: lookup management service is not ready")
	}
	return result, nil
}

type handlerAlertServices struct {
	coordinator AlertCoordinator
	complete    bool
}

func normalizeHandlerAlertServices(config Config) handlerAlertServices {
	coordinator := config.AlertCoordinator
	if isNilDependency(coordinator) {
		coordinator = nil
	}
	return handlerAlertServices{
		coordinator: coordinator,
		complete: config.AlertService != nil && !isNilDependency(config.AlertRepository) &&
			!isNilDependency(config.AlertDeliverer) && coordinator != nil,
	}
}

type handlerAdministrativeServices struct {
	ingestionTokens          IngestionTokenAdministration
	hecOperations            HECOperationalSnapshotter
	auditEvents              AuditEvents
	searchAttemptAuditEvents SearchAttemptAuditEvents
	serverSettings           Settings
	collectorAdmin           CollectorAdministration
	appAdmin                 AppAdministration
	browserAuthenticator     auth.BrowserAuthenticator
	appCursorKey             []byte
}

func normalizeHandlerAdministrativeServices(
	config Config,
	indexes handlerIndexServices,
	knowledge handlerKnowledgeServices,
	lookups handlerLookupServices,
	alerts handlerAlertServices,
	inspection SearchInspections,
) (handlerAdministrativeServices, error) {
	result := handlerAdministrativeServices{
		ingestionTokens: config.IngestionTokens, hecOperations: config.HECOperations,
		auditEvents: config.AuditEvents, searchAttemptAuditEvents: config.SearchAttemptAuditEvents,
		serverSettings: config.ServerSettings, collectorAdmin: config.CollectorAdmin,
		appAdmin: config.AppAdmin, browserAuthenticator: config.BrowserAuthenticator,
	}
	if isNilDependency(result.ingestionTokens) {
		result.ingestionTokens = nil
	}
	if isNilDependency(result.hecOperations) {
		result.hecOperations = nil
	}
	if isNilDependency(result.auditEvents) {
		result.auditEvents = nil
	}
	if isNilDependency(result.searchAttemptAuditEvents) {
		result.searchAttemptAuditEvents = nil
	}
	if isNilDependency(result.serverSettings) {
		result.serverSettings = nil
	}
	if isNilDependency(result.collectorAdmin) {
		result.collectorAdmin = nil
	}
	if isNilDependency(result.appAdmin) {
		result.appAdmin = nil
	}
	if isNilDependency(result.browserAuthenticator) {
		result.browserAuthenticator = nil
	}
	if (indexes.administration != nil || indexes.statistics != nil || indexes.fields != nil ||
		result.ingestionTokens != nil || result.hecOperations != nil || result.auditEvents != nil ||
		result.searchAttemptAuditEvents != nil || result.serverSettings != nil || result.collectorAdmin != nil ||
		result.appAdmin != nil || knowledge.catalog != nil || lookups.management != nil || inspection != nil || alerts.complete) &&
		result.browserAuthenticator == nil {
		return handlerAdministrativeServices{}, errors.New("create server handler: administrative services require browser authentication")
	}
	if result.appAdmin != nil {
		if len(config.AppCursorKey) < 32 || len(config.AppCursorKey) > 1<<10 {
			return handlerAdministrativeServices{}, errors.New("create server handler: app cursor key must contain between 32 and 1024 bytes")
		}
		result.appCursorKey = slices.Clone(config.AppCursorKey)
	}
	return result, nil
}
