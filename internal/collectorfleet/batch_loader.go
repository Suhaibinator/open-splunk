package collectorfleet

import (
	"errors"
	"fmt"
	"math"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

const maximumCollectorBatchSize = 4

var errCollectorBatchMissingIdentity = errors.New(
	"collector batch is missing a requested identity in the control-plane database",
)

type collectorRecordSet struct {
	fleet        fleetRecord
	runtime      runtimeRecord
	capabilities []capabilityRecord
	indexes      []authorizedIndexRecord
	inputs       []inputRecord
	health       []inputHealthRecord
}

// loadCollectors hydrates a bounded, ordered set of collector identities in a
// constant six queries: fleet, runtime, capabilities, authorized indexes,
// input registrations, and input health. Callers that need a coherent catalog
// page must call it through the same read transaction that selected the IDs.
//
// Missing selected parents are corruption or a caller transaction error. The
// one-record loadCollector wrapper is the only path that translates that
// condition to the public not-found result expected by Store.Get.
func loadCollectors(
	database *gorm.DB,
	scope Scope,
	collectorIDs []string,
) ([]Collector, error) {
	if database == nil || database.Statement == nil {
		return nil, invalid("collector database is required")
	}
	normalizedScope, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	if normalizedScope != scope {
		return nil, invalid("collector tenant scope is not normalized")
	}
	if len(collectorIDs) > maximumCollectorBatchSize {
		return nil, invalid(
			"collector batch cannot contain more than %d identities",
			maximumCollectorBatchSize,
		)
	}
	if len(collectorIDs) == 0 {
		return []Collector{}, nil
	}

	requested := make(map[string]struct{}, len(collectorIDs))
	for _, collectorID := range collectorIDs {
		if !validIdentifier(collectorID) {
			return nil, invalid("collector ID is not a canonical identifier")
		}
		if _, duplicate := requested[collectorID]; duplicate {
			return nil, invalid("collector batch contains a duplicate identity")
		}
		requested[collectorID] = struct{}{}
	}

	records := make(map[string]*collectorRecordSet, len(collectorIDs))
	var fleets []fleetRecord
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order("collector_id").
		Limit(len(collectorIDs) + 1).
		Find(&fleets).Error; err != nil {
		return nil, mapContextError(
			database.Statement.Context,
			"get collector batch identities",
			err,
		)
	}
	if len(fleets) > len(collectorIDs) {
		return nil, errors.New(
			"collector batch returned too many identities from the control-plane database",
		)
	}
	for index := range fleets {
		row := fleets[index]
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			row.TenantID,
			row.CollectorID,
			"identity",
		); err != nil {
			return nil, err
		}
		if _, duplicate := records[row.CollectorID]; duplicate {
			return nil, errors.New(
				"collector batch returned a duplicate identity from the control-plane database",
			)
		}
		records[row.CollectorID] = &collectorRecordSet{fleet: row}
	}
	if len(records) != len(collectorIDs) {
		return nil, errCollectorBatchMissingIdentity
	}

	var runtimes []runtimeRecord
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order("collector_id").
		Limit(len(collectorIDs) + 1).
		Find(&runtimes).Error; err != nil {
		return nil, mapContextError(
			database.Statement.Context,
			"get collector batch runtimes",
			err,
		)
	}
	if len(runtimes) > len(collectorIDs) {
		return nil, errors.New(
			"collector batch returned too many runtime rows from the control-plane database",
		)
	}
	runtimeSeen := make(map[string]struct{}, len(runtimes))
	for _, row := range runtimes {
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			row.TenantID,
			row.CollectorID,
			"runtime",
		); err != nil {
			return nil, err
		}
		if _, duplicate := runtimeSeen[row.CollectorID]; duplicate {
			return nil, errors.New(
				"collector batch returned a duplicate runtime row from the control-plane database",
			)
		}
		runtimeSeen[row.CollectorID] = struct{}{}
		records[row.CollectorID].runtime = row
	}
	if len(runtimeSeen) != len(collectorIDs) {
		return nil, errors.New(
			"collector batch is missing a runtime row in the control-plane database",
		)
	}

	if err := loadCollectorChildren(
		database,
		scope,
		collectorIDs,
		requested,
		records,
		collectorChildQuery[capabilityRecord]{
			order:            "collector_id, capability",
			perParentMaximum: maximumCapabilities,
			queryLabel:       "get collector batch capabilities",
			rowKind:          "capability",
			aggregateDetail:  "capability rows exceed aggregate persisted bounds",
			perParentDetail:  "capabilities exceed per-collector persisted bounds",
			slot: func(set *collectorRecordSet) *[]capabilityRecord {
				return &set.capabilities
			},
			tenantOf:    func(row capabilityRecord) string { return row.TenantID },
			collectorOf: func(row capabilityRecord) string { return row.CollectorID },
		},
	); err != nil {
		return nil, err
	}

	if err := loadCollectorChildren(
		database,
		scope,
		collectorIDs,
		requested,
		records,
		collectorChildQuery[authorizedIndexRecord]{
			order:            "collector_id, index_name",
			perParentMaximum: maximumAuthorizedIndexes,
			queryLabel:       "get collector batch authorized indexes",
			rowKind:          "authorized index",
			aggregateDetail:  "authorized-index rows exceed aggregate persisted bounds",
			perParentDetail:  "authorized indexes exceed per-collector persisted bounds",
			slot: func(set *collectorRecordSet) *[]authorizedIndexRecord {
				return &set.indexes
			},
			tenantOf: func(row authorizedIndexRecord) string { return row.TenantID },
			collectorOf: func(row authorizedIndexRecord) string {
				return row.CollectorID
			},
		},
	); err != nil {
		return nil, err
	}

	if err := loadCollectorChildren(
		database,
		scope,
		collectorIDs,
		requested,
		records,
		collectorChildQuery[inputRecord]{
			order:            "collector_id, input_id",
			perParentMaximum: maximumInputs,
			queryLabel:       "get collector batch inputs",
			rowKind:          "input",
			aggregateDetail:  "input rows exceed aggregate persisted bounds",
			perParentDetail:  "inputs exceed per-collector persisted bounds",
			slot: func(set *collectorRecordSet) *[]inputRecord {
				return &set.inputs
			},
			tenantOf:    func(row inputRecord) string { return row.TenantID },
			collectorOf: func(row inputRecord) string { return row.CollectorID },
		},
	); err != nil {
		return nil, err
	}

	if err := loadCollectorChildren(
		database,
		scope,
		collectorIDs,
		requested,
		records,
		collectorChildQuery[inputHealthRecord]{
			order:            "collector_id, input_id",
			perParentMaximum: maximumInputs,
			queryLabel:       "get collector batch input health",
			rowKind:          "input health",
			aggregateDetail:  "input-health rows exceed aggregate persisted bounds",
			perParentDetail:  "input health exceeds per-collector persisted bounds",
			slot: func(set *collectorRecordSet) *[]inputHealthRecord {
				return &set.health
			},
			tenantOf:    func(row inputHealthRecord) string { return row.TenantID },
			collectorOf: func(row inputHealthRecord) string { return row.CollectorID },
		},
	); err != nil {
		return nil, err
	}

	result := make([]Collector, 0, len(collectorIDs))
	for _, collectorID := range collectorIDs {
		parent := records[collectorID]
		collector, err := collectorFromRecords(
			parent.fleet,
			parent.runtime,
			parent.capabilities,
			parent.indexes,
			parent.inputs,
			parent.health,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"hydrate collector %q from control-plane database: %w",
				collectorID,
				err,
			)
		}
		result = append(result, collector)
	}
	return result, nil
}

func collectorChildBoundsError(detail string) error {
	return fmt.Errorf(
		"collector child snapshot exceeds persisted bounds: %s",
		detail,
	)
}

// collectorChildQuery describes one bounded child-row hydration pass: the
// deterministic order, the per-collector maximum, the strings the failure
// paths must emit, and the accessors that place each row on its parent.
type collectorChildQuery[T any] struct {
	order            string
	perParentMaximum int
	queryLabel       string
	rowKind          string
	aggregateDetail  string
	perParentDetail  string
	slot             func(*collectorRecordSet) *[]T
	tenantOf         func(T) string
	collectorOf      func(T) string
}

// loadCollectorChildren runs one child query for an already-hydrated parent
// set. Every parent in records must exist before it is called: a row whose
// parent was not requested is rejected as control-plane corruption.
func loadCollectorChildren[T any](
	database *gorm.DB,
	scope Scope,
	collectorIDs []string,
	requested map[string]struct{},
	records map[string]*collectorRecordSet,
	spec collectorChildQuery[T],
) error {
	limit, err := aggregateCollectorChildLimit(
		len(collectorIDs),
		spec.perParentMaximum,
	)
	if err != nil {
		return err
	}
	var rows []T
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order(spec.order).
		Limit(limit).
		Find(&rows).Error; err != nil {
		return mapContextError(
			database.Statement.Context,
			spec.queryLabel,
			err,
		)
	}
	if len(rows) == limit {
		return collectorChildBoundsError(spec.aggregateDetail)
	}
	for _, row := range rows {
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			spec.tenantOf(row),
			spec.collectorOf(row),
			spec.rowKind,
		); err != nil {
			return err
		}
		target := spec.slot(records[spec.collectorOf(row)])
		if len(*target) == spec.perParentMaximum {
			return collectorChildBoundsError(spec.perParentDetail)
		}
		*target = append(*target, row)
	}
	return nil
}

func requireRequestedCollectorRow(
	scope Scope,
	requested map[string]struct{},
	tenantID string,
	collectorID string,
	rowKind string,
) error {
	if tenantID != scope.TenantID {
		return fmt.Errorf(
			"collector batch returned a cross-tenant %s row from the control-plane database",
			rowKind,
		)
	}
	if _, exists := requested[collectorID]; !exists {
		return fmt.Errorf(
			"collector batch returned an unknown-parent %s row from the control-plane database",
			rowKind,
		)
	}
	return nil
}

func aggregateCollectorChildLimit(
	parentCount int,
	perParentMaximum int,
) (int, error) {
	if parentCount < 0 || perParentMaximum < 0 {
		return 0, invalid("collector child limit cannot be negative")
	}
	if parentCount == 0 || perParentMaximum == 0 {
		return 1, nil
	}
	if parentCount > (math.MaxInt-1)/perParentMaximum {
		return 0, fmt.Errorf(
			"collector child query limit exceeds integer capacity: %w",
			control.ErrCapacityExceeded,
		)
	}
	return parentCount*perParentMaximum + 1, nil
}
