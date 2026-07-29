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

	capabilityLimit, err := aggregateCollectorChildLimit(
		len(collectorIDs),
		maximumCapabilities,
	)
	if err != nil {
		return nil, err
	}
	var capabilities []capabilityRecord
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order("collector_id, capability").
		Limit(capabilityLimit).
		Find(&capabilities).Error; err != nil {
		return nil, mapContextError(
			database.Statement.Context,
			"get collector batch capabilities",
			err,
		)
	}
	if len(capabilities) == capabilityLimit {
		return nil, collectorChildBoundsError(
			"capability rows exceed aggregate persisted bounds",
		)
	}
	for _, row := range capabilities {
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			row.TenantID,
			row.CollectorID,
			"capability",
		); err != nil {
			return nil, err
		}
		parent := records[row.CollectorID]
		if len(parent.capabilities) == maximumCapabilities {
			return nil, collectorChildBoundsError(
				"capabilities exceed per-collector persisted bounds",
			)
		}
		parent.capabilities = append(parent.capabilities, row)
	}

	indexLimit, err := aggregateCollectorChildLimit(
		len(collectorIDs),
		maximumAuthorizedIndexes,
	)
	if err != nil {
		return nil, err
	}
	var indexes []authorizedIndexRecord
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order("collector_id, index_name").
		Limit(indexLimit).
		Find(&indexes).Error; err != nil {
		return nil, mapContextError(
			database.Statement.Context,
			"get collector batch authorized indexes",
			err,
		)
	}
	if len(indexes) == indexLimit {
		return nil, collectorChildBoundsError(
			"authorized-index rows exceed aggregate persisted bounds",
		)
	}
	for _, row := range indexes {
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			row.TenantID,
			row.CollectorID,
			"authorized index",
		); err != nil {
			return nil, err
		}
		parent := records[row.CollectorID]
		if len(parent.indexes) == maximumAuthorizedIndexes {
			return nil, collectorChildBoundsError(
				"authorized indexes exceed per-collector persisted bounds",
			)
		}
		parent.indexes = append(parent.indexes, row)
	}

	inputLimit, err := aggregateCollectorChildLimit(
		len(collectorIDs),
		maximumInputs,
	)
	if err != nil {
		return nil, err
	}
	var inputs []inputRecord
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order("collector_id, input_id").
		Limit(inputLimit).
		Find(&inputs).Error; err != nil {
		return nil, mapContextError(
			database.Statement.Context,
			"get collector batch inputs",
			err,
		)
	}
	if len(inputs) == inputLimit {
		return nil, collectorChildBoundsError(
			"input rows exceed aggregate persisted bounds",
		)
	}
	for _, row := range inputs {
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			row.TenantID,
			row.CollectorID,
			"input",
		); err != nil {
			return nil, err
		}
		parent := records[row.CollectorID]
		if len(parent.inputs) == maximumInputs {
			return nil, collectorChildBoundsError(
				"inputs exceed per-collector persisted bounds",
			)
		}
		parent.inputs = append(parent.inputs, row)
	}

	var health []inputHealthRecord
	if err := database.
		Where(
			"tenant_id = ? AND collector_id IN ?",
			scope.TenantID,
			collectorIDs,
		).
		Order("collector_id, input_id").
		Limit(inputLimit).
		Find(&health).Error; err != nil {
		return nil, mapContextError(
			database.Statement.Context,
			"get collector batch input health",
			err,
		)
	}
	if len(health) == inputLimit {
		return nil, collectorChildBoundsError(
			"input-health rows exceed aggregate persisted bounds",
		)
	}
	for _, row := range health {
		if err := requireRequestedCollectorRow(
			scope,
			requested,
			row.TenantID,
			row.CollectorID,
			"input health",
		); err != nil {
			return nil, err
		}
		parent := records[row.CollectorID]
		if len(parent.health) == maximumInputs {
			return nil, collectorChildBoundsError(
				"input health exceeds per-collector persisted bounds",
			)
		}
		parent.health = append(parent.health, row)
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
