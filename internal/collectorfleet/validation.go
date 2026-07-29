package collectorfleet

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

type normalizedClaim struct {
	scope       Scope
	collectorID string
	bootEpoch   string
	streamID    string
	receivedAt  time.Time
	hello       Hello
	helloBytes  int
}

func normalizeClaim(request ClaimRequest) (normalizedClaim, error) {
	prepared, err := PrepareClaim(request)
	if err != nil {
		return normalizedClaim{}, err
	}
	return authorizePreparedClaim(
		prepared.normalized,
		request.Hello.AuthorizedIndexes,
	)
}

// PrepareClaim validates and copies every untrusted collector/server claim
// field except the credential-derived authorized-index snapshot. It performs
// the potentially large Hello normalization before SQLite takes its global
// writer lock.
//
// ClaimRequest.Hello.AuthorizedIndexes is deliberately ignored. The exact
// fresh credential scope must be supplied to ClaimPreparedInTransaction.
func PrepareClaim(request ClaimRequest) (PreparedClaim, error) {
	scope, err := normalizeScope(request.Scope)
	if err != nil {
		return PreparedClaim{}, err
	}
	for name, value := range map[string]string{
		"collector ID": request.CollectorID,
		"boot epoch":   request.BootEpoch,
		"stream ID":    request.StreamID,
		"instance ID":  request.Hello.InstanceID,
	} {
		if !validIdentifier(value) {
			return PreparedClaim{}, invalid("%s is not a canonical identifier", name)
		}
	}
	receivedAt, err := normalizeTimestamp("server receive time", request.ReceivedAt)
	if err != nil {
		return PreparedClaim{}, err
	}
	startedAt, err := normalizeTimestamp("collector start time", request.Hello.StartedAt)
	if err != nil {
		return PreparedClaim{}, err
	}
	metadata := []struct {
		name    string
		value   string
		maximum int
	}{
		{"collector version", request.Hello.CollectorVersion, maximumCollectorVersionBytes},
		{"hostname", request.Hello.Hostname, maximumHostnameBytes},
		{"operating system", request.Hello.OperatingSystem, maximumOperatingSystemBytes},
		{"architecture", request.Hello.Architecture, maximumArchitectureBytes},
	}
	aggregateBytes := 0
	for _, field := range metadata {
		if err := validateText(field.name, field.value, field.maximum, true); err != nil {
			return PreparedClaim{}, err
		}
		if !addBoundedBytes(&aggregateBytes, len(field.value), maximumHelloSnapshotBytes) {
			return PreparedClaim{}, invalid(
				"collector hello exceeds %d bytes",
				maximumHelloSnapshotBytes,
			)
		}
	}

	capabilities, err := normalizeCapabilities(request.Hello.Capabilities)
	if err != nil {
		return PreparedClaim{}, err
	}
	if !addBoundedBytes(&aggregateBytes, len(capabilities)*8, maximumHelloSnapshotBytes) {
		return PreparedClaim{}, invalid(
			"collector hello exceeds %d bytes",
			maximumHelloSnapshotBytes,
		)
	}
	inputs, err := normalizeInputRegistrations(
		request.Hello.Inputs,
		nil,
		&aggregateBytes,
	)
	if err != nil {
		return PreparedClaim{}, err
	}
	lastAcknowledged, err := normalizeOptionalUint64(
		"last acknowledged batch sequence",
		request.Hello.LastAcknowledgedBatchSequence,
	)
	if err != nil {
		return PreparedClaim{}, err
	}
	return PreparedClaim{
		valid: true,
		normalized: normalizedClaim{
			scope:       scope,
			collectorID: request.CollectorID,
			bootEpoch:   request.BootEpoch,
			streamID:    request.StreamID,
			receivedAt:  receivedAt,
			hello: Hello{
				InstanceID:                    request.Hello.InstanceID,
				ProtocolMajor:                 request.Hello.ProtocolMajor,
				ProtocolMinor:                 request.Hello.ProtocolMinor,
				CollectorVersion:              request.Hello.CollectorVersion,
				Hostname:                      request.Hello.Hostname,
				OperatingSystem:               request.Hello.OperatingSystem,
				Architecture:                  request.Hello.Architecture,
				StartedAt:                     startedAt,
				Capabilities:                  capabilities,
				Inputs:                        inputs,
				LastAcknowledgedBatchSequence: lastAcknowledged,
			},
			helloBytes: aggregateBytes,
		}}, nil
}

func authorizePreparedClaim(
	prepared normalizedClaim,
	authorizedIndexNames []string,
) (normalizedClaim, error) {
	authorizedIndexes, err := normalizeAuthorizedIndexes(authorizedIndexNames)
	if err != nil {
		return normalizedClaim{}, err
	}
	aggregateBytes := prepared.helloBytes
	authorizedSet := make(map[string]struct{}, len(authorizedIndexes))
	for _, indexName := range authorizedIndexes {
		authorizedSet[indexName] = struct{}{}
		if !addBoundedBytes(&aggregateBytes, len(indexName), maximumHelloSnapshotBytes) {
			return normalizedClaim{}, invalid(
				"collector hello exceeds %d bytes",
				maximumHelloSnapshotBytes,
			)
		}
	}
	for _, input := range prepared.hello.Inputs {
		if _, allowed := authorizedSet[input.IndexName]; !allowed {
			return normalizedClaim{}, invalid(
				"input index is not in the authorized index snapshot",
			)
		}
	}
	prepared.hello.AuthorizedIndexes = authorizedIndexes
	prepared.helloBytes = aggregateBytes
	return prepared, nil
}

func normalizeScope(scope Scope) (Scope, error) {
	if strings.TrimSpace(scope.TenantID) != scope.TenantID {
		return Scope{}, invalid("tenant ID must not contain surrounding whitespace")
	}
	if err := validateText("tenant ID", scope.TenantID, maximumTenantIDBytes, false); err != nil {
		return Scope{}, err
	}
	return scope, nil
}

func normalizeLease(lease Lease) (Lease, error) {
	scope, err := normalizeScope(lease.Scope)
	if err != nil {
		return Lease{}, err
	}
	for name, value := range map[string]string{
		"collector ID": lease.CollectorID,
		"boot epoch":   lease.BootEpoch,
		"stream ID":    lease.StreamID,
	} {
		if !validIdentifier(value) {
			return Lease{}, invalid("%s is not a canonical identifier", name)
		}
	}
	if lease.Generation == 0 || lease.Generation > math.MaxInt64 {
		return Lease{}, invalid("lease generation is outside the supported range")
	}
	lease.Scope = scope
	return lease, nil
}

func normalizeAdministration(input Administration) (Administration, error) {
	if input.State != AdministrativeStateEnabled &&
		input.State != AdministrativeStateDisabled {
		return Administration{}, invalid("administrative state is invalid")
	}
	result := Administration{State: input.State}
	if input.DisplayName != nil {
		displayName := strings.TrimSpace(*input.DisplayName)
		if err := validateText(
			"display name",
			displayName,
			maximumDisplayNameBytes,
			false,
		); err != nil {
			return Administration{}, err
		}
		result.DisplayName = &displayName
	}
	return result, nil
}

func normalizeHeartbeat(input Heartbeat) (Heartbeat, error) {
	if input.ObservationSequence == 0 || input.ObservationSequence > math.MaxInt64 {
		return Heartbeat{}, invalid("heartbeat observation sequence is outside the supported range")
	}
	observedAt, err := normalizeTimestamp("heartbeat observed time", input.ObservedAt)
	if err != nil {
		return Heartbeat{}, err
	}
	receivedAt, err := normalizeTimestamp("heartbeat receive time", input.ReceivedAt)
	if err != nil {
		return Heartbeat{}, err
	}
	queue, err := normalizeQueue(input.Queue)
	if err != nil {
		return Heartbeat{}, err
	}
	lastSent, err := normalizeOptionalUint64(
		"last sent batch sequence",
		input.LastSentBatchSequence,
	)
	if err != nil {
		return Heartbeat{}, err
	}
	lastAcknowledged, err := normalizeOptionalUint64(
		"last acknowledged batch sequence",
		input.LastAcknowledgedBatchSequence,
	)
	if err != nil {
		return Heartbeat{}, err
	}
	if input.ProcessResidentMemoryBytes > math.MaxInt64 {
		return Heartbeat{}, invalid("process resident memory is outside the supported range")
	}
	if math.IsNaN(input.ProcessCPUPercent) ||
		math.IsInf(input.ProcessCPUPercent, 0) ||
		input.ProcessCPUPercent < 0 ||
		input.ProcessCPUPercent > 1_000_000 {
		return Heartbeat{}, invalid("process CPU percent is outside the supported range")
	}
	aggregateBytes := 128
	inputs, err := normalizeInputHealth(input.Inputs, &aggregateBytes)
	if err != nil {
		return Heartbeat{}, err
	}
	return Heartbeat{
		ObservationSequence:           input.ObservationSequence,
		ObservedAt:                    observedAt,
		ReceivedAt:                    receivedAt,
		Queue:                         queue,
		Inputs:                        inputs,
		LastSentBatchSequence:         lastSent,
		LastAcknowledgedBatchSequence: lastAcknowledged,
		ProcessResidentMemoryBytes:    input.ProcessResidentMemoryBytes,
		ProcessCPUPercent:             input.ProcessCPUPercent,
	}, nil
}

func normalizeCapabilities(inputs []uint32) ([]uint32, error) {
	if len(inputs) > maximumCapabilities {
		return nil, invalid("capabilities cannot contain more than %d values", maximumCapabilities)
	}
	unique := make(map[uint32]struct{}, len(inputs))
	for _, capability := range inputs {
		if capability == 0 || capability > maximumPersistedEnumValue {
			return nil, invalid("collector capability is outside the supported range")
		}
		unique[capability] = struct{}{}
	}
	result := make([]uint32, 0, len(unique))
	for capability := range unique {
		result = append(result, capability)
	}
	slices.Sort(result)
	return result, nil
}

func normalizeAuthorizedIndexes(inputs []string) ([]string, error) {
	if len(inputs) == 0 || len(inputs) > maximumAuthorizedIndexes {
		return nil, invalid(
			"authorized indexes must contain between 1 and %d values",
			maximumAuthorizedIndexes,
		)
	}
	unique := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		canonical, err := control.NormalizeIndexName(input)
		if err != nil || canonical != input {
			return nil, invalid("authorized index name is not canonical")
		}
		unique[canonical] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for indexName := range unique {
		result = append(result, indexName)
	}
	slices.Sort(result)
	return result, nil
}

func normalizeInputRegistrations(
	inputs []InputRegistration,
	authorized map[string]struct{},
	aggregateBytes *int,
) ([]InputRegistration, error) {
	if len(inputs) > maximumInputs {
		return nil, invalid("input registrations cannot contain more than %d values", maximumInputs)
	}
	result := make([]InputRegistration, 0, len(inputs))
	seen := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if !validIdentifier(input.InputID) {
			return nil, invalid("input ID is not a canonical identifier")
		}
		if _, duplicate := seen[input.InputID]; duplicate {
			return nil, invalid("input registrations contain a duplicate input ID")
		}
		seen[input.InputID] = struct{}{}
		if input.InputType == 0 || input.InputType > maximumPersistedEnumValue {
			return nil, invalid("input type is outside the supported range")
		}
		canonicalIndex, err := control.NormalizeIndexName(input.IndexName)
		if err != nil || canonicalIndex != input.IndexName {
			return nil, invalid("input index name is not canonical")
		}
		if authorized != nil {
			if _, allowed := authorized[input.IndexName]; !allowed {
				return nil, invalid("input index is not in the authorized index snapshot")
			}
		}
		source, err := normalizeOptionalText("input source", input.Source, maximumSourceBytes)
		if err != nil {
			return nil, err
		}
		sourcetype, err := normalizeOptionalText(
			"input sourcetype",
			input.Sourcetype,
			maximumSourcetypeBytes,
		)
		if err != nil {
			return nil, err
		}
		recordBytes := len(input.InputID) + len(input.IndexName) + 16
		if source != nil {
			recordBytes += len(*source)
		}
		if sourcetype != nil {
			recordBytes += len(*sourcetype)
		}
		if !addBoundedBytes(aggregateBytes, recordBytes, maximumHelloSnapshotBytes) {
			return nil, invalid("collector hello exceeds %d bytes", maximumHelloSnapshotBytes)
		}
		result = append(result, InputRegistration{
			InputID: input.InputID, InputType: input.InputType,
			IndexName: input.IndexName, Source: source, Sourcetype: sourcetype,
		})
	}
	slices.SortFunc(result, func(left, right InputRegistration) int {
		return strings.Compare(left.InputID, right.InputID)
	})
	return result, nil
}

func normalizeQueue(input QueueTelemetry) (QueueTelemetry, error) {
	for name, value := range map[string]uint64{
		"queued events":             input.QueuedEvents,
		"queued bytes":              input.QueuedBytes,
		"sent events total":         input.SentEventsTotal,
		"acknowledged events total": input.AcknowledgedEventsTotal,
		"retried batches total":     input.RetriedBatchesTotal,
		"rejected events total":     input.RejectedEventsTotal,
		"dropped events total":      input.DroppedEventsTotal,
	} {
		if value > math.MaxInt64 {
			return QueueTelemetry{}, invalid("%s is outside the supported range", name)
		}
	}
	result := input
	if input.OldestEventAge != nil {
		if *input.OldestEventAge < 0 {
			return QueueTelemetry{}, invalid("oldest event age cannot be negative")
		}
		value := *input.OldestEventAge
		result.OldestEventAge = &value
	}
	return result, nil
}

func normalizeInputHealth(inputs []InputHealth, aggregateBytes *int) ([]InputHealth, error) {
	if len(inputs) > maximumInputs {
		return nil, invalid("input health cannot contain more than %d values", maximumInputs)
	}
	result := make([]InputHealth, 0, len(inputs))
	seen := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if !validIdentifier(input.InputID) {
			return nil, invalid("input health ID is not a canonical identifier")
		}
		if _, duplicate := seen[input.InputID]; duplicate {
			return nil, invalid("input health contains a duplicate input ID")
		}
		seen[input.InputID] = struct{}{}
		if input.State == 0 || input.State > maximumPersistedEnumValue {
			return nil, invalid("input health state is outside the supported range")
		}
		if err := validateText(
			"input health status message",
			input.StatusMessage,
			maximumStatusMessageBytes,
			true,
		); err != nil {
			return nil, err
		}
		for name, value := range map[string]uint64{
			"discovered sources": input.DiscoveredSources,
			"active sources":     input.ActiveSources,
			"events read total":  input.EventsReadTotal,
			"bytes read total":   input.BytesReadTotal,
		} {
			if value > math.MaxInt64 {
				return nil, invalid("%s is outside the supported range", name)
			}
		}
		if input.ActiveSources > input.DiscoveredSources {
			return nil, invalid("active sources cannot exceed discovered sources")
		}
		lastEventAt, err := normalizeOptionalTimestamp("last event time", input.LastEventAt)
		if err != nil {
			return nil, err
		}
		lastErrorAt, err := normalizeOptionalTimestamp("last error time", input.LastErrorAt)
		if err != nil {
			return nil, err
		}
		if !addBoundedBytes(
			aggregateBytes,
			len(input.InputID)+len(input.StatusMessage)+64,
			maximumHeartbeatSnapshotBytes,
		) {
			return nil, invalid(
				"collector heartbeat exceeds %d bytes",
				maximumHeartbeatSnapshotBytes,
			)
		}
		result = append(result, InputHealth{
			InputID: input.InputID, State: input.State,
			StatusMessage:     input.StatusMessage,
			DiscoveredSources: input.DiscoveredSources,
			ActiveSources:     input.ActiveSources,
			EventsReadTotal:   input.EventsReadTotal,
			BytesReadTotal:    input.BytesReadTotal,
			LastEventAt:       lastEventAt, LastErrorAt: lastErrorAt,
		})
	}
	slices.SortFunc(result, func(left, right InputHealth) int {
		return strings.Compare(left.InputID, right.InputID)
	})
	return result, nil
}

func normalizeOptionalUint64(name string, input *uint64) (*uint64, error) {
	if input == nil {
		return nil, nil
	}
	if *input > math.MaxInt64 {
		return nil, invalid("%s is outside the supported range", name)
	}
	value := *input
	return &value, nil
}

func normalizeOptionalText(name string, input *string, maximum int) (*string, error) {
	if input == nil {
		return nil, nil
	}
	if err := validateText(name, *input, maximum, false); err != nil {
		return nil, err
	}
	value := *input
	return &value, nil
}

func normalizeTimestamp(name string, input time.Time) (time.Time, error) {
	if input.IsZero() {
		return time.Time{}, invalid("%s is required", name)
	}
	canonicalInput := input.UTC()
	if canonicalInput.Year() > 9999 {
		return time.Time{}, invalid("%s is outside the supported range", name)
	}
	result := time.UnixMicro(canonicalInput.UnixMicro()).UTC()
	if !timestampIsPublic(result) ||
		!result.Equal(canonicalInput.Truncate(time.Microsecond)) {
		return time.Time{}, invalid("%s is outside the supported range", name)
	}
	return result, nil
}

func normalizeOptionalTimestamp(name string, input *time.Time) (*time.Time, error) {
	if input == nil {
		return nil, nil
	}
	value, err := normalizeTimestamp(name, *input)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func timestampIsPublic(value time.Time) bool {
	unixMicro := value.UnixMicro()
	return unixMicro > 0 &&
		unixMicro <= maximumPublicUnixMicro &&
		value.UTC().Year() <= 9999
}

func validateText(name, value string, maximum int, allowEmpty bool) error {
	if (!allowEmpty && value == "") ||
		len(value) > maximum ||
		!utf8.ValidString(value) ||
		strings.IndexByte(value, 0) >= 0 {
		return invalid("%s is invalid or exceeds %d bytes", name, maximum)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return invalid("%s contains a control character", name)
		}
	}
	return nil
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > maximumIdentifierBytes || !utf8.ValidString(value) {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		alphanumeric := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9'
		if alphanumeric {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:-", rune(character)) {
			return false
		}
	}
	return true
}

func addBoundedBytes(total *int, addition, maximum int) bool {
	if total == nil || addition < 0 || *total < 0 || *total > maximum-addition {
		return false
	}
	*total += addition
	return true
}

func invalid(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", control.ErrInvalidArgument, fmt.Sprintf(format, arguments...))
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return invalid("nil context")
	}
	return ctx.Err()
}

func mapContextError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
