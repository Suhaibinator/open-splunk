package lookupcatalog

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
	"modernc.org/sqlite"
)

const (
	lookupDefinitionTextContainsFunction = "open_splunk_lookup_definition_text_contains_v1"
	lookupCatalogStateFunction           = "open_splunk_lookup_catalog_state_v1"
	lookupCatalogStateDomain             = "open-splunk/lookup-owner-catalog-state/v1\x00"
)

func init() {
	sqlite.MustRegisterDeterministicScalarFunction(
		lookupDefinitionTextContainsFunction,
		2,
		lookupDefinitionTextContains,
	)
	sqlite.MustRegisterFunction(lookupCatalogStateFunction, &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		MakeAggregate: func(sqlite.FunctionContext) (sqlite.AggregateFunction, error) {
			return &lookupCatalogStateAggregate{}, nil
		},
	})
}

func lookupDefinitionTextContains(
	_ *sqlite.FunctionContext,
	arguments []driver.Value,
) (driver.Value, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("lookup definition text predicate received %d arguments", len(arguments))
	}
	definitionBytes, ok := arguments[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("lookup definition text predicate received %T definition", arguments[0])
	}
	needle, ok := arguments[1].(string)
	if !ok || needle == "" || needle != strings.ToLower(needle) {
		return nil, fmt.Errorf("lookup definition text predicate received invalid search text")
	}
	definition := &opensplunk.LookupDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(definitionBytes, definition); err != nil {
		return nil, fmt.Errorf("decode lookup definition text projection: %w", err)
	}
	if strings.Contains(strings.ToLower(definition.GetName()), needle) ||
		strings.Contains(strings.ToLower(definition.GetDescription()), needle) {
		return int64(1), nil
	}
	return int64(0), nil
}

type lookupCatalogStateEntry struct {
	lookupID string
	version  uint64
}

type lookupCatalogStateAggregate struct {
	entries []lookupCatalogStateEntry
}

func (aggregate *lookupCatalogStateAggregate) Step(
	_ *sqlite.FunctionContext,
	arguments []driver.Value,
) error {
	if len(arguments) != 2 {
		return fmt.Errorf("lookup catalog state received %d arguments", len(arguments))
	}
	lookupID, ok := arguments[0].(string)
	if !ok || !validIdentity(lookupID, 128) {
		return fmt.Errorf("lookup catalog state received invalid lookup identity")
	}
	version, ok := arguments[1].(int64)
	if !ok || version < 1 {
		return fmt.Errorf("lookup catalog state received invalid definition version")
	}
	aggregate.entries = append(aggregate.entries, lookupCatalogStateEntry{
		lookupID: strings.Clone(lookupID),
		version:  uint64(version),
	})
	return nil
}

func (*lookupCatalogStateAggregate) WindowInverse(
	_ *sqlite.FunctionContext,
	_ []driver.Value,
) error {
	return fmt.Errorf("lookup catalog state does not support window evaluation")
}

func (aggregate *lookupCatalogStateAggregate) WindowValue(
	_ *sqlite.FunctionContext,
) (driver.Value, error) {
	entries := slices.Clone(aggregate.entries)
	slices.SortFunc(entries, func(left, right lookupCatalogStateEntry) int {
		return strings.Compare(left.lookupID, right.lookupID)
	})
	hash := sha256.New()
	_, _ = hash.Write([]byte(lookupCatalogStateDomain))
	buffer := make([]byte, 8)
	for index, entry := range entries {
		if index > 0 && entries[index-1].lookupID == entry.lookupID {
			return nil, fmt.Errorf("lookup catalog state contains duplicate identity")
		}
		binary.BigEndian.PutUint64(buffer, uint64(len(entry.lookupID)))
		_, _ = hash.Write(buffer)
		_, _ = hash.Write([]byte(entry.lookupID))
		binary.BigEndian.PutUint64(buffer, entry.version)
		_, _ = hash.Write(buffer)
	}
	return hash.Sum(nil), nil
}

func (aggregate *lookupCatalogStateAggregate) Final(*sqlite.FunctionContext) {
	aggregate.entries = nil
}

func emptyLookupCatalogState() [sha256.Size]byte {
	return sha256.Sum256([]byte(lookupCatalogStateDomain))
}
