package lookupcatalog

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
)

const (
	lookupCatalogStateDomain = "open-splunk/lookup-owner-catalog-state/v1\x00"
)

type lookupCatalogStateEntry struct {
	lookupID string
	version  uint64
}

type lookupCatalogStateAggregate struct {
	entries []lookupCatalogStateEntry
}

func (aggregate *lookupCatalogStateAggregate) Step(
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
	_ []driver.Value,
) error {
	return fmt.Errorf("lookup catalog state does not support window evaluation")
}

func (aggregate *lookupCatalogStateAggregate) WindowValue() (driver.Value, error) {
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

func (aggregate *lookupCatalogStateAggregate) Final() {
	aggregate.entries = nil
}

func emptyLookupCatalogState() [sha256.Size]byte {
	return sha256.Sum256([]byte(lookupCatalogStateDomain))
}
