package ingest

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// IngestionSourceKind identifies the trusted transport family which admitted
// an event. It is server-derived provenance, never a client-selected field.
type IngestionSourceKind uint8

const (
	IngestionSourceKindUnspecified IngestionSourceKind = iota
	IngestionSourceKindNativeCollector
	IngestionSourceKindHEC
)

// IngestionSource is the durable identity of the transport principal which
// admitted a batch. Native collectors retain their collector identity; HEC
// requests use the stable ingestion-token record ID and have no CollectorID.
type IngestionSource struct {
	Kind        IngestionSourceKind
	ID          string
	CollectorID string
}

// NativeCollectorSource returns canonical native-collector provenance.
func NativeCollectorSource(collectorID string) IngestionSource {
	return IngestionSource{
		Kind:        IngestionSourceKindNativeCollector,
		ID:          collectorID,
		CollectorID: collectorID,
	}
}

// HECSource returns canonical HTTP Event Collector provenance. tokenID is the
// stable control-plane record identity, not the plaintext credential, prefix,
// digest, name, or request channel.
func HECSource(tokenID string) IngestionSource {
	return IngestionSource{Kind: IngestionSourceKindHEC, ID: tokenID}
}

// CanonicalIngestionSource preserves source compatibility for native callers
// which predate explicit provenance. An explicit source must agree with the
// legacy collector field rather than silently overriding it.
func CanonicalIngestionSource(source IngestionSource, legacyCollectorID string) (IngestionSource, error) {
	if source.Kind == IngestionSourceKindUnspecified && source.ID == "" && source.CollectorID == "" {
		source = NativeCollectorSource(legacyCollectorID)
	}
	if err := source.Validate(); err != nil {
		return IngestionSource{}, err
	}
	if legacyCollectorID != source.CollectorID {
		return IngestionSource{}, errors.New("ingestion source does not match collector identity")
	}
	return source, nil
}

// Validate enforces the closed durable source domain. IDs deliberately share
// the native protocol's conservative identifier byte ceiling without requiring
// UUID syntax.
func (source IngestionSource) Validate() error {
	if !validIngestionSourceID(source.ID) {
		return errors.New("ingestion source ID is invalid")
	}
	switch source.Kind {
	case IngestionSourceKindNativeCollector:
		if source.CollectorID != source.ID {
			return errors.New("native ingestion source must match collector ID")
		}
	case IngestionSourceKindHEC:
		if source.CollectorID != "" {
			return errors.New("HEC ingestion source cannot carry a collector ID")
		}
	default:
		return errors.New("ingestion source kind is invalid")
	}
	return nil
}

func validIngestionSourceID(value string) bool {
	return len(value) >= 1 &&
		len(value) <= int(HardMaxIDBytes) &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, 0)
}
