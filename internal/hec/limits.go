package hec

import (
	"errors"
	"fmt"
)

const (
	HardMaximumCompressedBodyBytes   int64 = 8 << 20
	HardMaximumDecompressedBodyBytes int64 = 8 << 20
	HardMaximumNormalizedBytes       int64 = 8 << 20
	HardMaximumEventBytes            int64 = 1 << 20
	HardMaximumEvents                      = 1_000
	HardMaximumJSONDepth                   = 16
	// These structural ceilings bound the decoded tree independently of the
	// source-byte ceiling. Tiny JSON scalars and member names otherwise amplify
	// an 8 MiB request into hundreds of MiB of heap before event validation.
	HardMaximumJSONValues                    = 16_384
	HardMaximumObjectMembers                 = 4_096
	HardMaximumHeaderBytes                   = 8 << 10
	HardMaximumRequestTargetBytes            = 8 << 10
	HardMaximumChannelBytes                  = 128
	HardMaximumAcknowledgmentIDs             = 1_000
	HardMaximumAcknowledgmentBodyBytes int64 = 64 << 10
	HardMaximumResponseBytes           int64 = 1 << 20
)

// Limits contains the independently enforced resource ceilings for the HEC
// wire layer. A caller may tighten DefaultLimits, but Validate rejects values
// above the durable contract.
type Limits struct {
	MaximumCompressedBodyBytes     int64
	MaximumDecompressedBodyBytes   int64
	MaximumNormalizedBytes         int64
	MaximumEventBytes              int64
	MaximumEvents                  int
	MaximumJSONDepth               int
	MaximumJSONValues              int
	MaximumObjectMembers           int
	MaximumHeaderBytes             int
	MaximumRequestTargetBytes      int
	MaximumChannelBytes            int
	MaximumAcknowledgmentIDs       int
	MaximumAcknowledgmentBodyBytes int64
	MaximumResponseBytes           int64
}

// DefaultLimits returns the HEC ceilings. JSON depth counts composite
// values below, but not including, the event envelope.
func DefaultLimits() Limits {
	return Limits{
		MaximumCompressedBodyBytes:     HardMaximumCompressedBodyBytes,
		MaximumDecompressedBodyBytes:   HardMaximumDecompressedBodyBytes,
		MaximumNormalizedBytes:         HardMaximumNormalizedBytes,
		MaximumEventBytes:              HardMaximumEventBytes,
		MaximumEvents:                  HardMaximumEvents,
		MaximumJSONDepth:               HardMaximumJSONDepth,
		MaximumJSONValues:              HardMaximumJSONValues,
		MaximumObjectMembers:           HardMaximumObjectMembers,
		MaximumHeaderBytes:             HardMaximumHeaderBytes,
		MaximumRequestTargetBytes:      HardMaximumRequestTargetBytes,
		MaximumChannelBytes:            HardMaximumChannelBytes,
		MaximumAcknowledgmentIDs:       HardMaximumAcknowledgmentIDs,
		MaximumAcknowledgmentBodyBytes: HardMaximumAcknowledgmentBodyBytes,
		MaximumResponseBytes:           HardMaximumResponseBytes,
	}
}

// Validate proves that every limit is positive and no wider than the fixed
// protocol ceiling.
func (limits Limits) Validate() error {
	checks := []struct {
		name string
		got  int64
		hard int64
	}{
		{"maximum compressed body bytes", limits.MaximumCompressedBodyBytes, HardMaximumCompressedBodyBytes},
		{"maximum decompressed body bytes", limits.MaximumDecompressedBodyBytes, HardMaximumDecompressedBodyBytes},
		{"maximum normalized bytes", limits.MaximumNormalizedBytes, HardMaximumNormalizedBytes},
		{"maximum event bytes", limits.MaximumEventBytes, HardMaximumEventBytes},
		{"maximum events", int64(limits.MaximumEvents), HardMaximumEvents},
		{"maximum JSON depth", int64(limits.MaximumJSONDepth), HardMaximumJSONDepth},
		{"maximum JSON values", int64(limits.MaximumJSONValues), HardMaximumJSONValues},
		{"maximum object members", int64(limits.MaximumObjectMembers), HardMaximumObjectMembers},
		{"maximum header bytes", int64(limits.MaximumHeaderBytes), HardMaximumHeaderBytes},
		{"maximum request target bytes", int64(limits.MaximumRequestTargetBytes), HardMaximumRequestTargetBytes},
		{"maximum channel bytes", int64(limits.MaximumChannelBytes), HardMaximumChannelBytes},
		{"maximum acknowledgment IDs", int64(limits.MaximumAcknowledgmentIDs), HardMaximumAcknowledgmentIDs},
		{"maximum acknowledgment body bytes", limits.MaximumAcknowledgmentBodyBytes, HardMaximumAcknowledgmentBodyBytes},
		{"maximum response bytes", limits.MaximumResponseBytes, HardMaximumResponseBytes},
	}
	for _, check := range checks {
		if check.got <= 0 {
			return fmt.Errorf("HEC %s must be positive", check.name)
		}
		if check.got > check.hard {
			return fmt.Errorf("HEC %s cannot exceed %d", check.name, check.hard)
		}
	}
	if limits.MaximumEventBytes > limits.MaximumDecompressedBodyBytes {
		return errors.New("HEC maximum event bytes cannot exceed maximum decompressed body bytes")
	}
	if limits.MaximumNormalizedBytes < limits.MaximumEventBytes {
		return errors.New("HEC maximum normalized bytes cannot be smaller than maximum event bytes")
	}
	return nil
}
