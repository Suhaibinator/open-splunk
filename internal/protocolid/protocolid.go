// Package protocolid validates the bounded ASCII identifiers used by the
// collector protocol and its durable control-plane representations.
package protocolid

// MaximumBytes is the protocol-wide hard limit for collector, stream, input,
// batch, event, and instance identifiers.
const MaximumBytes uint32 = 128

// Valid reports whether value is a canonical protocol identifier.
func Valid(value string) bool {
	return ValidWithMaximum(value, MaximumBytes)
}

// ValidWithMaximum reports whether value is a canonical protocol identifier
// within maximumBytes. The caller-provided maximum may tighten, but cannot
// widen, the protocol-wide hard limit.
func ValidWithMaximum(value string, maximumBytes uint32) bool {
	if maximumBytes > MaximumBytes {
		maximumBytes = MaximumBytes
	}
	if len(value) == 0 || uint64(len(value)) > uint64(maximumBytes) {
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
		if index == 0 || character != '.' && character != '_' &&
			character != ':' && character != '-' {
			return false
		}
	}
	return true
}
