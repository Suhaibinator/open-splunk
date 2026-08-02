// Package recoverycontract defines the immutable names shared by the
// ClickHouse recovery producer, verifier, and restore coordinator.
package recoverycontract

const (
	Disk                  = "open_splunk_recovery"
	CanonicalDatabase     = "open_splunk"
	ArchiveDatabasePrefix = "open_splunk_recovery_"
	ArchiveSuffix         = ".tar.zst"
	RecoverySetIDLength   = 32
)

// ValidRecoverySetID reports whether value is the exact lowercase hexadecimal
// recovery-set identifier admitted by the recovery protocol.
func ValidRecoverySetID(value string) bool {
	if len(value) != RecoverySetIDLength {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < '0' || character > '9') &&
			(character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// ValidArchiveName reports whether value is the exact native archive name
// derived from one recovery-set identifier.
func ValidArchiveName(value string) bool {
	return len(value) == RecoverySetIDLength+len(ArchiveSuffix) &&
		ValidRecoverySetID(value[:RecoverySetIDLength]) &&
		value[RecoverySetIDLength:] == ArchiveSuffix
}

// ValidDatabaseName reports whether value is the canonical production
// database or an archive alias bound to one recovery-set ID.
func ValidDatabaseName(value string) bool {
	if value == CanonicalDatabase {
		return true
	}
	return validPrefixedDatabaseName(value, ArchiveDatabasePrefix)
}

func validPrefixedDatabaseName(value, prefix string) bool {
	if len(value) != len(prefix)+RecoverySetIDLength || value[:len(prefix)] != prefix {
		return false
	}
	return ValidRecoverySetID(value[len(prefix):])
}
