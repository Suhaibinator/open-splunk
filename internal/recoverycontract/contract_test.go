package recoverycontract

import (
	"strings"
	"testing"
)

func TestConstantsMatchRecoveryProtocol(t *testing.T) {
	t.Parallel()

	if Disk != "open_splunk_recovery" {
		t.Fatalf("Disk = %q", Disk)
	}
	if CanonicalDatabase != "open_splunk" {
		t.Fatalf("CanonicalDatabase = %q", CanonicalDatabase)
	}
	if ArchiveDatabasePrefix != "open_splunk_recovery_" {
		t.Fatalf("ArchiveDatabasePrefix = %q", ArchiveDatabasePrefix)
	}
	if ArchiveSuffix != ".tar.zst" {
		t.Fatalf("ArchiveSuffix = %q", ArchiveSuffix)
	}
	if RecoverySetIDLength != 32 {
		t.Fatalf("RecoverySetIDLength = %d", RecoverySetIDLength)
	}
}

func TestValidRecoverySetIDUsesExactLowercaseHexGrammar(t *testing.T) {
	t.Parallel()

	valid := []string{
		"0123456789abcdef0123456789abcdef",
		strings.Repeat("0", RecoverySetIDLength),
		strings.Repeat("f", RecoverySetIDLength),
	}
	for _, value := range valid {
		if !ValidRecoverySetID(value) {
			t.Errorf("ValidRecoverySetID(%q) = false", value)
		}
	}

	invalid := []string{
		"",
		strings.Repeat("a", RecoverySetIDLength-1),
		strings.Repeat("a", RecoverySetIDLength+1),
		strings.Repeat("A", RecoverySetIDLength),
		strings.Repeat("g", RecoverySetIDLength),
		strings.Repeat("0", RecoverySetIDLength-1) + "-",
	}
	for _, value := range invalid {
		if ValidRecoverySetID(value) {
			t.Errorf("ValidRecoverySetID(%q) = true", value)
		}
	}
}

func TestValidArchiveNameUsesExactRecoveryGrammar(t *testing.T) {
	t.Parallel()

	id := "0123456789abcdef0123456789abcdef"
	for _, value := range []string{
		id + ArchiveSuffix,
		strings.Repeat("0", RecoverySetIDLength) + ArchiveSuffix,
	} {
		if !ValidArchiveName(value) {
			t.Errorf("ValidArchiveName(%q) = false", value)
		}
	}
	for _, value := range []string{
		"",
		id,
		strings.ToUpper(id) + ArchiveSuffix,
		id + ".zip",
		"../" + id + ArchiveSuffix,
	} {
		if ValidArchiveName(value) {
			t.Errorf("ValidArchiveName(%q) = true", value)
		}
	}
}

func TestValidDatabaseNameUsesClosedRecoveryGrammar(t *testing.T) {
	t.Parallel()

	id := "0123456789abcdef0123456789abcdef"
	valid := []string{
		CanonicalDatabase,
		ArchiveDatabasePrefix + id,
	}
	for _, value := range valid {
		if !ValidDatabaseName(value) {
			t.Errorf("ValidDatabaseName(%q) = false", value)
		}
	}

	invalid := []string{
		"",
		"Open_Splunk",
		CanonicalDatabase + " ",
		CanonicalDatabase + ".events",
		CanonicalDatabase + "' OR 1 = 1 --",
		ArchiveDatabasePrefix + strings.Repeat("a", RecoverySetIDLength-1),
		ArchiveDatabasePrefix + strings.Repeat("A", RecoverySetIDLength),
		"open_splunk_restore",
		"open_splunk_restore_" + id,
		"open_splunk_backup_" + id,
	}
	for _, value := range invalid {
		if ValidDatabaseName(value) {
			t.Errorf("ValidDatabaseName(%q) = true", value)
		}
	}
}
