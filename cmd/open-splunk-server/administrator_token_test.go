package main

import (
	"strings"
	"testing"
)

// Privileged operations must come from a persisted GoCommon user, so the
// removed CLI credential paths cannot silently re-enable independent admins.
func TestAdministratorCredentialFlagsAreUnavailable(t *testing.T) {
	for _, argument := range []string{"-administrator-token=secret-must-not-leak", "-administrator-token-file=/missing"} {
		_, _, err := parseRuntimeOptionsForTest(t, []string{argument}, nil)
		if err == nil || strings.Contains(err.Error(), "secret-must-not-leak") {
			t.Fatalf("retired credential flag result: %v", err)
		}
	}
}
func TestAdministratorProvisioningHelpIsUnavailable(t *testing.T) {
	_, found := findDeploymentCommand("provision-administrator-token")
	if found {
		t.Fatal("retired administrator provisioning remains registered")
	}
}
