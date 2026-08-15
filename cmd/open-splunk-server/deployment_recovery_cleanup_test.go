package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
)

const testDeploymentRecoveryDeleteArchiveName = "0123456789abcdef0123456789abcdef.tar.zst"

func TestRunDeploymentSubcommandRecognizesDeleteDeploymentRecoveryArchive(
	t *testing.T,
) {
	t.Parallel()

	handled, err := runDeploymentSubcommand([]string{
		"delete-deployment-recovery-archive",
		"-unknown",
	})
	if !handled || err == nil {
		t.Fatalf("cleanup recovery archive dispatch = (%t, %v), want (true, error)", handled, err)
	}
}

func TestDeleteDeploymentRecoveryArchiveFlagContract(t *testing.T) {
	t.Parallel()

	valid := []string{
		"-archive-root", "/var/lib/open-splunk/clickhouse-backups",
		"-archive-name", testDeploymentRecoveryDeleteArchiveName,
		"-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName,
	}
	for name, arguments := range map[string][]string{
		"missing all":  nil,
		"missing root": {"-archive-name", testDeploymentRecoveryDeleteArchiveName, "-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName},
		"missing name": {"-archive-root", "/recovery", "-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName},
		"missing confirmation": {
			"-archive-root", "/recovery", "-archive-name", testDeploymentRecoveryDeleteArchiveName,
		},
		"duplicate root": append(append([]string{}, valid...), "-archive-root", "/other"),
		"duplicate name": append(append([]string{}, valid...), "-archive-name", testDeploymentRecoveryDeleteArchiveName),
		"duplicate confirmation": append(
			append([]string{}, valid...),
			"-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName,
		),
		"unknown":    {"-unknown"},
		"positional": append(append([]string{}, valid...), "unexpected"),
		"relative root": {
			"-archive-root", "recovery",
			"-archive-name", testDeploymentRecoveryDeleteArchiveName,
			"-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName,
		},
		"filesystem root": {
			"-archive-root", "/",
			"-archive-name", testDeploymentRecoveryDeleteArchiveName,
			"-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName,
		},
		"unclean root": {
			"-archive-root", "/recovery/../archive",
			"-archive-name", testDeploymentRecoveryDeleteArchiveName,
			"-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName,
		},
		"uppercase archive ID": {
			"-archive-root", "/recovery",
			"-archive-name", "0123456789ABCDEF0123456789abcdef.tar.zst",
			"-confirm-archive-name", "0123456789ABCDEF0123456789abcdef.tar.zst",
		},
		"wrong suffix": {
			"-archive-root", "/recovery",
			"-archive-name", "0123456789abcdef0123456789abcdef.zip",
			"-confirm-archive-name", "0123456789abcdef0123456789abcdef.zip",
		},
		"mismatched confirmation": {
			"-archive-root", "/recovery",
			"-archive-name", testDeploymentRecoveryDeleteArchiveName,
			"-confirm-archive-name", "fedcba9876543210fedcba9876543210.tar.zst",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			effectiveUIDCalls := 0
			effectiveGIDCalls := 0
			removeCalls := 0
			err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
				t.Context(),
				arguments,
				deploymentRecoveryArchiveDeleteDependencies{
					effectiveUID: func() int {
						effectiveUIDCalls++
						return deploymentRecoveryArchiveUID
					},
					effectiveGID: func() int {
						effectiveGIDCalls++
						return deploymentRecoveryArchiveGID
					},
					delete: func(
						context.Context,
						recoveryset.DeleteAttestedArchiveOptions,
					) error {
						removeCalls++
						return nil
					},
				},
			)
			if err == nil {
				t.Fatalf("arguments %#v succeeded", arguments)
			}
			if effectiveUIDCalls != 0 || effectiveGIDCalls != 0 || removeCalls != 0 {
				t.Fatalf("invalid flags reached dependencies: euid=%d egid=%d remove=%d",
					effectiveUIDCalls,
					effectiveGIDCalls,
					removeCalls,
				)
			}
		})
	}
}

func TestDeleteDeploymentRecoveryArchiveRequiresExactClickHouseUID(t *testing.T) {
	t.Parallel()

	for _, effectiveUID := range []int{0, 65532, deploymentRecoveryArchiveUID + 1} {
		t.Run(fmt.Sprintf("uid-%d", effectiveUID), func(t *testing.T) {
			t.Parallel()

			removeCalls := 0
			err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
				t.Context(),
				validDeploymentRecoveryArchiveDeleteArguments(),
				deploymentRecoveryArchiveDeleteDependencies{
					effectiveUID: func() int { return effectiveUID },
					effectiveGID: func() int { return deploymentRecoveryArchiveGID },
					delete: func(
						context.Context,
						recoveryset.DeleteAttestedArchiveOptions,
					) error {
						removeCalls++
						return nil
					},
				},
			)
			if err == nil || !strings.Contains(err.Error(), "want exactly 101") {
				t.Fatalf("effective UID %d error = %v", effectiveUID, err)
			}
			if removeCalls != 0 {
				t.Fatalf("effective UID %d reached removal", effectiveUID)
			}
		})
	}
}

func TestDeleteDeploymentRecoveryArchiveRequiresExactClickHouseGID(t *testing.T) {
	t.Parallel()

	for _, effectiveGID := range []int{0, 101, deploymentRecoveryArchiveGID + 1} {
		t.Run(fmt.Sprintf("gid-%d", effectiveGID), func(t *testing.T) {
			t.Parallel()

			deleteCalls := 0
			err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
				t.Context(),
				validDeploymentRecoveryArchiveDeleteArguments(),
				deploymentRecoveryArchiveDeleteDependencies{
					effectiveUID: func() int { return deploymentRecoveryArchiveUID },
					effectiveGID: func() int { return effectiveGID },
					delete: func(
						context.Context,
						recoveryset.DeleteAttestedArchiveOptions,
					) error {
						deleteCalls++
						return nil
					},
				},
			)
			if err == nil || !strings.Contains(err.Error(), "want exactly 65532") {
				t.Fatalf("effective GID %d error = %v", effectiveGID, err)
			}
			if deleteCalls != 0 {
				t.Fatalf("effective GID %d reached deletion", effectiveGID)
			}
		})
	}
}

func TestDeleteDeploymentRecoveryArchiveCallsExactRemoval(t *testing.T) {
	t.Parallel()

	var got recoveryset.DeleteAttestedArchiveOptions
	removeCalls := 0
	err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
		t.Context(),
		validDeploymentRecoveryArchiveDeleteArguments(),
		deploymentRecoveryArchiveDeleteDependencies{
			effectiveUID: func() int { return deploymentRecoveryArchiveUID },
			effectiveGID: func() int { return deploymentRecoveryArchiveGID },
			delete: func(
				ctx context.Context,
				options recoveryset.DeleteAttestedArchiveOptions,
			) error {
				if ctx == nil {
					t.Fatal("removal received nil context")
				}
				removeCalls++
				got = options
				return nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := recoveryset.DeleteAttestedArchiveOptions{
		ArchiveRoot:          "/var/lib/open-splunk/clickhouse-backups",
		ArchiveName:          testDeploymentRecoveryDeleteArchiveName,
		ConfirmedArchiveName: testDeploymentRecoveryDeleteArchiveName,
		ArchiveOwnership:     deploymentRecoveryArchiveOwnership(),
	}
	if removeCalls != 1 || got != want {
		t.Fatalf("removal = calls:%d options:%+v, want one %+v", removeCalls, got, want)
	}
}

func TestDeleteDeploymentRecoveryArchiveRejectsContextAndDependencyFailures(
	t *testing.T,
) {
	t.Parallel()

	validDependencies := deploymentRecoveryArchiveDeleteDependencies{
		effectiveUID: func() int { return deploymentRecoveryArchiveUID },
		effectiveGID: func() int { return deploymentRecoveryArchiveGID },
		delete: func(
			context.Context,
			recoveryset.DeleteAttestedArchiveOptions,
		) error {
			return nil
		},
	}
	//nolint:staticcheck // The injected command boundary must reject a nil context.
	if err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
		nil,
		validDeploymentRecoveryArchiveDeleteArguments(),
		validDependencies,
	); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("nil-context error = %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
		canceled,
		validDeploymentRecoveryArchiveDeleteArguments(),
		validDependencies,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled-context error = %v", err)
	}
	for name, dependencies := range map[string]deploymentRecoveryArchiveDeleteDependencies{
		"missing effective UID": {effectiveGID: validDependencies.effectiveGID, delete: validDependencies.delete},
		"missing effective GID": {effectiveUID: validDependencies.effectiveUID, delete: validDependencies.delete},
		"missing deletion": {
			effectiveUID: validDependencies.effectiveUID,
			effectiveGID: validDependencies.effectiveGID,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
				t.Context(),
				validDeploymentRecoveryArchiveDeleteArguments(),
				dependencies,
			); err == nil || !strings.Contains(err.Error(), "dependencies are incomplete") {
				t.Fatalf("dependency error = %v", err)
			}
		})
	}
}

func TestDeleteDeploymentRecoveryArchivePropagatesRemovalFailure(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected removal failure")
	err := runDeleteDeploymentRecoveryArchiveSubcommandWithDependencies(
		t.Context(),
		validDeploymentRecoveryArchiveDeleteArguments(),
		deploymentRecoveryArchiveDeleteDependencies{
			effectiveUID: func() int { return deploymentRecoveryArchiveUID },
			effectiveGID: func() int { return deploymentRecoveryArchiveGID },
			delete: func(
				context.Context,
				recoveryset.DeleteAttestedArchiveOptions,
			) error {
				return injected
			},
		},
	)
	if !errors.Is(err, injected) {
		t.Fatalf("removal failure = %v, want injected error", err)
	}
}

func validDeploymentRecoveryArchiveDeleteArguments() []string {
	return []string{
		"-archive-root", "/var/lib/open-splunk/clickhouse-backups",
		"-archive-name", testDeploymentRecoveryDeleteArchiveName,
		"-confirm-archive-name", testDeploymentRecoveryDeleteArchiveName,
	}
}
