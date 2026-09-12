package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
)

func TestRecoveryLockRejectsLegacySetting(t *testing.T) {
	for _, name := range []string{"legacy only", "both matching", "both different", "empty legacy"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			currentPath := filepath.Join(directory, "current.lock")
			legacyPath := filepath.Join(directory, "legacy.lock")
			values := map[string]string{legacyServerSingletonLockPathEnv: legacyPath}
			if name != "legacy only" {
				values[serverSingletonLockPathEnv] = currentPath
			}
			switch name {
			case "both matching":
				values[legacyServerSingletonLockPathEnv] = currentPath
			case "empty legacy":
				values[legacyServerSingletonLockPathEnv] = ""
			}
			for _, key := range []string{serverSingletonLockPathEnv, legacyServerSingletonLockPathEnv} {
				t.Setenv(key, values[key])
				if _, configured := values[key]; !configured {
					if err := os.Unsetenv(key); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, _, runtimeErr := parseRuntimeOptionsForTest(t, nil, values)
			if runtimeErr == nil || !strings.Contains(runtimeErr.Error(), "was renamed to") {
				t.Fatalf("runtime legacy configuration error = %v", runtimeErr)
			}
			for _, acquire := range []hostLockAcquirer{
				acquireHostServerLock,
				func() (*serverLock, error) {
					return acquireServerLock(filepath.Join(directory, "control.db"))
				},
			} {
				lock, err := acquire()
				if lock != nil {
					_ = lock.Close()
					t.Fatal("legacy recovery configuration acquired a lock")
				}
				if err == nil || !strings.Contains(err.Error(), "was renamed to") {
					t.Fatalf("recovery legacy configuration error = %v", err)
				}
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("rejected configuration created lock files: %v, %v", entries, err)
			}
		})
	}
}

func TestRecoveryLockExplicitDefaultUsesRuntimeDirectoryPolicy(t *testing.T) {
	t.Setenv(serverSingletonLockPathEnv, hostSingletonLockPath)
	path, err := configuredServerSingletonLockPath()
	if err != nil || path != hostSingletonLockPath {
		t.Fatalf("explicit default path = %q, %v", path, err)
	}
	// Do not acquire the host's actual default lock in the test suite.
	if err := validateServerSingletonLockDirectory(path); err != nil {
		t.Fatalf("explicit default rejected runtime directory policy: %v", err)
	}
}

func TestRecoveryCommandsShareRuntimeSingletonAcrossProcesses(t *testing.T) {
	for _, configuration := range []string{"environment", "cli override", "exact path spelling"} {
		t.Run(configuration, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			lockPath := filepath.Join(directory, "deployment.lock")
			if configuration == "exact path spelling" {
				lockPath = filepath.Join(directory, "deployment lock-λ.lock")
			}
			t.Setenv(serverSingletonLockPathEnv, lockPath)
			t.Setenv(legacyServerSingletonLockPathEnv, "")
			if err := os.Unsetenv(legacyServerSingletonLockPathEnv); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoverySingletonProcessHelper$")
			environmentPath := lockPath
			if configuration == "cli override" {
				environmentPath = filepath.Join(directory, "overridden.lock")
			}
			command.Env = []string{
				"OPEN_SPLUNK_TEST_LOCK_PROCESS=" + configuration,
				"OPEN_SPLUNK_TEST_LOCK_PATH=" + lockPath,
				"OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE=" + filepath.Join(directory, "live.db"),
				serverSingletonLockPathEnv + "=" + environmentPath,
				"TMPDIR=" + t.TempDir(),
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = stdin.Close()
					if err := command.Wait(); err != nil {
						t.Errorf("runtime lock process: %v: %s", err, stderr.String())
					}
				}
			})
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() || scanner.Text() != "locked" {
				output := scanner.Text()
				for scanner.Scan() {
					output += "\n" + scanner.Text()
				}
				err := command.Wait()
				waited = true
				t.Fatalf("runtime lock process did not become ready: %v: %s\n%s", err, output, stderr.String())
			}

			missing := filepath.Join(directory, "must-not-be-accessed")
			databasePath := filepath.Join(missing, "recovery.db")
			release := testRecoveryReleaseIdentity()
			commands := map[string]func() error{
				"backup-control-plane": func() error {
					return runBackupControlPlane(t.Context(), controlbackup.CreateOptions{
						DatabasePath: databasePath,
						Destination:  filepath.Join(missing, "bundle"),
					}, release, acquireServerLock)
				},
				"restore-control-plane": func() error {
					return runRestoreControlPlane(t.Context(), controlbackup.RestoreOptions{
						DatabasePath: databasePath,
						Source:       filepath.Join(missing, "bundle"),
					}, release, acquireHostServerLock)
				},
				"backup-deployment-recovery-set": func() error {
					return runBackupDeploymentRecoverySetWithDependencies(
						t.Context(),
						deploymentRecoveryBackupOptions{DatabasePath: databasePath},
						release,
						defaultDeploymentRecoveryDependencies(),
					)
				},
				"restore-deployment-recovery-set": func() error {
					return runRestoreDeploymentRecoverySetWithDependencies(
						t.Context(),
						deploymentRecoveryRestoreOptions{DatabasePath: databasePath},
						release,
						defaultDeploymentRecoveryDependencies(),
					)
				},
				"reconcile-deployment-recovery-marker": func() error {
					return runDeploymentRecoveryMarkerReconcile(
						t.Context(),
						validDeploymentRecoveryMarkerReconcileOptions(),
						defaultDeploymentRecoveryMarkerReconcileDependencies(),
					)
				},
			}
			for name, run := range commands {
				t.Run(name, func(t *testing.T) {
					if err := run(); !errors.Is(err, errServerAlreadyRunning) ||
						!strings.Contains(err.Error(), lockPath) {
						t.Fatalf("recovery did not contend on the runtime lock: %v", err)
					}
				})
			}
			if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("blocked recovery created state: %v", err)
			}

			original, err := os.Stat(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := stdin.Close(); err != nil {
				t.Fatal(err)
			}
			if err := command.Wait(); err != nil {
				waited = true
				t.Fatalf("stop runtime lock process: %v: %s", err, stderr.String())
			}
			waited = true
			for _, acquire := range []hostLockAcquirer{
				acquireHostServerLock,
				func() (*serverLock, error) {
					return acquireServerLock(filepath.Join(directory, "recovery.db"))
				},
			} {
				lock, err := acquire()
				if err != nil {
					t.Fatalf("stopped-server recovery lock: %v", err)
				}
				file, fileErr := lock.fileForExactPath(lockPath)
				if fileErr == nil {
					current, statErr := file.Stat()
					if statErr != nil || !os.SameFile(original, current) {
						t.Errorf("recovery did not reacquire the runtime inode: %v", statErr)
					}
				} else {
					t.Error(fileErr)
				}
				if err := lock.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRecoverySingletonProcessHelper(t *testing.T) {
	configuration := os.Getenv("OPEN_SPLUNK_TEST_LOCK_PROCESS")
	if configuration == "" {
		return
	}
	flags := flag.NewFlagSet("runtime-lock", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var arguments []string
	if configuration == "cli override" {
		arguments = []string{"-server-lock-file", os.Getenv("OPEN_SPLUNK_TEST_LOCK_PATH")}
	}
	config, err := parseRuntimeOptions(
		flags,
		arguments,
		runtimeEnvironment{lookup: os.LookupEnv, unset: os.Unsetenv},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := normalizeRuntimeOptions(&config); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireConfiguredServerLock(config.controlDBPath, config.serverLockFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lock.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := fmt.Fprintln(os.Stdout, "locked"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
}
