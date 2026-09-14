package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeploymentCommandHelpInventoryAndForms(t *testing.T) {
	var root bytes.Buffer
	handled, err := runDeploymentSubcommandWithOutput([]string{"help"}, &root)
	if err != nil || !handled {
		t.Fatalf("root help dispatch = (%v, %v), want (true, nil)", handled, err)
	}
	for _, heading := range []string{
		"Server options:",
		"Deployment commands:",
		"Coordinated deployment recovery (control plane and ClickHouse; stopped server only):",
		"Control-plane-only recovery (SQLite and local artifacts; stopped server only):",
	} {
		if !strings.Contains(root.String(), heading) {
			t.Errorf("root help omitted heading %q:\n%s", heading, root.String())
		}
	}
	for _, option := range []string{
		"-administrator-token-file",
		"-clickhouse-address",
		"-server-lock-file",
	} {
		if !strings.Contains(root.String(), option) {
			t.Errorf("root help omitted runtime option %q:\n%s", option, root.String())
		}
	}
	for _, arguments := range [][]string{{"-h"}, {"-help"}, {"--help"}} {
		var output bytes.Buffer
		handled, err := runDeploymentSubcommandWithOutput(arguments, &output)
		if err != nil || !handled || output.String() != root.String() {
			t.Errorf("root help %v = (%v, %v, %q), want canonical inventory", arguments, handled, err, output.String())
		}
	}
	for _, command := range deploymentCommands {
		if !strings.Contains(root.String(), "  "+command.name+" ") {
			t.Errorf("root help omitted command %q:\n%s", command.name, root.String())
		}
		var canonical string
		for _, arguments := range [][]string{
			{"help", command.name},
			{command.name, "-h"},
			{command.name, "-help"},
			{command.name, "--help"},
		} {
			var output bytes.Buffer
			handled, err := runDeploymentSubcommandWithOutput(arguments, &output)
			if err != nil || !handled {
				t.Errorf("help %v dispatch = (%v, %v), want (true, nil)", arguments, handled, err)
				continue
			}
			if !strings.Contains(output.String(), "Usage:\n  open-splunk-server "+command.name) ||
				!strings.Contains(output.String(), command.summary) {
				t.Errorf("help %v is incomplete:\n%s", arguments, output.String())
			}
			if canonical == "" {
				canonical = output.String()
			} else if output.String() != canonical {
				t.Errorf("help %v differs from canonical help", arguments)
			}
		}
	}
}

func TestDeploymentCommandHelpPrecedesAllOtherCommandArguments(t *testing.T) {
	for _, command := range deploymentCommands {
		var output bytes.Buffer
		handled, err := runDeploymentSubcommandWithOutput(
			[]string{command.name, "-unknown-that-must-not-be-parsed", "/missing/credential", "--help"},
			&output,
		)
		if err != nil || !handled || !strings.Contains(output.String(), command.summary) {
			t.Errorf("late help for %q = (%v, %v, %q)", command.name, handled, err, output.String())
		}
	}
}

func TestDeploymentCommandInventorySeparatesRecoveryScopes(t *testing.T) {
	wantGroups := map[string]deploymentCommandGroup{
		"version":                              deploymentCommandGroupRuntime,
		"healthcheck":                          deploymentCommandGroupRuntime,
		"provision-administrator-token":        deploymentCommandGroupRuntime,
		"migrate-clickhouse":                   deploymentCommandGroupRuntime,
		"prepare-clickhouse-recovery-volume":   deploymentCommandGroupCoordinatedRecovery,
		"backup-deployment-recovery-set":       deploymentCommandGroupCoordinatedRecovery,
		"verify-deployment-recovery-set":       deploymentCommandGroupCoordinatedRecovery,
		"restore-deployment-recovery-set":      deploymentCommandGroupCoordinatedRecovery,
		"reconcile-deployment-recovery-marker": deploymentCommandGroupCoordinatedRecovery,
		"delete-deployment-recovery-archive":   deploymentCommandGroupCoordinatedRecovery,
		"backup-control-plane":                 deploymentCommandGroupControlPlaneRecovery,
		"verify-control-plane-backup":          deploymentCommandGroupControlPlaneRecovery,
		"restore-control-plane":                deploymentCommandGroupControlPlaneRecovery,
	}
	if len(deploymentCommands) != len(wantGroups) {
		t.Fatalf("deployment command count = %d, want %d", len(deploymentCommands), len(wantGroups))
	}
	seen := make(map[string]struct{}, len(deploymentCommands))
	for _, command := range deploymentCommands {
		want, known := wantGroups[command.name]
		if !known {
			t.Errorf("undocumented deployment command %q", command.name)
			continue
		}
		if _, duplicate := seen[command.name]; duplicate {
			t.Errorf("duplicate deployment command %q", command.name)
		}
		seen[command.name] = struct{}{}
		if command.group != want {
			t.Errorf("deployment command %q group = %d, want %d", command.name, command.group, want)
		}
		if command.summary == "" || (command.name != "version" && command.options == "") {
			t.Errorf("deployment command %q has incomplete help metadata", command.name)
		}
	}
}

func TestDeploymentCommandHelpRejectsUnknownOrExtraTargets(t *testing.T) {
	for _, arguments := range [][]string{
		{"help", "unknown"},
		{"help", "version", "extra"},
	} {
		var output bytes.Buffer
		handled, err := runDeploymentSubcommandWithOutput(arguments, &output)
		if err == nil || !handled {
			t.Errorf("help %v dispatch = (%v, %v), want (true, error)", arguments, handled, err)
		}
		if output.Len() != 0 {
			t.Errorf("invalid help %v wrote stdout %q", arguments, output.String())
		}
	}
}

func TestDeploymentCommandHelpIsSideEffectFreeAcrossProcesses(t *testing.T) {
	if commandName := os.Getenv("OPEN_SPLUNK_TEST_HELP_COMMAND"); commandName != "" {
		if commandName == "root" {
			os.Args = []string{"open-splunk-server", "-help"}
		} else {
			os.Args = []string{"open-splunk-server", commandName, "--help"}
		}
		if exitCode := run(); exitCode != 0 {
			t.Fatalf("child help exit code = %d, want 0", exitCode)
		}
		return
	}

	targets := []string{"root"}
	for _, command := range deploymentCommands {
		targets = append(targets, command.name)
	}
	for _, commandName := range targets {
		t.Run(commandName, func(t *testing.T) {
			root := t.TempDir()
			forbidden := filepath.Join(root, "must-not-exist")
			process := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDeploymentCommandHelpIsSideEffectFreeAcrossProcesses$")
			process.Env = append(os.Environ(),
				"OPEN_SPLUNK_TEST_HELP_COMMAND="+commandName,
				"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH="+filepath.Join(forbidden, "legacy.lock"),
				"OPEN_SPLUNK_SERVER_LOCK_FILE="+filepath.Join(forbidden, "server.lock"),
				"OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE="+filepath.Join(forbidden, "control.db"),
				"OPEN_SPLUNK_SERVER_MASTER_KEY_FILE="+filepath.Join(forbidden, "master.key"),
				"OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN_FILE="+filepath.Join(forbidden, "administrator.token"),
			)
			stdout, err := process.Output()
			if err != nil {
				t.Fatalf("help child: %v", err)
			}
			usage := "Usage:\n  open-splunk-server " + commandName
			if commandName == "root" {
				usage = "Usage:\n  open-splunk-server [server options]"
			}
			if !strings.Contains(string(stdout), usage) {
				t.Fatalf("help child output:\n%s", stdout)
			}
			if _, err := os.Lstat(forbidden); !os.IsNotExist(err) {
				t.Fatalf("help accessed deployment state: %v", err)
			}
		})
	}
}
