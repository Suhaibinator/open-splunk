package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

type deploymentCommandGroup uint8

const (
	deploymentCommandGroupRuntime deploymentCommandGroup = iota
	deploymentCommandGroupCoordinatedRecovery
	deploymentCommandGroupControlPlaneRecovery
)

type deploymentCommand struct {
	name        string
	summary     string
	usageSuffix string
	group       deploymentCommandGroup
	options     string
	run         func([]string, io.Writer) error
}

var deploymentCommands = []deploymentCommand{
	{
		name:    "version",
		summary: "Print the embedded release identity.",
		group:   deploymentCommandGroupRuntime,
		run: func(arguments []string, output io.Writer) error {
			return runVersionSubcommand(arguments, output)
		},
	},
	{
		name:        "healthcheck",
		summary:     "Probe an exact loopback liveness or readiness endpoint.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupRuntime,
		options: `  -url <url>           strict loopback HTTP or HTTPS /healthz or /readyz URL
  -ca-cert <path>      explicit certificate-only CA bundle for HTTPS
  -server-name <name>  explicit TLS certificate name for HTTPS`,
		run: deploymentCommandRunner(runDeploymentHealthcheckSubcommand),
	},
	{
		name:        "provision-administrator-token",
		summary:     "Publish a generated administrator token into private runtime state.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupRuntime,
		options: `  -source <path>       read-only generated administrator-token source
  -destination <path>  private runtime administrator-token destination`,
		run: deploymentCommandRunner(runProvisionAdministratorTokenSubcommand),
	},
	{
		name:        "migrate-clickhouse",
		summary:     "Apply and verify ClickHouse migrations with the migration principal.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupRuntime,
		options: `  -address <host:port>  ClickHouse native TLS address
  -password-file <path> read-only migration-principal password file
  -ca-cert <path>       explicit certificate-only CA bundle
  -server-name <name>   explicit ClickHouse TLS certificate name`,
		run: deploymentCommandRunner(runDeploymentClickHouseMigrationSubcommand),
	},
	{
		name:        "prepare-clickhouse-recovery-volume",
		summary:     "Prepare empty ClickHouse archive and optional log volume roots.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupCoordinatedRecovery,
		options: `  -path <path>      absolute ClickHouse recovery archive volume root
  -log-path <path>  optional absolute ClickHouse log volume root`,
		run: deploymentCommandRunner(runPrepareClickHouseRecoveryVolumeSubcommand),
	},
	{
		name:        "backup-deployment-recovery-set",
		summary:     "Create one coordinated control-plane and ClickHouse recovery set.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupCoordinatedRecovery,
		options:     coordinatedRecoverySetOptions("backup"),
		run:         deploymentCommandRunner(runBackupDeploymentRecoverySetSubcommand),
	},
	{
		name:        "verify-deployment-recovery-set",
		summary:     "Verify both members of a coordinated deployment recovery set.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupCoordinatedRecovery,
		options: `  -source <path>        absolute private recovery-set directory
  -archive-root <path>  absolute ClickHouse recovery archive root`,
		run: deploymentCommandRunner(runVerifyDeploymentRecoverySetSubcommand),
	},
	{
		name:        "restore-deployment-recovery-set",
		summary:     "Restore a verified coordinated recovery set into fresh deployment state.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupCoordinatedRecovery,
		options:     coordinatedRecoverySetOptions("restore"),
		run:         deploymentCommandRunner(runRestoreDeploymentRecoverySetSubcommand),
	},
	{
		name:        "reconcile-deployment-recovery-marker",
		summary:     "Clear one exactly identified interrupted backup marker after reconciliation.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupCoordinatedRecovery,
		options: `  -recovery-set-id <id>                 exact retained recovery-set marker identity
  -confirm-recovery-set-id <id>         repeat the exact recovery-set identity
  -backup-operation-uuid <uuid>          canonical retained backup operation UUID
  -confirm-backup-operation-uuid <uuid>  repeat the exact backup operation UUID
  -address <host:port>                    ClickHouse native TLS address
  -password-file <path>                   read-only backup-principal password file
  -ca-cert <path>                         explicit certificate-only CA bundle
  -server-name <name>                     explicit ClickHouse TLS certificate name`,
		run: deploymentCommandRunner(runDeploymentRecoveryMarkerReconcileSubcommand),
	},
	{
		name:        "delete-deployment-recovery-archive",
		summary:     "Delete one attested ClickHouse archive after successful retention handling.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupCoordinatedRecovery,
		options: `  -archive-root <path>          exact ClickHouse recovery archive volume root
  -archive-name <name>          canonical archive name to destroy
  -confirm-archive-name <name>  repeat the exact archive name to attest deletion`,
		run: deploymentCommandRunner(runDeleteDeploymentRecoveryArchiveSubcommand),
	},
	{
		name:        "backup-control-plane",
		summary:     "Back up only SQLite and local control-plane artifacts.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupControlPlaneRecovery,
		options:     controlPlaneRecoveryOptions("backup"),
		run:         deploymentCommandRunner(runBackupControlPlaneSubcommand),
	},
	{
		name:        "verify-control-plane-backup",
		summary:     "Verify a control-plane-only backup bundle.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupControlPlaneRecovery,
		options:     "  -source <path>  absolute private control-plane bundle directory",
		run:         deploymentCommandRunner(runVerifyControlPlaneBackupSubcommand),
	},
	{
		name:        "restore-control-plane",
		summary:     "Restore only SQLite and local control-plane artifacts.",
		usageSuffix: "[options]",
		group:       deploymentCommandGroupControlPlaneRecovery,
		options:     controlPlaneRecoveryOptions("restore"),
		run:         deploymentCommandRunner(runRestoreControlPlaneSubcommand),
	},
}

func coordinatedRecoverySetOptions(operation string) string {
	if operation == "backup" {
		return `  -destination <path>                absolute new recovery-set directory
  -control-db <path>                 absolute stopped-server SQLite database path
  -master-key <path>                 absolute matching server master-key path
  -administrator-token-file <path>   absolute matching administrator-token path
  -search-artifact-directory <path>  optional absolute retained-search directory
  -archive-root <path>               absolute ClickHouse recovery archive root
  -address <host:port>               ClickHouse native TLS address
  -password-file <path>              read-only backup-principal password file
  -ca-cert <path>                    explicit certificate-only CA bundle
  -server-name <name>                explicit ClickHouse TLS certificate name`
	}
	return `  -source <path>                     absolute verified recovery-set directory
  -control-db <path>                 absolute absent or exactly resumed SQLite database path
  -master-key <path>                 absolute absent or exactly resumed server master-key path
  -administrator-token-file <path>   absolute absent or exactly resumed administrator-token path
  -search-artifact-directory <path>  optional absolute absent or exactly resumed retained-search directory
  -archive-root <path>               absolute ClickHouse recovery archive root
  -address <host:port>               ClickHouse native TLS address
  -password-file <path>              read-only restore-principal password file
  -ca-cert <path>                    explicit certificate-only CA bundle
  -server-name <name>                explicit ClickHouse TLS certificate name`
}

func controlPlaneRecoveryOptions(operation string) string {
	pathRole := "matching"
	bundleFlag := "-destination"
	bundleRole := "new"
	if operation == "restore" {
		pathRole = "absent"
		bundleFlag = "-source"
		bundleRole = "verified"
	}
	return fmt.Sprintf(`  %s <path>                 absolute %s private bundle directory
  -control-db <path>                 absolute %s SQLite database path
  -master-key <path>                 absolute %s server master-key path
  -administrator-token-file <path>   absolute %s administrator-token path
  -search-artifact-directory <path>  optional absolute %s retained-search directory`,
		bundleFlag,
		bundleRole,
		pathRole,
		pathRole,
		pathRole,
		pathRole,
	)
}

func deploymentCommandRunner(run func([]string) error) func([]string, io.Writer) error {
	return func(arguments []string, _ io.Writer) error {
		return run(arguments)
	}
}

func runDeploymentSubcommandWithOutput(arguments []string, output io.Writer) (bool, error) {
	if len(arguments) == 0 {
		return false, nil
	}
	if output == nil {
		return true, errors.New("deployment command output is required")
	}
	if arguments[0] == "help" || isDeploymentHelpFlag(arguments[0]) {
		if len(arguments) == 1 {
			return true, writeDeploymentCommandInventory(output)
		}
		if arguments[0] != "help" {
			return true, errors.New("root help does not accept a command after a help flag; use help <command>")
		}
		if len(arguments) != 2 {
			return true, errors.New("help accepts exactly one command name")
		}
		command, found := findDeploymentCommand(arguments[1])
		if !found {
			return true, fmt.Errorf("unknown help command %q", arguments[1])
		}
		return true, writeDeploymentCommandHelp(output, command)
	}

	command, found := findDeploymentCommand(arguments[0])
	if !found {
		return false, nil
	}
	for _, argument := range arguments[1:] {
		if isDeploymentHelpFlag(argument) {
			return true, writeDeploymentCommandHelp(output, command)
		}
	}
	return true, command.run(arguments[1:], output)
}

func isDeploymentHelpFlag(argument string) bool {
	return argument == "-h" || argument == "-help" || argument == "--help"
}

func findDeploymentCommand(name string) (deploymentCommand, bool) {
	for _, command := range deploymentCommands {
		if command.name == name {
			return command, true
		}
	}
	return deploymentCommand{}, false
}

func writeDeploymentCommandInventory(output io.Writer) error {
	var help strings.Builder
	help.WriteString(`Usage:
  open-splunk-server [server options]
  open-splunk-server <command> [options]
  open-splunk-server help <command>

Open Splunk server and isolated deployment operations.
`)
	help.WriteString("\nServer options:\n")
	runtimeFlags := flag.NewFlagSet("open-splunk-server", flag.ContinueOnError)
	runtimeFlags.SetOutput(&help)
	var runtimeOptions options
	registerRuntimeFlags(runtimeFlags, &runtimeOptions)
	runtimeFlags.PrintDefaults()
	for _, group := range []deploymentCommandGroup{
		deploymentCommandGroupRuntime,
		deploymentCommandGroupCoordinatedRecovery,
		deploymentCommandGroupControlPlaneRecovery,
	} {
		help.WriteString("\n")
		help.WriteString(deploymentCommandGroupHeading(group))
		help.WriteString("\n")
		for _, command := range deploymentCommands {
			if command.group == group {
				fmt.Fprintf(&help, "  %-42s %s\n", command.name, command.summary)
			}
		}
	}
	help.WriteString(`
Coordinated recovery covers both control-plane state and ClickHouse data. Use
prepare, backup, verify, and restore in that order. Reconcile a retained marker
or delete an attested archive only when the recovery procedure directs it.
Control-plane-only bundles cannot restore a complete deployment.

Use "open-splunk-server help <command>" for command options.
`)
	if _, err := io.WriteString(output, help.String()); err != nil {
		return fmt.Errorf("write deployment command help: %w", err)
	}
	return nil
}

func writeDeploymentCommandHelp(output io.Writer, command deploymentCommand) error {
	var help strings.Builder
	fmt.Fprintf(&help, "Usage:\n  open-splunk-server %s", command.name)
	if command.usageSuffix != "" {
		help.WriteString(" ")
		help.WriteString(command.usageSuffix)
	}
	help.WriteString("\n\n")
	help.WriteString(command.summary)
	help.WriteString("\n\nScope:\n  ")
	help.WriteString(deploymentCommandGroupScope(command.group))
	help.WriteString("\n")
	if command.options != "" {
		help.WriteString("\nOptions:\n")
		help.WriteString(command.options)
		help.WriteString("\n")
	}
	if _, err := io.WriteString(output, help.String()); err != nil {
		return fmt.Errorf("write %s help: %w", command.name, err)
	}
	return nil
}

func deploymentCommandGroupHeading(group deploymentCommandGroup) string {
	switch group {
	case deploymentCommandGroupRuntime:
		return "Deployment commands:"
	case deploymentCommandGroupCoordinatedRecovery:
		return "Coordinated deployment recovery (control plane and ClickHouse; stopped server only):"
	case deploymentCommandGroupControlPlaneRecovery:
		return "Control-plane-only recovery (SQLite and local artifacts; stopped server only):"
	default:
		return "Unknown commands:"
	}
}

func deploymentCommandGroupScope(group deploymentCommandGroup) string {
	switch group {
	case deploymentCommandGroupRuntime:
		return "isolated deployment operation"
	case deploymentCommandGroupCoordinatedRecovery:
		return "coordinated deployment recovery: control plane and ClickHouse; server must be stopped"
	case deploymentCommandGroupControlPlaneRecovery:
		return "control-plane-only recovery: SQLite and local artifacts; server must be stopped"
	default:
		return "unknown"
	}
}
