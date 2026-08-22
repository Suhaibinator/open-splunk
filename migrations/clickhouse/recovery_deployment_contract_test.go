package clickhouse_test

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

type deploymentComposeConfig struct {
	Services map[string]deploymentComposeService `yaml:"services"`
	Volumes  map[string]deploymentComposeVolume  `yaml:"volumes"`
}

type deploymentComposeVolume struct {
	External bool   `yaml:"external"`
	Name     string `yaml:"name"`
}

type deploymentComposeService struct {
	User        string                       `yaml:"user"`
	WorkingDir  string                       `yaml:"working_dir"`
	ReadOnly    bool                         `yaml:"read_only"`
	Privileged  bool                         `yaml:"privileged"`
	NetworkMode string                       `yaml:"network_mode"`
	PIDMode     string                       `yaml:"pid"`
	IPCMode     string                       `yaml:"ipc"`
	UTSMode     string                       `yaml:"uts"`
	UserNSMode  string                       `yaml:"userns_mode"`
	CgroupMode  string                       `yaml:"cgroup"`
	Command     []string                     `yaml:"command"`
	Volumes     []string                     `yaml:"volumes"`
	VolumesFrom []string                     `yaml:"volumes_from"`
	Tmpfs       []string                     `yaml:"tmpfs"`
	Networks    []string                     `yaml:"networks"`
	Devices     []any                        `yaml:"devices"`
	DeviceRules []string                     `yaml:"device_cgroup_rules"`
	CapAdd      []string                     `yaml:"cap_add"`
	CapDrop     []string                     `yaml:"cap_drop"`
	GroupAdd    []string                     `yaml:"group_add"`
	SecurityOpt []string                     `yaml:"security_opt"`
	Environment map[string]string            `yaml:"environment"`
	Profiles    []string                     `yaml:"profiles"`
	PidsLimit   int                          `yaml:"pids_limit"`
	Healthcheck deploymentComposeHealthcheck `yaml:"healthcheck"`
}

type deploymentComposeHealthcheck struct {
	Disable bool `yaml:"disable"`
}

func TestDeploymentClickHouseNativeRecoveryContract(t *testing.T) {
	t.Parallel()

	backupConfig := readFile(
		t,
		filepath.Join("..", "..", "deploy", "clickhouse-config", "recovery.xml"),
	)
	for _, fragment := range []string{
		"<open_splunk_recovery>",
		"<type>local</type>",
		"<path>/var/lib/open-splunk-clickhouse-backups/</path>",
		"<allowed_disk>open_splunk_recovery</allowed_disk>",
		"<allow_concurrent_backups>false</allow_concurrent_backups>",
		"<allow_concurrent_restores>false</allow_concurrent_restores>",
		"<remove_backup_files_after_failure>true</remove_backup_files_after_failure>",
	} {
		if !strings.Contains(backupConfig, fragment) {
			t.Errorf("ClickHouse recovery config is missing %q", fragment)
		}
	}
	if strings.Contains(backupConfig, "<allowed_path>") {
		t.Error("ClickHouse recovery config enables direct-path backups")
	}

	bootstrap := readFile(t, filepath.Join("..", "..", "deploy", "clickhouse-init.sh"))
	for _, fragment := range []string{
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
		"CREATE USER IF NOT EXISTS open_splunk_backup",
		"GRANT BACKUP, SHOW TABLES ON open_splunk.* TO open_splunk_backup",
		"CREATE USER IF NOT EXISTS open_splunk_restore",
		"GRANT CREATE DATABASE, SHOW TABLES ON open_splunk.* TO open_splunk_restore",
		"GRANT CREATE TABLE, INSERT, SELECT(visibility_seq) ON open_splunk.events TO open_splunk_restore",
		"GRANT CREATE TABLE, INSERT, SELECT(name, version) ON open_splunk.schema_migrations TO open_splunk_restore",
		"GRANT CREATE TABLE, INSERT, SELECT, TRUNCATE ON open_splunk.recovery_archive_markers TO open_splunk_restore",
		"GRANT CREATE TABLE, INSERT, SELECT(database_uuid, deployment_manifest_sha256, events_table_uuid, recovery_archive_markers_table_uuid, recovery_set_id, recovery_sets_table_uuid, restored_at, schema_migrations_table_uuid, slot), TRUNCATE ON open_splunk.recovery_sets TO open_splunk_restore",
		"GRANT SELECT ON system.disks TO open_splunk_restore",
		"GRANT SELECT ON system.mutations TO open_splunk_restore",
		"GRANT SHOW DATABASES ON *.* TO open_splunk_restore",
	} {
		if !strings.Contains(bootstrap, fragment) {
			t.Errorf("ClickHouse recovery principals are missing %q", fragment)
		}
	}
	for _, principal := range []string{
		"open_splunk_migrator",
		"open_splunk_runtime",
		"open_splunk_deletion",
		"open_splunk_backup",
		"open_splunk_restore",
	} {
		for _, statement := range []string{
			"REVOKE ALL ON *.* FROM " + principal,
			"REVOKE ALL FROM " + principal,
		} {
			if strings.Count(bootstrap, statement+";") != 1 {
				t.Errorf(
					"ClickHouse principal reset statement %q occurs %d times, want exactly once",
					statement,
					strings.Count(bootstrap, statement+";"),
				)
			}
		}
	}
	for _, prohibited := range []string{
		"GRANT ALL",
		"open_splunk_backup WITH GRANT OPTION",
		"open_splunk_restore WITH GRANT OPTION",
		"open_splunk_backup WITH ADMIN OPTION",
		"open_splunk_restore WITH ADMIN OPTION",
		"GRANT SELECT ON system.backups TO open_splunk_backup",
		"GRANT SELECT ON system.backups TO open_splunk_restore",
		"GRANT CREATE DATABASE, CREATE TABLE, INSERT ON open_splunk.* TO open_splunk_restore",
		" ON open_splunk*.",
		"DROP TABLE",
	} {
		if strings.Contains(bootstrap, prohibited) {
			t.Errorf("ClickHouse recovery principal contains prohibited grant %q", prohibited)
		}
	}

	compose := readDeploymentCompose(t)
	wantVolumes := []string{
		"clickhouse-data",
		"clickhouse-logs",
		"clickhouse-recovery",
		"server-exports",
		"server-lock",
		"server-recovery",
		"server-state",
	}
	gotVolumes := make([]string, 0, len(compose.Volumes))
	for name := range compose.Volumes {
		gotVolumes = append(gotVolumes, name)
	}
	slices.Sort(gotVolumes)
	if !slices.Equal(gotVolumes, wantVolumes) {
		t.Errorf("deployment Compose volumes = %q, want %q", gotVolumes, wantVolumes)
	}

	volumeBootstrap := requireDeploymentComposeService(t, compose, "clickhouse-recovery-volume-bootstrap")
	requireExactComposeSequence(t, "recovery volume bootstrap command", volumeBootstrap.Command, []string{
		"prepare-clickhouse-recovery-volume",
		"-path",
		"/var/lib/open-splunk/clickhouse-backups",
		"-log-path",
		"/var/log/clickhouse-server",
	})
	if volumeBootstrap.User != "0:65532" || volumeBootstrap.NetworkMode != "none" {
		t.Errorf(
			"recovery volume bootstrap identity/network = (%q, %q), want (0:65532, none)",
			volumeBootstrap.User,
			volumeBootstrap.NetworkMode,
		)
	}
	requireComposeConfinement(t, "recovery volume bootstrap", volumeBootstrap, 32)
	requireExactComposeSequence(t, "recovery volume bootstrap capabilities", volumeBootstrap.CapAdd, []string{"CHOWN", "DAC_OVERRIDE", "FOWNER"})
	requireExactComposeSequence(t, "recovery volume bootstrap mounts", volumeBootstrap.Volumes, []string{
		"clickhouse-recovery:/var/lib/open-splunk/clickhouse-backups",
		"clickhouse-logs:/var/log/clickhouse-server",
	})

	clickhouse := requireDeploymentComposeService(t, compose, "clickhouse")
	requireComposeValues(t, "ClickHouse recovery mounts", clickhouse.Volumes,
		"clickhouse-recovery:/var/lib/open-splunk-clickhouse-backups",
		"./clickhouse-config/recovery.xml:/etc/clickhouse-server/config.d/open_splunk_recovery.xml:ro",
	)
	requireExactComposeMountAtTarget(
		t,
		"normal ClickHouse recovery mount",
		clickhouse.Volumes,
		"/var/lib/open-splunk-clickhouse-backups",
		"clickhouse-recovery:/var/lib/open-splunk-clickhouse-backups",
	)
	requireComposeValues(t, "ClickHouse recovery supplemental groups", clickhouse.GroupAdd, "65532")

	restoreOverlay := readDeploymentComposeFile(
		t,
		filepath.Join("..", "..", "deploy", "docker-compose.restore.yaml"),
	)
	if len(restoreOverlay.Services) != 1 || len(restoreOverlay.Volumes) != 0 {
		t.Fatalf(
			"restore Compose overlay services/volumes = (%v, %v), want only a ClickHouse service override and no volume rebinding",
			restoreOverlay.Services,
			restoreOverlay.Volumes,
		)
	}
	restoreClickHouse := requireDeploymentComposeService(t, restoreOverlay, "clickhouse")
	requireExactComposeSequence(t, "restore ClickHouse overlay mounts", restoreClickHouse.Volumes, []string{
		"clickhouse-recovery:/var/lib/open-splunk-clickhouse-backups:ro",
	})
	recoveryTarget := readDeploymentComposeFile(
		t,
		filepath.Join("..", "..", "deploy", "docker-compose.recovery-target.yaml"),
	)
	if len(recoveryTarget.Services) != 0 || len(recoveryTarget.Volumes) != 4 {
		t.Fatalf(
			"recovery target binding services/volumes = (%v, %v), want no services and four persistent fresh-volume bindings",
			recoveryTarget.Services,
			recoveryTarget.Volumes,
		)
	}
	wantRecoveryVolumeVariables := map[string]string{
		"clickhouse-data": "${OPEN_SPLUNK_RECOVERY_CLICKHOUSE_DATA_VOLUME:?set a verified fresh ClickHouse data volume}",
		"clickhouse-logs": "${OPEN_SPLUNK_RECOVERY_CLICKHOUSE_LOGS_VOLUME:?set a verified fresh ClickHouse logs volume}",
		"server-state":    "${OPEN_SPLUNK_RECOVERY_SERVER_STATE_VOLUME:?set a verified fresh server state volume}",
		"server-exports":  "${OPEN_SPLUNK_RECOVERY_SERVER_EXPORTS_VOLUME:?set a verified fresh server exports volume}",
	}
	for volume, wantName := range wantRecoveryVolumeVariables {
		binding, exists := recoveryTarget.Volumes[volume]
		if !exists || !binding.External || binding.Name != wantName {
			t.Errorf(
				"recovery target binding %q = %#v, want external name %q",
				volume,
				binding,
				wantName,
			)
		}
	}
	runbook := readFile(t, filepath.Join("..", "..", "deploy", "README.md"))
	for _, command := range []string{
		"docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \\\n  up --detach --wait --no-build --no-deps clickhouse",
		"docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \\\n  --profile recovery run --rm --no-deps deployment-verify",
		"docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \\\n  --profile recovery run --rm --no-deps deployment-restore",
		"docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \\\n  run --rm --no-deps clickhouse-migrator",
		"docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \\\n  up --detach --wait --no-build --no-deps server",
	} {
		if !strings.Contains(runbook, command) {
			t.Errorf("deployment recovery runbook is missing persistent binding command %q", command)
		}
	}
	if strings.Contains(runbook, "Rotate its password or revoke the principal") {
		t.Error("deployment recovery runbook incorrectly presents restart-unstable revocation as durable")
	}

	const recoveryPath = "${OPEN_SPLUNK_DEPLOYMENT_RECOVERY_SET_PATH:-/var/lib/open-splunk/recovery/private/deployment-recovery-set}"
	const helperTmpfs = "/tmp:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532"
	const singletonLockPath = "/var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock"
	lockEnvironment := map[string]string{
		"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH": singletonLockPath,
	}
	backup := requireDeploymentComposeService(t, compose, "deployment-backup")
	if backup.User != "65532:65532" {
		t.Errorf("deployment backup user = %q, want 65532:65532", backup.User)
	}
	requireComposeConfinement(t, "deployment backup", backup, 64)
	requireNoComposeElevation(t, "deployment backup", backup, lockEnvironment)
	requireExactComposeSequence(t, "deployment backup command", backup.Command, []string{
		"backup-deployment-recovery-set",
		"-control-db", "/var/lib/open-splunk/state/private/open-splunk.db",
		"-master-key", "/var/lib/open-splunk/state/private/master.key",
		"-administrator-token-file", "/var/lib/open-splunk/state/private/administrator.token",
		"-destination", recoveryPath,
		"-archive-root", "/var/lib/open-splunk/clickhouse-backups",
		"-address", "clickhouse:9440",
		"-password-file", "/run/open-splunk/clickhouse/backup.password",
		"-ca-cert", "/run/open-splunk/clickhouse/ca.crt",
		"-server-name", "${OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME:?run ./generate-env.sh before docker compose up}",
	})
	requireExactComposeSequence(t, "deployment backup tmpfs", backup.Tmpfs, []string{helperTmpfs})
	requireExactComposeSequence(t, "deployment backup networks", backup.Networks, []string{"backend"})
	requireExactComposeSequence(t, "deployment backup profiles", backup.Profiles, []string{"recovery"})
	requireExactComposeSequence(t, "deployment backup mounts", backup.Volumes, []string{
		"server-state:/var/lib/open-splunk/state",
		"server-lock:/var/lib/open-splunk/lock",
		"server-recovery:/var/lib/open-splunk/recovery",
		"clickhouse-recovery:/var/lib/open-splunk/clickhouse-backups:ro",
		"${OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE:?run ./generate-env.sh before docker compose up}:/run/open-splunk/clickhouse/ca.crt:ro",
		"${OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE:?run ./generate-env.sh before docker compose up}:/run/open-splunk/clickhouse/backup.password:ro",
	})

	markerReconcile := requireDeploymentComposeService(t, compose, "deployment-marker-reconcile")
	if markerReconcile.User != "65532:65532" {
		t.Errorf("deployment marker reconcile user = %q, want 65532:65532", markerReconcile.User)
	}
	requireComposeConfinement(t, "deployment marker reconcile", markerReconcile, 64)
	requireNoComposeElevation(t, "deployment marker reconcile", markerReconcile, lockEnvironment)
	requireExactComposeSequence(t, "deployment marker reconcile command", markerReconcile.Command, []string{
		"reconcile-deployment-recovery-marker",
		"-recovery-set-id", "${OPEN_SPLUNK_STALE_RECOVERY_SET_ID:-}",
		"-confirm-recovery-set-id", "${OPEN_SPLUNK_CONFIRMED_STALE_RECOVERY_SET_ID:-}",
		"-backup-operation-uuid", "${OPEN_SPLUNK_STALE_BACKUP_OPERATION_UUID:-}",
		"-confirm-backup-operation-uuid", "${OPEN_SPLUNK_CONFIRMED_STALE_BACKUP_OPERATION_UUID:-}",
		"-address", "clickhouse:9440",
		"-password-file", "/run/open-splunk/clickhouse/backup.password",
		"-ca-cert", "/run/open-splunk/clickhouse/ca.crt",
		"-server-name", "${OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME:?run ./generate-env.sh before docker compose up}",
	})
	requireExactComposeSequence(t, "deployment marker reconcile tmpfs", markerReconcile.Tmpfs, []string{helperTmpfs})
	requireExactComposeSequence(t, "deployment marker reconcile networks", markerReconcile.Networks, []string{"backend"})
	requireExactComposeSequence(t, "deployment marker reconcile profiles", markerReconcile.Profiles, []string{"recovery"})
	requireExactComposeSequence(t, "deployment marker reconcile mounts", markerReconcile.Volumes, []string{
		"server-lock:/var/lib/open-splunk/lock",
		"${OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE:?run ./generate-env.sh before docker compose up}:/run/open-splunk/clickhouse/ca.crt:ro",
		"${OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE:?run ./generate-env.sh before docker compose up}:/run/open-splunk/clickhouse/backup.password:ro",
	})
	requireComposeServiceExcludes(t, "deployment marker reconcile", markerReconcile,
		"server-state", "server-recovery", "clickhouse-recovery",
		"migration.password", "runtime.password", "deletion.password", "restore.password",
	)

	verify := requireDeploymentComposeService(t, compose, "deployment-verify")
	if verify.User != "65532:65532" {
		t.Errorf("deployment verify user = %q, want 65532:65532", verify.User)
	}
	requireComposeConfinement(t, "deployment verify", verify, 64)
	requireNoComposeElevation(t, "deployment verify", verify, nil)
	requireExactComposeSequence(t, "deployment verify command", verify.Command, []string{
		"verify-deployment-recovery-set",
		"-source", recoveryPath,
		"-archive-root", "/var/lib/open-splunk/clickhouse-backups",
	})
	if verify.NetworkMode != "none" || len(verify.Networks) != 0 {
		t.Errorf("deployment verify network contract = mode %q networks %v, want none", verify.NetworkMode, verify.Networks)
	}
	requireExactComposeSequence(t, "deployment verify mounts", verify.Volumes, []string{
		"server-recovery:/var/lib/open-splunk/recovery:ro",
		"clickhouse-recovery:/var/lib/open-splunk/clickhouse-backups:ro",
	})
	requireComposeServiceExcludes(t, "deployment verify", verify,
		"server-state", "PASSWORD", "password", "ca.crt",
	)

	archiveDelete := requireDeploymentComposeService(t, compose, "deployment-archive-delete")
	if archiveDelete.User != "101:65532" || archiveDelete.WorkingDir != "/" ||
		archiveDelete.NetworkMode != "none" {
		t.Errorf(
			"deployment archive deletion identity/workdir/network = (%q, %q, %q), want (101:65532, /, none)",
			archiveDelete.User,
			archiveDelete.WorkingDir,
			archiveDelete.NetworkMode,
		)
	}
	requireComposeConfinement(t, "deployment archive deletion", archiveDelete, 32)
	requireNoComposeElevation(t, "deployment archive deletion", archiveDelete, nil)
	requireExactComposeSequence(t, "deployment archive deletion command", archiveDelete.Command, []string{
		"delete-deployment-recovery-archive",
		"-archive-root", "/var/lib/open-splunk/clickhouse-backups",
		"-archive-name", "${OPEN_SPLUNK_FAILED_RECOVERY_ARCHIVE_NAME:-}",
		"-confirm-archive-name", "${OPEN_SPLUNK_CONFIRMED_RECOVERY_ARCHIVE_NAME:-}",
	})
	requireExactComposeSequence(t, "deployment archive deletion mounts", archiveDelete.Volumes, []string{
		"clickhouse-recovery:/var/lib/open-splunk/clickhouse-backups",
	})
	requireExactComposeSequence(t, "deployment archive deletion profiles", archiveDelete.Profiles, []string{"recovery"})
	if len(archiveDelete.Networks) != 0 || len(archiveDelete.Tmpfs) != 0 {
		t.Errorf("deployment archive deletion has unnecessary authority: %+v", archiveDelete)
	}

	restore := requireDeploymentComposeService(t, compose, "deployment-restore")
	if restore.User != "65532:65532" {
		t.Errorf("deployment restore user = %q, want 65532:65532", restore.User)
	}
	requireComposeConfinement(t, "deployment restore", restore, 64)
	requireNoComposeElevation(t, "deployment restore", restore, lockEnvironment)
	requireExactComposeSequence(t, "deployment restore command", restore.Command, []string{
		"restore-deployment-recovery-set",
		"-source", recoveryPath,
		"-archive-root", "/var/lib/open-splunk/clickhouse-backups",
		"-control-db", "/var/lib/open-splunk/state/private/open-splunk.db",
		"-master-key", "/var/lib/open-splunk/state/private/master.key",
		"-administrator-token-file", "/var/lib/open-splunk/state/private/administrator.token",
		"-address", "clickhouse:9440",
		"-password-file", "/run/open-splunk/clickhouse/restore.password",
		"-ca-cert", "/run/open-splunk/clickhouse/ca.crt",
		"-server-name", "${OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME:?run ./generate-env.sh before docker compose up}",
	})
	requireExactComposeSequence(t, "deployment restore tmpfs", restore.Tmpfs, []string{helperTmpfs})
	requireExactComposeSequence(t, "deployment restore networks", restore.Networks, []string{"backend"})
	requireExactComposeSequence(t, "deployment restore profiles", restore.Profiles, []string{"recovery"})
	requireExactComposeSequence(t, "deployment restore mounts", restore.Volumes, []string{
		"server-state:/var/lib/open-splunk/state",
		"server-lock:/var/lib/open-splunk/lock",
		"server-recovery:/var/lib/open-splunk/recovery:ro",
		"clickhouse-recovery:/var/lib/open-splunk/clickhouse-backups:ro",
		"${OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE:?run ./generate-env.sh before docker compose up}:/run/open-splunk/clickhouse/ca.crt:ro",
		"${OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE:?run ./generate-env.sh before docker compose up}:/run/open-splunk/clickhouse/restore.password:ro",
	})

	server := requireDeploymentComposeService(t, compose, "server")
	if !maps.Equal(server.Environment, map[string]string{
		"TMPDIR":                                 "/tmp",
		"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH": singletonLockPath,
	}) {
		t.Errorf("long-running server environment = %q, want exact TMPDIR and singleton lock", server.Environment)
	}
	requireExactComposeMountAtTarget(
		t,
		"long-running server singleton lock mount",
		server.Volumes,
		"/var/lib/open-splunk/lock",
		"server-lock:/var/lib/open-splunk/lock",
	)
	requireComposeServiceExcludes(t, "long-running server", server,
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
		"backup.password",
		"restore.password",
	)
	for _, serviceName := range []string{"clickhouse-migrator", "server-bootstrap"} {
		service := requireDeploymentComposeService(t, compose, serviceName)
		if !service.Healthcheck.Disable {
			t.Errorf("%s inherits the server image healthcheck", serviceName)
		}
	}

	environmentExample := readFile(t, filepath.Join("..", "..", "deploy", ".env.example"))
	generator := readFile(t, filepath.Join("..", "..", "deploy", "generate-env.sh"))
	for _, name := range []string{
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE",
	} {
		if !strings.Contains(environmentExample, name+"=") {
			t.Errorf("deployment environment example is missing %s", name)
		}
		if !strings.Contains(generator, name) {
			t.Errorf("deployment environment generator is missing %s", name)
		}
	}
	for _, name := range []string{
		"OPEN_SPLUNK_STALE_RECOVERY_SET_ID",
		"OPEN_SPLUNK_CONFIRMED_STALE_RECOVERY_SET_ID",
		"OPEN_SPLUNK_STALE_BACKUP_OPERATION_UUID",
		"OPEN_SPLUNK_CONFIRMED_STALE_BACKUP_OPERATION_UUID",
	} {
		if !strings.Contains(environmentExample, "# "+name+"=") {
			t.Errorf("deployment environment example is missing inert %s", name)
		}
	}
}

func readDeploymentCompose(t *testing.T) deploymentComposeConfig {
	t.Helper()
	return readDeploymentComposeFile(
		t,
		filepath.Join("..", "..", "deploy", "docker-compose.yaml"),
	)
}

func readDeploymentComposeFile(t *testing.T, path string) deploymentComposeConfig {
	t.Helper()
	contents := readFile(t, path)
	var config deploymentComposeConfig
	if err := yaml.Unmarshal([]byte(contents), &config); err != nil {
		t.Fatalf("decode deployment Compose %q: %v", path, err)
	}
	return config
}

func requireDeploymentComposeService(
	t *testing.T,
	config deploymentComposeConfig,
	name string,
) deploymentComposeService {
	t.Helper()
	service, exists := config.Services[name]
	if !exists {
		t.Fatalf("deployment Compose has no %q service", name)
	}
	return service
}

func requireExactComposeSequence(t *testing.T, label string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s = %q, want %q", label, got, want)
	}
}

func requireComposeConfinement(
	t *testing.T,
	label string,
	service deploymentComposeService,
	wantPidsLimit int,
) {
	t.Helper()
	if !service.ReadOnly {
		t.Errorf("%s root filesystem is writable", label)
	}
	if !service.Healthcheck.Disable {
		t.Errorf("%s inherits the server image healthcheck", label)
	}
	requireExactComposeSequence(t, label+" dropped capabilities", service.CapDrop, []string{"ALL"})
	requireExactComposeSequence(t, label+" security options", service.SecurityOpt, []string{"no-new-privileges:true"})
	if service.PidsLimit != wantPidsLimit {
		t.Errorf("%s pids_limit = %d, want %d", label, service.PidsLimit, wantPidsLimit)
	}
	if service.Privileged || service.PIDMode != "" || service.IPCMode != "" ||
		service.UTSMode != "" || service.UserNSMode != "" || service.CgroupMode != "" ||
		len(service.Devices) != 0 || len(service.DeviceRules) != 0 || len(service.VolumesFrom) != 0 {
		t.Errorf("%s enables a privileged device or host-namespace boundary: %+v", label, service)
	}
}

func requireNoComposeElevation(
	t *testing.T,
	label string,
	service deploymentComposeService,
	wantEnvironment map[string]string,
) {
	t.Helper()
	if len(service.CapAdd) != 0 || len(service.GroupAdd) != 0 ||
		!maps.Equal(service.Environment, wantEnvironment) {
		t.Errorf(
			"%s has unnecessary capabilities/groups or environment %q, want %q: %+v",
			label,
			service.Environment,
			wantEnvironment,
			service,
		)
	}
}

func requireComposeValues(t *testing.T, label string, values []string, expected ...string) {
	t.Helper()
	for _, value := range expected {
		if !slices.Contains(values, value) {
			t.Errorf("%s = %q, missing %q", label, values, value)
		}
	}
}

func requireExactComposeMountAtTarget(
	t *testing.T,
	label string,
	mounts []string,
	target string,
	want string,
) {
	t.Helper()
	var matches []string
	for _, mount := range mounts {
		parts := strings.Split(mount, ":")
		if len(parts) >= 2 && parts[1] == target {
			matches = append(matches, mount)
		}
	}
	if !slices.Equal(matches, []string{want}) {
		t.Errorf("%s = %q, want exactly %q", label, matches, want)
	}
}

func requireComposeServiceExcludes(
	t *testing.T,
	label string,
	service deploymentComposeService,
	prohibited ...string,
) {
	t.Helper()
	values := append(append([]string(nil), service.Command...), service.Volumes...)
	values = append(values, service.Tmpfs...)
	values = append(values, service.Networks...)
	for key, value := range service.Environment {
		values = append(values, key, value)
	}
	joined := strings.Join(values, "\x00")
	for _, value := range prohibited {
		if strings.Contains(joined, value) {
			t.Errorf("%s contains prohibited %q", label, value)
		}
	}
}
