//go:build !windows

package integration_test

import "github.com/Suhaibinator/open-splunk/internal/testsupport"

func clickHouseServerEnvironment(
	environment []string,
	clickHouse *testsupport.ClickHouseContainer,
) []string {
	environment = environmentWithValue(
		environment,
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD",
		clickHouse.MigrationPassword,
	)
	environment = environmentWithValue(
		environment,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		clickHouse.RuntimePassword,
	)
	return environmentWithValue(
		environment,
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
		clickHouse.DeletionPassword,
	)
}

func clickHouseServerArguments(
	clickHouse *testsupport.ClickHouseContainer,
) []string {
	return []string{
		"-clickhouse-address=" + clickHouse.Address,
		"-clickhouse-database=" + clickHouse.Database,
		"-clickhouse-migration-username=" + clickHouse.MigrationUsername,
		"-clickhouse-runtime-username=" + clickHouse.RuntimeUsername,
		"-clickhouse-deletion-username=" + clickHouse.DeletionUsername,
	}
}
