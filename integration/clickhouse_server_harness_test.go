//go:build !windows

package integration_test

import "github.com/Suhaibinator/open-splunk/internal/testsupport"

func clickHouseServerEnvironment(
	environment []string,
	clickHouse *testsupport.ClickHouseContainer,
) []string {
	environment = environmentWithValue(
		environment,
		"OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD",
		clickHouse.Password,
	)
	return environment
}

func clickHouseServerArguments(
	clickHouse *testsupport.ClickHouseContainer,
) []string {
	return []string{
		"-clickhouse-address=" + clickHouse.Address,
		"-clickhouse-database=" + clickHouse.Database,
		"-clickhouse-username=" + clickHouse.Username,
	}
}
