package main

import (
	"flag"
	"io"
	"testing"
)

func TestClickHouseSkipMigrationsFlag(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "default", want: false},
		{name: "enabled", args: []string{"-clickhouse-skip-migrations"}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags := flag.NewFlagSet("open-splunk-server", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			var config options
			registerClickHouseSkipMigrationsFlag(flags, &config)
			if err := flags.Parse(test.args); err != nil {
				t.Fatal(err)
			}
			if config.clickhouseSkipMigrations != test.want {
				t.Fatalf(
					"-clickhouse-skip-migrations = %t, want %t",
					config.clickhouseSkipMigrations,
					test.want,
				)
			}
		})
	}
}
