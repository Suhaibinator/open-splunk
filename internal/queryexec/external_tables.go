package queryexec

import (
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
)

func appendExternalTableOption(
	options []clickhousedriver.QueryOption,
	tables []*ext.Table,
) []clickhousedriver.QueryOption {
	if len(tables) == 0 {
		return options
	}
	return append(options, clickhousedriver.WithExternalTable(tables...))
}
