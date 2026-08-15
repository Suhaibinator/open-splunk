package queryexec

import (
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type resultColumnContractMode uint8

const (
	resultColumnRequireScanType resultColumnContractMode = 1 << iota
	resultColumnTrimDatabaseType
)

type resultColumnContract struct {
	name         string
	databaseType string
	scanType     reflect.Type
	nullable     bool
}

type resultColumnContractViolation uint8

const (
	resultColumnContractValid resultColumnContractViolation = iota
	resultColumnContractShapeMismatch
	resultColumnContractTypeMismatch
)

func validateResultColumnContracts(
	columns []string,
	columnTypes []driver.ColumnType,
	contracts []resultColumnContract,
	mode resultColumnContractMode,
) (resultColumnContractViolation, string) {
	if len(columns) != len(contracts) || len(columnTypes) != len(contracts) {
		return resultColumnContractShapeMismatch, ""
	}
	for index, contract := range contracts {
		if columns[index] != contract.name {
			return resultColumnContractShapeMismatch, ""
		}
	}
	for index, contract := range contracts {
		columnType := columnTypes[index]
		if isNilDriverValue(columnType) ||
			columnType.Name() != contract.name ||
			columnType.Nullable() != contract.nullable {
			return resultColumnContractTypeMismatch, contract.name
		}
		databaseType := columnType.DatabaseTypeName()
		if mode&resultColumnTrimDatabaseType != 0 {
			databaseType = strings.TrimSpace(databaseType)
		}
		if databaseType != contract.databaseType ||
			mode&resultColumnRequireScanType != 0 && columnType.ScanType() != contract.scanType {
			return resultColumnContractTypeMismatch, contract.name
		}
	}
	return resultColumnContractValid, ""
}
