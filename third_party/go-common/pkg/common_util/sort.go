package common_util

import (
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// BuildOrderClause executes its operation in the shared utility flow used by service and persistence layers.
func BuildOrderClause(sort *proto_models.SortRequest, field string, defaultDesc bool) string {
	direction := "ASC"
	if SortDesc(sort, defaultDesc) {
		direction = "DESC"
	}
	return fmt.Sprintf("%s %s", field, direction)
}

// SortDesc executes its operation in the shared utility flow used by service and persistence layers.
func SortDesc(sort *proto_models.SortRequest, defaultDesc bool) bool {
	if sort == nil {
		return defaultDesc
	}
	return sort.SortDesc
}
