package permission

import (
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CompareUserTypes executes this step in the permission backend flow.
func CompareUserTypes(userType1, userType2 proto_models.UserType) int {
	return int(userType1 - userType2)
}
