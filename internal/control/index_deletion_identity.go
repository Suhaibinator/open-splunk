package control

import "gorm.io/gorm"

func queryIndexDeletionIdentityInUse(
	database *gorm.DB,
	query string,
	arguments ...any,
) (bool, error) {
	var inUse int64
	result := database.Raw(query, arguments...).Scan(&inUse)
	if result.Error != nil {
		return false, result.Error
	}
	return inUse == 1, nil
}
