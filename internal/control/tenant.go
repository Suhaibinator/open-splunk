package control

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const maximumTenantIDBytes = 255

func validateTenantID(value string) error {
	if value == "" || len(value) > maximumTenantIDBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return ErrInvalidArgument
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return ErrInvalidArgument
		}
	}
	return nil
}
