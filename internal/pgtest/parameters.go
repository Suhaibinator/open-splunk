package pgtest

import (
	"strconv"
	"strings"
)

// Parameters compiles positional placeholders in dynamically assembled test
// templates to PostgreSQL parameters. It does not alter SQL dialect or values.
func Parameters(template string) string {
	var result strings.Builder
	var quote byte
	parameter := 0
	for i := 0; i < len(template); i++ {
		character := template[i]
		if quote != 0 {
			result.WriteByte(character)
			if character == quote {
				if i+1 < len(template) && template[i+1] == quote {
					i++
					result.WriteByte(template[i])
				} else {
					quote = 0
				}
			}
			continue
		}
		switch character {
		case '\'', '"':
			quote = character
			result.WriteByte(character)
		case '?':
			parameter++
			result.WriteByte('$')
			result.WriteString(strconv.Itoa(parameter))
		default:
			result.WriteByte(character)
		}
	}
	return result.String()
}
