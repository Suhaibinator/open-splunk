package hec

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const authorizationPrefix = "Splunk "

// ParseAuthorization accepts exactly one case-sensitive
// "Authorization: Splunk <plaintext-token>" value. It performs syntax and
// resource validation only; credential lookup and purpose checks remain at the
// protected authentication boundary.
func ParseAuthorization(values []string, maximumHeaderBytes int) (string, error) {
	if len(values) == 0 {
		return "", NewProtocolError(ErrorTokenRequired, nil)
	}
	if len(values) != 1 {
		return "", NewProtocolError(ErrorInvalidAuthorization, nil)
	}
	value := values[0]
	if maximumHeaderBytes <= 0 || maximumHeaderBytes > HardMaximumHeaderBytes ||
		len(value) > maximumHeaderBytes || !utf8.ValidString(value) {
		return "", NewProtocolError(ErrorInvalidAuthorization, nil)
	}
	if !strings.HasPrefix(value, authorizationPrefix) {
		return "", NewProtocolError(ErrorInvalidAuthorization, nil)
	}
	credential := value[len(authorizationPrefix):]
	if credential == "" {
		return "", NewProtocolError(ErrorInvalidAuthorization, nil)
	}
	for index := 0; index < len(credential); index++ {
		character := credential[index]
		if character < 0x21 || character > 0x7e || character == ',' {
			return "", NewProtocolError(ErrorInvalidAuthorization, errors.New("credential contains a forbidden character"))
		}
	}
	return credential, nil
}
