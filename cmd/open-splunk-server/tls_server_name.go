package main

import (
	"net"
	"strings"
	"unicode"
	"unicode/utf8"
)

func validExplicitTLSServerName(serverName string) bool {
	if !utf8.ValidString(serverName) || len(serverName) == 0 ||
		len(serverName) > 253 ||
		strings.ContainsFunc(serverName, unicode.IsControl) ||
		strings.ContainsFunc(serverName, unicode.IsSpace) {
		return false
	}
	if net.ParseIP(serverName) != nil {
		return true
	}
	labels := strings.SplitSeq(serverName, ".")
	for label := range labels {
		if len(label) == 0 || len(label) > 63 ||
			!isASCIIAlphaNumeric(label[0]) ||
			!isASCIIAlphaNumeric(label[len(label)-1]) {
			return false
		}
		for index := 1; index < len(label)-1; index++ {
			character := label[index]
			if !isASCIIAlphaNumeric(character) && character != '-' {
				return false
			}
		}
	}
	return true
}

func isASCIIAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}
