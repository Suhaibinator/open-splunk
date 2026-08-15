package spl_test

import (
	"errors"
	"fmt"
	"regexp"
)

func compatibilityDocumentRuleIDs(document []byte, prefix string) ([]string, error) {
	escapedPrefix := regexp.QuoteMeta(prefix)
	rulePattern := escapedPrefix + `-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}`
	headingPattern := regexp.MustCompile(`(?m)^### ` + "`" + `(` + rulePattern + `)` + "`" + ` — `)
	allPattern := regexp.MustCompile(rulePattern)

	headingMatches := headingPattern.FindAllSubmatch(document, -1)
	if len(headingMatches) == 0 {
		return nil, errors.New("contract has no rule headings")
	}
	headings := make([]string, 0, len(headingMatches))
	seen := make(map[string]struct{}, len(headingMatches))
	for _, match := range headingMatches {
		id := string(match[1])
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("contract duplicates rule heading %q", id)
		}
		seen[id] = struct{}{}
		headings = append(headings, id)
	}

	for _, match := range allPattern.FindAll(document, -1) {
		id := string(match)
		if _, exists := seen[id]; !exists {
			return nil, fmt.Errorf("contract mentions rule %q without a rule heading", id)
		}
	}
	return headings, nil
}
