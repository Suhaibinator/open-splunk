package spl_test

import (
	"errors"
	"fmt"
	"regexp"
)

func compatibilityDocumentRuleIDs(document []byte) ([]string, error) {
	rulePattern := `SPL-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}`
	inventoryPattern := regexp.MustCompile(`(?m)^\| ` + "`" + `(` + rulePattern + `)` + "`" + ` \|`)
	allPattern := regexp.MustCompile(rulePattern)

	inventoryMatches := inventoryPattern.FindAllSubmatch(document, -1)
	if len(inventoryMatches) == 0 {
		return nil, errors.New("contract has no rule inventory")
	}
	result := make([]string, 0, len(inventoryMatches))
	seen := make(map[string]struct{}, len(inventoryMatches))
	for _, match := range inventoryMatches {
		id := string(match[1])
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("contract duplicates rule inventory entry %q", id)
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}

	for _, match := range allPattern.FindAll(document, -1) {
		id := string(match)
		if _, exists := seen[id]; !exists {
			return nil, fmt.Errorf("contract mentions rule %q without an inventory entry", id)
		}
	}
	return result, nil
}
