package parserconfig

import (
	"errors"

	yaml "go.yaml.in/yaml/v3"
)

type optionPresence uint8

const (
	fieldsPresent optionPresence = 1 << iota
	patternPresent
	layoutPresent
	timezonePresent
)

// UnmarshalYAML distinguishes omitted options from explicit empty values.
// Node.Decode does not inherit KnownFields, so inspect all decoded keys.
func (o *Options) UnmarshalYAML(node *yaml.Node) error {
	var values map[string]yaml.Node
	if err := node.Decode(&values); err != nil {
		return err
	}
	var supplied optionPresence
	for key, value := range values {
		switch key {
		case "fields":
			supplied |= fieldsPresent
		case "pattern":
			supplied |= patternPresent
		case "timestamp_layout":
			supplied |= layoutPresent
		case "timezone":
			supplied |= timezonePresent
		default:
			return errors.New("unknown parser option")
		}
		resolved, err := optionNode(&value)
		if err != nil {
			return err
		}
		if key == "fields" {
			if resolved.Kind != yaml.MappingNode {
				return errors.New("parser fields must be a mapping")
			}
			var fields map[string]yaml.Node
			if err := resolved.Decode(&fields); err != nil {
				return err
			}
			if err := fieldMappingKeys(resolved, make(map[*yaml.Node]bool), 0); err != nil {
				return err
			}
			for _, field := range fields {
				if err := optionString(&field); err != nil {
					return err
				}
			}
		} else if err := optionString(resolved); err != nil {
			return err
		}
	}
	type plain Options
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	decoded.supplied = supplied
	*o = Options(decoded)
	return nil
}

func optionString(node *yaml.Node) error {
	node, err := optionNode(node)
	if err != nil {
		return err
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return errors.New("parser option values must be strings")
	}
	return nil
}

func optionNode(node *yaml.Node) (*yaml.Node, error) {
	for depth := 0; node != nil && depth < 64; depth++ {
		if node.Kind != yaml.AliasNode {
			return node, nil
		}
		node = node.Alias
	}
	return nil, errors.New("invalid or excessively nested parser option alias")
}

// The YAML decoder compares raw node kinds when checking map duplicates. An
// alias key and its anchored scalar can therefore name the same canonical
// role twice. Check resolved field keys, retaining normal merge precedence.
func fieldMappingKeys(node *yaml.Node, visited map[*yaml.Node]bool, depth int) error {
	if depth > 64 {
		return errors.New("parser field merges exceed maximum depth")
	}
	node, err := optionNode(node)
	if err != nil {
		return err
	}
	if visited[node] {
		return nil
	}
	visited[node] = true
	if node.Kind == yaml.SequenceNode {
		for _, child := range node.Content {
			if err := fieldMappingKeys(child, visited, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return errors.New("parser fields must be a mapping")
	}
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key, err := optionNode(node.Content[i])
		if err != nil {
			return err
		}
		if key.Kind != yaml.ScalarNode || (key.Tag != "!!str" && key.Tag != "!!merge") {
			return errors.New("parser field keys must be strings")
		}
		if seen[key.Value] {
			return errors.New("duplicate parser field key")
		}
		seen[key.Value] = true
		if key.Tag == "!!merge" {
			if err := fieldMappingKeys(node.Content[i+1], visited, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
