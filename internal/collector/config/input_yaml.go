package config

import (
	"errors"

	yaml "go.yaml.in/yaml/v3"
)

// UnmarshalYAML distinguishes an absent parser from explicit null. The callback
// form deliberately retains the parent decoder's KnownFields setting, including
// nested input fields; Node.Decode would create a new, non-strict decoder.
func (input *InputConfig) UnmarshalYAML(unmarshal func(any) error) error {
	var fields map[string]yaml.Node
	if err := unmarshal(&fields); err != nil {
		return err
	}
	if parser, present := fields["parser"]; present {
		node := &parser
		// Mapping decode above already applies YAML merge precedence. Resolve an
		// effective parser alias without traversing unrelated configuration nodes.
		for depth := 0; node != nil && node.Kind == yaml.AliasNode && depth < 64; depth++ {
			node = node.Alias
		}
		if node == nil || node.Kind != yaml.MappingNode {
			return errors.New("input parser must be a mapping; omit it when unused")
		}
	}
	type plainInput InputConfig
	var decoded plainInput
	if err := unmarshal(&decoded); err != nil {
		return err
	}
	*input = InputConfig(decoded)
	return nil
}
