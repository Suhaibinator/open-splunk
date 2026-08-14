package plan

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const MaximumAutomaticLookupStages = 16

const automaticLookupAuthorityDomain = "open-splunk-automatic-lookup-group-v1\x00"

type queryAutomaticLookupAuthority struct {
	commitment [sha256.Size]byte
	set        bool
}

// AutomaticLookupSpec is trusted admission input for one automatic lookup.
// StableID is the immutable logical definition identity used as the secondary
// deterministic ordering key. Lookup carries only logical mappings; physical
// asset authority remains in the compiler's separately pinned resolution.
type AutomaticLookupSpec struct {
	StableID string
	Lookup   Lookup
	Selector knowledgeprogram.Selector
}

// AutomaticLookup is one detached view of opaque generated group authority.
type AutomaticLookup struct {
	stableID string
	lookup   Lookup
	selector knowledgeprogram.Selector
}

func (lookup AutomaticLookup) StableID() string { return strings.Clone(lookup.stableID) }

func (lookup AutomaticLookup) LogicalLookup() Lookup {
	return cloneLogicalLookup(lookup.lookup)
}

func (lookup AutomaticLookup) Selector() knowledgeprogram.Selector {
	return lookup.selector
}

// AutomaticLookupGroup is the one generated, parallel-match stage. Its
// entries are opaque so authored SPL and direct struct literals cannot mint a
// partial automatic marker or selector authority.
type AutomaticLookupGroup struct {
	entries    []AutomaticLookup
	commitment [sha256.Size]byte
}

func (*AutomaticLookupGroup) operator()              {}
func (*AutomaticLookupGroup) LogicalName() string    { return "AutomaticLookupGroup" }
func (*AutomaticLookupGroup) SourceRange() spl.Range { return spl.Range{} }

func (group *AutomaticLookupGroup) Lookups() []AutomaticLookup {
	if group == nil {
		return nil
	}
	result := make([]AutomaticLookup, len(group.entries))
	for index, entry := range group.entries {
		result[index] = AutomaticLookup{
			stableID: strings.Clone(entry.stableID),
			lookup:   cloneLogicalLookup(entry.lookup),
			selector: entry.selector,
		}
	}
	return result
}

func (group *AutomaticLookupGroup) AuthorityDigest() ([sha256.Size]byte, bool) {
	if !validAutomaticLookupGroup(group) {
		return [sha256.Size]byte{}, false
	}
	return group.commitment, true
}

// InjectAutomaticLookupGroup inserts exactly one generated group after the
// complete admitted Tier-1 knowledge prefix and before every authored
// operator, including the base-search Filter. A deliberate knowledge marker
// is required even when that program is empty, making placement unambiguous.
func InjectAutomaticLookupGroup(
	query *Query,
	specs []AutomaticLookupSpec,
) (*Query, error) {
	if query == nil || len(specs) == 0 || len(specs) > MaximumAutomaticLookupStages {
		return nil, errors.New("inject automatic lookups: query or stage count is invalid")
	}
	if err := ValidateKnowledgePreludeIntegrity(query); err != nil {
		return nil, fmt.Errorf("inject automatic lookups: %w", err)
	}
	program, present := query.KnowledgePrelude()
	if !present {
		return nil, errors.New("inject automatic lookups: admitted knowledge marker is required")
	}
	if query.automaticLookups.set || query.automaticLookups.commitment != ([sha256.Size]byte{}) {
		return nil, errors.New("inject automatic lookups: query already contains automatic authority")
	}
	for _, operator := range query.Operators {
		if _, automatic := operator.(*AutomaticLookupGroup); automatic {
			return nil, errors.New("inject automatic lookups: query already contains an automatic group")
		}
	}

	entries := make([]AutomaticLookup, len(specs))
	for index, spec := range specs {
		entry := AutomaticLookup{
			stableID: strings.Clone(spec.StableID),
			lookup:   cloneLogicalLookup(spec.Lookup),
			selector: spec.Selector,
		}
		if !validAutomaticLookup(entry) {
			return nil, fmt.Errorf("inject automatic lookups: stage %d is invalid", index)
		}
		if index > 0 && compareAutomaticLookup(entries[index-1], entry) >= 0 {
			return nil, errors.New(
				"inject automatic lookups: stages are not in unique normalized-name/stable-ID order",
			)
		}
		entries[index] = entry
	}
	commitment, ok := automaticLookupCommitment(entries)
	if !ok {
		return nil, errors.New("inject automatic lookups: authority commitment is invalid")
	}
	group := &AutomaticLookupGroup{entries: entries, commitment: commitment}
	prefix, err := knowledgePreludeOperators(program)
	if err != nil {
		return nil, fmt.Errorf("inject automatic lookups: %w", err)
	}
	insertAt := 1 + len(prefix)
	if insertAt > len(query.Operators) {
		return nil, errors.New("inject automatic lookups: knowledge prefix is incomplete")
	}
	result := cloneQueryHeader(query)
	result.Operators = make([]Operator, 0, len(query.Operators)+1)
	result.Operators = append(result.Operators, query.Operators[:insertAt]...)
	result.Operators = append(result.Operators, group)
	result.Operators = append(result.Operators, query.Operators[insertAt:]...)
	result.automaticLookups = queryAutomaticLookupAuthority{
		commitment: commitment,
		set:        true,
	}
	if err := ValidateAutomaticLookupIntegrity(result); err != nil {
		return nil, fmt.Errorf("inject automatic lookups: %w", err)
	}
	return result, nil
}

// ValidateAutomaticLookupIntegrity rejects forged, moved, duplicated, or
// partially marked automatic groups. Legacy queries may omit both marker and
// group.
func ValidateAutomaticLookupIntegrity(query *Query) error {
	if query == nil {
		return errors.New("validate automatic lookups: query is nil")
	}
	markerPresent := query.automaticLookups.set
	commitmentPresent := query.automaticLookups.commitment != ([sha256.Size]byte{})
	if markerPresent != commitmentPresent {
		return errors.New("validate automatic lookups: marker is malformed")
	}
	groupIndex := -1
	var group *AutomaticLookupGroup
	for index, operator := range query.Operators {
		candidate, ok := operator.(*AutomaticLookupGroup)
		if !ok {
			continue
		}
		if groupIndex >= 0 {
			return errors.New("validate automatic lookups: group is duplicated")
		}
		groupIndex, group = index, candidate
	}
	if !markerPresent {
		if groupIndex >= 0 {
			return errors.New("validate automatic lookups: unmarked group is present")
		}
		return nil
	}
	if err := ValidateKnowledgePreludeIntegrity(query); err != nil {
		return fmt.Errorf("validate automatic lookups: %w", err)
	}
	program, present := query.KnowledgePrelude()
	if !present || group == nil || !validAutomaticLookupGroup(group) ||
		group.commitment != query.automaticLookups.commitment {
		return errors.New("validate automatic lookups: authority is invalid")
	}
	prefix, err := knowledgePreludeOperators(program)
	if err != nil {
		return fmt.Errorf("validate automatic lookups: %w", err)
	}
	if groupIndex != 1+len(prefix) {
		return errors.New("validate automatic lookups: group is outside the generated prefix boundary")
	}
	return nil
}

func validAutomaticLookupGroup(group *AutomaticLookupGroup) bool {
	if group == nil || len(group.entries) == 0 ||
		len(group.entries) > MaximumAutomaticLookupStages {
		return false
	}
	for index, entry := range group.entries {
		if !validAutomaticLookup(entry) ||
			index > 0 && compareAutomaticLookup(group.entries[index-1], entry) >= 0 {
			return false
		}
	}
	commitment, ok := automaticLookupCommitment(group.entries)
	return ok && commitment == group.commitment
}

func validAutomaticLookup(entry AutomaticLookup) bool {
	if entry.stableID == "" || len(entry.stableID) > 255 ||
		!utf8.ValidString(entry.stableID) || strings.IndexByte(entry.stableID, 0) >= 0 ||
		!validLookupContract(&entry.lookup) || len(entry.selector.CanonicalBytes()) == 0 {
		return false
	}
	constrained := false
	for dimension := knowledge.DimensionIndex; dimension <= knowledge.DimensionSourcetype; dimension++ {
		_, ok := entry.selector.RuntimeProgram(dimension)
		constrained = constrained || ok
	}
	return entry.selector.IsUnrestricted() != constrained
}

func compareAutomaticLookup(left, right AutomaticLookup) int {
	if compared := strings.Compare(left.lookup.DefinitionName, right.lookup.DefinitionName); compared != 0 {
		return compared
	}
	return strings.Compare(left.stableID, right.stableID)
}

func automaticLookupCommitment(entries []AutomaticLookup) ([sha256.Size]byte, bool) {
	if len(entries) == 0 || len(entries) > MaximumAutomaticLookupStages {
		return [sha256.Size]byte{}, false
	}
	digest := sha256.New()
	writeAutomaticLookupPart := func(value string) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(value))
	}
	writeAutomaticLookupPart(automaticLookupAuthorityDomain)
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(entries)))
	_, _ = digest.Write(count[:])
	for _, entry := range entries {
		if !validAutomaticLookup(entry) {
			return [sha256.Size]byte{}, false
		}
		writeAutomaticLookupPart(entry.stableID)
		writeAutomaticLookupPart(entry.lookup.DefinitionName)
		binary.BigEndian.PutUint64(count[:], uint64(entry.lookup.WriteMode))
		_, _ = digest.Write(count[:])
		binary.BigEndian.PutUint64(count[:], uint64(len(entry.lookup.Keys)))
		_, _ = digest.Write(count[:])
		for _, key := range entry.lookup.Keys {
			writeAutomaticLookupPart(key.LookupField)
			writeAutomaticLookupPart(key.EventField.Name)
		}
		binary.BigEndian.PutUint64(count[:], uint64(len(entry.lookup.Outputs)))
		_, _ = digest.Write(count[:])
		for _, output := range entry.lookup.Outputs {
			writeAutomaticLookupPart(output.LookupField)
			writeAutomaticLookupPart(output.EventField.Name)
		}
		selector := entry.selector.CanonicalBytes()
		binary.BigEndian.PutUint64(count[:], uint64(len(selector)))
		_, _ = digest.Write(count[:])
		_, _ = digest.Write(selector)
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result, true
}

func cloneLogicalLookup(lookup Lookup) Lookup {
	result := lookup
	result.DefinitionName = strings.Clone(lookup.DefinitionName)
	result.Keys = slices.Clone(lookup.Keys)
	result.Outputs = slices.Clone(lookup.Outputs)
	for index := range result.Keys {
		result.Keys[index].LookupField = strings.Clone(result.Keys[index].LookupField)
		result.Keys[index].EventField.Name = strings.Clone(result.Keys[index].EventField.Name)
	}
	for index := range result.Outputs {
		result.Outputs[index].LookupField = strings.Clone(result.Outputs[index].LookupField)
		result.Outputs[index].EventField.Name = strings.Clone(result.Outputs[index].EventField.Name)
	}
	return result
}

func (analyzer *queryAnalyzer) visitAutomaticLookupGroup(
	group *AutomaticLookupGroup,
	depth int,
) error {
	if !validAutomaticLookupGroup(group) {
		return errors.New("analyze logical query: automatic lookup group is invalid")
	}
	for _, entry := range group.entries {
		if err := analyzer.visitKnowledgeSelector(entry.selector, depth+1); err != nil {
			return err
		}
		lookup := entry.lookup
		if err := analyzer.visitLookup(&lookup, depth+1); err != nil {
			return err
		}
	}
	return nil
}
