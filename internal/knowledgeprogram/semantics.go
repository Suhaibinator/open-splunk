package knowledgeprogram

import (
	"bytes"
	"fmt"
	"sort"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"google.golang.org/protobuf/proto"
)

type semanticObjectKey struct {
	id      string
	version uint64
}

type semanticEdgeKey struct {
	source semanticObjectKey
	target semanticObjectKey
	role   opensplunk.KnowledgeDependencyRole
}

type semanticObject struct {
	key       semanticObjectKey
	origin    Origin
	selector  *knowledge.Selector
	inputs    []string
	outputs   []string
	stageRank uint8
}

// validateProgramSemantics independently re-derives the executable field
// graph from the typed immutable operations. The submitted dependency list is
// accepted only when it is the exact canonical FIELD_INPUT closure of that
// graph, including its source depths.
func validateProgramSemantics(
	state *programState,
	dependencies []*opensplunk.KnowledgeObjectDependency,
) error {
	expected, err := deriveCanonicalDependencies(state)
	if err != nil {
		return err
	}
	if len(dependencies) != len(expected) {
		return fmt.Errorf(
			"%w: submitted dependencies do not equal derived FIELD_INPUT edges",
			ErrInvalidProgram,
		)
	}
	for index := range expected {
		if !proto.Equal(dependencies[index], expected[index]) {
			return fmt.Errorf(
				"%w: submitted dependency %d disagrees with canonical derivation",
				ErrInvalidProgram,
				index,
			)
		}
	}
	return nil
}

func deriveCanonicalDependencies(
	state *programState,
) ([]*opensplunk.KnowledgeObjectDependency, error) {
	objects, err := semanticObjects(state)
	if err != nil {
		return nil, err
	}
	if err := validateParallelSemantics(objects); err != nil {
		return nil, err
	}

	edges, err := deriveSemanticEdges(objects)
	if err != nil {
		return nil, err
	}
	if len(edges) > MaximumDependencies {
		return nil, fmt.Errorf(
			"%w: derived dependencies exceed %d",
			ErrResourceLimit,
			MaximumDependencies,
		)
	}
	depths, err := semanticDepths(objects, edges)
	if err != nil {
		return nil, err
	}
	byKey := make(map[semanticObjectKey]semanticObject, len(objects))
	for _, object := range objects {
		byKey[object.key] = object
	}
	dependencies := make([]*opensplunk.KnowledgeObjectDependency, 0, len(edges))
	for edge := range edges {
		source, sourceFound := byKey[edge.source]
		target, targetFound := byKey[edge.target]
		if !sourceFound || !targetFound {
			return nil, fmt.Errorf("%w: derived dependency endpoint is absent", ErrInvalidProgram)
		}
		dependencies = append(dependencies, &opensplunk.KnowledgeObjectDependency{
			Source: semanticVersionReference(source),
			Target: &opensplunk.KnowledgeDependencyTarget{
				Target: &opensplunk.KnowledgeDependencyTarget_Object{
					Object: semanticVersionReference(target),
				},
			},
			Role:             edge.role,
			SourceStage:      source.origin.stage,
			TargetStage:      target.origin.stage,
			TopologicalDepth: depths[source.key],
		})
	}
	sort.Slice(dependencies, func(left, right int) bool {
		return dependencyAfter(dependencies[left], dependencies[right])
	})
	for index := range dependencies {
		dependencies[index].CanonicalOrdinal = uint32(index)
	}
	return dependencies, nil
}

func semanticVersionReference(
	object semanticObject,
) *opensplunk.KnowledgeObjectVersionReference {
	return &opensplunk.KnowledgeObjectVersionReference{
		KnowledgeObjectId: object.origin.objectID,
		Version:           object.origin.version,
		DefinitionSha256:  bytes.Clone(object.origin.definitionDigest[:]),
	}
}

func semanticObjects(state *programState) ([]semanticObject, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: semantic program state is nil", ErrInvalidProgram)
	}
	objects := make([]semanticObject, 0, state.objectCount)
	for _, operation := range state.regex {
		outputs := make([]string, len(operation.captures))
		for index, capture := range operation.captures {
			outputs[index] = capture.name
		}
		objects = append(objects, newSemanticObject(
			operation.origin,
			operation.selector,
			[]string{operation.input},
			outputs,
		))
	}
	for _, operation := range state.json {
		objects = append(objects, newSemanticObject(
			operation.origin,
			operation.selector,
			[]string{operation.input},
			[]string{operation.output},
		))
	}
	for _, operation := range state.aliases {
		objects = append(objects, newSemanticObject(
			operation.origin,
			operation.selector,
			[]string{operation.source},
			[]string{operation.destination},
		))
	}
	for _, operation := range state.calculated {
		objects = append(objects, newSemanticObject(
			operation.origin,
			operation.selector,
			operation.inputFields,
			[]string{operation.destination},
		))
	}
	if len(objects) != int(state.objectCount) {
		return nil, fmt.Errorf("%w: semantic object inventory disagrees", ErrInvalidProgram)
	}
	sort.Slice(objects, func(left, right int) bool {
		return objects[left].origin.resolutionOrdinal < objects[right].origin.resolutionOrdinal
	})
	for index := range objects {
		if objects[index].origin.resolutionOrdinal != uint32(index) ||
			objects[index].selector == nil ||
			len(objects[index].outputs) == 0 {
			return nil, fmt.Errorf("%w: semantic object %d is invalid", ErrInvalidProgram, index)
		}
		_, stageRank, err := stageForType(objects[index].origin.objectType)
		if err != nil || objects[index].origin.stage == 0 {
			return nil, fmt.Errorf("%w: semantic object %d stage is invalid", ErrInvalidProgram, index)
		}
		objects[index].stageRank = stageRank
	}
	return objects, nil
}

func newSemanticObject(
	origin Origin,
	selector Selector,
	inputs []string,
	outputs []string,
) semanticObject {
	return semanticObject{
		key: semanticObjectKey{
			id:      origin.objectID,
			version: origin.version,
		},
		origin:   origin,
		selector: selector.compiled,
		inputs:   knowledge.CanonicalFields(inputs),
		outputs:  knowledge.CanonicalFields(outputs),
	}
}

func validateParallelSemantics(objects []semanticObject) error {
	for leftIndex := range objects {
		left := &objects[leftIndex]
		for rightIndex := leftIndex + 1; rightIndex < len(objects); rightIndex++ {
			right := &objects[rightIndex]
			if left.stageRank != right.stageRank {
				continue
			}
			if knowledge.SelectorsProvablyDisjoint(left.selector, right.selector) {
				continue
			}
			if knowledge.FieldsIntersect(left.outputs, right.outputs) {
				return fmt.Errorf(
					"%w: same-stage objects %q and %q may write the same destination",
					ErrInvalidProgram,
					left.origin.objectID,
					right.origin.objectID,
				)
			}
			if knowledge.FieldsIntersect(left.outputs, right.inputs) ||
				knowledge.FieldsIntersect(right.outputs, left.inputs) {
				return fmt.Errorf(
					"%w: same-stage objects %q and %q form a possible data dependency",
					ErrInvalidProgram,
					left.origin.objectID,
					right.origin.objectID,
				)
			}
		}
	}
	return nil
}

func deriveSemanticEdges(objects []semanticObject) (map[semanticEdgeKey]struct{}, error) {
	edges := make(map[semanticEdgeKey]struct{})
	for sourceIndex := range objects {
		source := &objects[sourceIndex]
		for targetIndex := range objects {
			target := &objects[targetIndex]
			if source.key == target.key || source.stageRank <= target.stageRank ||
				!knowledge.FieldsIntersect(source.inputs, target.outputs) ||
				knowledge.SelectorsProvablyDisjoint(source.selector, target.selector) {
				continue
			}
			if !semanticDependencyScopeAllows(source.origin, target.origin) {
				return nil, fmt.Errorf(
					"%w: derived dependency violates sharing authority",
					ErrInvalidProgram,
				)
			}
			if !knowledge.SelectorImplies(source.selector, target.selector) {
				return nil, fmt.Errorf(
					"%w: derived dependency selector implication is unproven",
					ErrInvalidProgram,
				)
			}
			edges[semanticEdgeKey{
				source: source.key,
				target: target.key,
				role:   opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			}] = struct{}{}
		}
	}
	return edges, nil
}

func semanticDepths(
	objects []semanticObject,
	edges map[semanticEdgeKey]struct{},
) (map[semanticObjectKey]uint32, error) {
	adjacency := make(map[semanticObjectKey][]semanticObjectKey, len(objects))
	for edge := range edges {
		adjacency[edge.source] = append(adjacency[edge.source], edge.target)
	}
	depths := make(map[semanticObjectKey]uint32, len(objects))
	states := make(map[semanticObjectKey]uint8, len(objects))
	var depthOf func(semanticObjectKey) (uint32, error)
	depthOf = func(key semanticObjectKey) (uint32, error) {
		switch states[key] {
		case 1:
			return 0, fmt.Errorf("%w: dependency graph contains a cycle", ErrInvalidProgram)
		case 2:
			return depths[key], nil
		}
		states[key] = 1
		var depth uint32
		for _, target := range adjacency[key] {
			targetDepth, err := depthOf(target)
			if err != nil {
				return 0, err
			}
			if targetDepth >= MaximumDependencyDepth {
				return 0, fmt.Errorf(
					"%w: dependency depth exceeds %d",
					ErrResourceLimit,
					MaximumDependencyDepth,
				)
			}
			depth = max(depth, targetDepth+1)
		}
		states[key] = 2
		depths[key] = depth
		return depth, nil
	}
	for _, object := range objects {
		if _, err := depthOf(object.key); err != nil {
			return nil, err
		}
	}
	return depths, nil
}

func semanticDependencyScopeAllows(source, target Origin) bool {
	switch source.sharingScope {
	case opensplunk.SharingScope_SHARING_SCOPE_PRIVATE:
		return target.sharingScope == opensplunk.SharingScope_SHARING_SCOPE_GLOBAL ||
			target.sharingScope == opensplunk.SharingScope_SHARING_SCOPE_APP && target.appID == source.appID ||
			target.sharingScope == opensplunk.SharingScope_SHARING_SCOPE_PRIVATE &&
				target.appID == source.appID && target.ownerID == source.ownerID
	case opensplunk.SharingScope_SHARING_SCOPE_APP:
		return target.sharingScope == opensplunk.SharingScope_SHARING_SCOPE_GLOBAL ||
			target.sharingScope == opensplunk.SharingScope_SHARING_SCOPE_APP && target.appID == source.appID
	case opensplunk.SharingScope_SHARING_SCOPE_GLOBAL:
		return target.sharingScope == opensplunk.SharingScope_SHARING_SCOPE_GLOBAL
	default:
		return false
	}
}
