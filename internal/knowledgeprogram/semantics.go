package knowledgeprogram

import (
	"fmt"
	"sort"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
)

type semanticObjectKey struct {
	id      string
	version uint64
}

type semanticEdgeKey struct {
	source semanticObjectKey
	target semanticObjectKey
	role   opensplunkv1.KnowledgeDependencyRole
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
	dependencies []*opensplunkv1.KnowledgeObjectDependency,
) error {
	objects, err := semanticObjects(state)
	if err != nil {
		return err
	}
	if err := validateParallelSemantics(objects); err != nil {
		return err
	}

	expected, err := deriveSemanticEdges(objects)
	if err != nil {
		return err
	}
	if len(expected) > MaximumDependencies {
		return fmt.Errorf(
			"%w: derived dependencies exceed %d",
			ErrResourceLimit,
			MaximumDependencies,
		)
	}

	submitted := make(map[semanticEdgeKey]*opensplunkv1.KnowledgeObjectDependency, len(dependencies))
	for _, dependency := range dependencies {
		key := semanticEdgeKey{
			source: semanticReferenceKey(dependency.GetSource()),
			target: semanticReferenceKey(dependency.GetTarget().GetObject()),
			role:   dependency.GetRole(),
		}
		submitted[key] = dependency
	}
	if len(submitted) != len(expected) {
		return fmt.Errorf(
			"%w: submitted dependencies do not equal derived FIELD_INPUT edges",
			ErrInvalidProgram,
		)
	}
	for edge := range expected {
		if submitted[edge] == nil {
			return fmt.Errorf(
				"%w: a derived FIELD_INPUT edge is missing",
				ErrInvalidProgram,
			)
		}
	}

	depths, err := semanticDepths(objects, expected)
	if err != nil {
		return err
	}
	for index, dependency := range dependencies {
		source := semanticReferenceKey(dependency.GetSource())
		if dependency.GetTopologicalDepth() != depths[source] {
			return fmt.Errorf(
				"%w: dependency %d topological depth disagrees",
				ErrInvalidProgram,
				index,
			)
		}
		if index > 0 && !dependencyAfter(dependencies[index-1], dependency) {
			return fmt.Errorf(
				"%w: dependency %d is not in canonical order",
				ErrInvalidProgram,
				index,
			)
		}
	}
	return nil
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
		inputs:   canonicalSemanticFields(inputs),
		outputs:  canonicalSemanticFields(outputs),
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
			if semanticFieldsIntersect(left.outputs, right.outputs) {
				return fmt.Errorf(
					"%w: same-stage objects %q and %q may write the same destination",
					ErrInvalidProgram,
					left.origin.objectID,
					right.origin.objectID,
				)
			}
			if semanticFieldsIntersect(left.outputs, right.inputs) ||
				semanticFieldsIntersect(right.outputs, left.inputs) {
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
				!semanticFieldsIntersect(source.inputs, target.outputs) ||
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
				role:   opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
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
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return target.sharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL ||
			target.sharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_APP && target.appID == source.appID ||
			target.sharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE &&
				target.appID == source.appID && target.ownerID == source.ownerID
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return target.sharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL ||
			target.sharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_APP && target.appID == source.appID
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return target.sharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
	default:
		return false
	}
}

func semanticReferenceKey(reference *opensplunkv1.KnowledgeObjectVersionReference) semanticObjectKey {
	return semanticObjectKey{
		id:      reference.GetKnowledgeObjectId(),
		version: reference.GetVersion(),
	}
}

func canonicalSemanticFields(fields []string) []string {
	result := append([]string(nil), fields...)
	sort.Strings(result)
	write := 0
	for _, field := range result {
		if field == "" || write > 0 && result[write-1] == field {
			continue
		}
		result[write] = field
		write++
	}
	return result[:write:write]
}

func semanticFieldsIntersect(left, right []string) bool {
	for leftIndex, rightIndex := 0, 0; leftIndex < len(left) && rightIndex < len(right); {
		switch {
		case left[leftIndex] < right[rightIndex]:
			leftIndex++
		case left[leftIndex] > right[rightIndex]:
			rightIndex++
		default:
			return true
		}
	}
	return false
}
