// Package orderedindex implements the deterministic treap shared by bounded
// keyset-paginated indexes.
package orderedindex

// Node is one node in a deterministic treap. Callers retain ownership of the
// values and serialize mutations externally.
type Node[T any] struct {
	value    T
	priority uint64
	left     *Node[T]
	right    *Node[T]
	size     int
}

// NewNode returns a singleton node whose stable priority is derived from a
// caller-supplied unique generation.
func NewNode[T any](value T, generation uint64) *Node[T] {
	return &Node[T]{value: value, priority: priority(generation), size: 1}
}

// Size reports the number of values rooted at node.
func Size[T any](node *Node[T]) int {
	if node == nil {
		return 0
	}
	return node.size
}

// Value returns the value stored at node.
func (node *Node[T]) Value() T {
	return node.value
}

// Insert adds inserted according to comesBefore.
func Insert[T any](root, inserted *Node[T], comesBefore func(T, T) bool) *Node[T] {
	if root == nil {
		return inserted
	}
	if comesBefore(inserted.value, root.value) {
		root.left = Insert(root.left, inserted, comesBefore)
		if root.left.priority > root.priority {
			root = rotateRight(root)
		}
	} else {
		root.right = Insert(root.right, inserted, comesBefore)
		if root.right.priority > root.priority {
			root = rotateLeft(root)
		}
	}
	update(root)
	return root
}

// Remove deletes the node whose comparable value equals target.
func Remove[T comparable](root *Node[T], target T, comesBefore func(T, T) bool) *Node[T] {
	if root == nil {
		return nil
	}
	if root.value == target {
		return merge(root.left, root.right)
	}
	if comesBefore(target, root.value) {
		root.left = Remove(root.left, target, comesBefore)
	} else {
		root.right = Remove(root.right, target, comesBefore)
	}
	update(root)
	return root
}

// CollectBefore appends at most limit snapshots in reverse tree order whose
// keys are strictly before the optional boundary.
func CollectBefore[T, Boundary, Snapshot any](
	root *Node[T],
	before *Boundary,
	result *[]Snapshot,
	limit int,
	comesBeforeBoundary func(T, Boundary) bool,
	snapshot func(T) Snapshot,
) {
	if root == nil || len(*result) >= limit {
		return
	}
	if before != nil && !comesBeforeBoundary(root.value, *before) {
		CollectBefore(root.left, before, result, limit, comesBeforeBoundary, snapshot)
		return
	}
	CollectBefore(root.right, before, result, limit, comesBeforeBoundary, snapshot)
	if len(*result) >= limit {
		return
	}
	*result = append(*result, snapshot(root.value))
	CollectBefore(root.left, before, result, limit, comesBeforeBoundary, snapshot)
}

func update[T any](node *Node[T]) {
	node.size = 1 + Size(node.left) + Size(node.right)
}

func merge[T any](left, right *Node[T]) *Node[T] {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority > right.priority {
		left.right = merge(left.right, right)
		update(left)
		return left
	}
	right.left = merge(left, right.left)
	update(right)
	return right
}

func rotateLeft[T any](root *Node[T]) *Node[T] {
	pivot := root.right
	root.right = pivot.left
	pivot.left = root
	update(root)
	update(pivot)
	return pivot
}

func rotateRight[T any](root *Node[T]) *Node[T] {
	pivot := root.left
	root.left = pivot.right
	pivot.right = root
	update(root)
	update(pivot)
	return pivot
}

// priority is SplitMix64's bijective mixer. Unique generations yield unique,
// independent-looking priorities without mutable randomness.
func priority(generation uint64) uint64 {
	value := generation + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
