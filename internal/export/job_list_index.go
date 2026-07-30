package export

// exportListIndexNode is one node in a deterministic treap ordered by the
// immutable (CreatedAt, ID) list key. The splitmix priority derived from the
// globally unique admission generation makes insertion and removal expected
// O(log N), including out-of-order clocks and reused IDs. Manager.mu protects
// every tree mutation and traversal.
type exportListIndexNode struct {
	entry    *jobEntry
	priority uint64
	left     *exportListIndexNode
	right    *exportListIndexNode
	size     int
}

func newExportListIndexNode(entry *jobEntry) *exportListIndexNode {
	return &exportListIndexNode{
		entry:    entry,
		priority: exportListIndexPriority(entry.generation),
		size:     1,
	}
}

func exportListIndexSize(node *exportListIndexNode) int {
	if node == nil {
		return 0
	}
	return node.size
}

func exportListIndexUpdate(node *exportListIndexNode) {
	node.size = 1 + exportListIndexSize(node.left) + exportListIndexSize(node.right)
}

func exportListIndexInsert(root, inserted *exportListIndexNode) *exportListIndexNode {
	if root == nil {
		return inserted
	}
	if exportListEntriesComeBefore(inserted.entry, root.entry) {
		root.left = exportListIndexInsert(root.left, inserted)
		if root.left.priority > root.priority {
			root = exportListIndexRotateRight(root)
		}
	} else {
		root.right = exportListIndexInsert(root.right, inserted)
		if root.right.priority > root.priority {
			root = exportListIndexRotateLeft(root)
		}
	}
	exportListIndexUpdate(root)
	return root
}

func exportListIndexRemove(root *exportListIndexNode, entry *jobEntry) *exportListIndexNode {
	if root == nil {
		return nil
	}
	if root.entry == entry {
		return exportListIndexMerge(root.left, root.right)
	}
	if exportListEntriesComeBefore(entry, root.entry) {
		root.left = exportListIndexRemove(root.left, entry)
	} else {
		root.right = exportListIndexRemove(root.right, entry)
	}
	exportListIndexUpdate(root)
	return root
}

func exportListIndexMerge(left, right *exportListIndexNode) *exportListIndexNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority > right.priority {
		left.right = exportListIndexMerge(left.right, right)
		exportListIndexUpdate(left)
		return left
	}
	right.left = exportListIndexMerge(left, right.left)
	exportListIndexUpdate(right)
	return right
}

func exportListIndexRotateLeft(root *exportListIndexNode) *exportListIndexNode {
	pivot := root.right
	root.right = pivot.left
	pivot.left = root
	exportListIndexUpdate(root)
	exportListIndexUpdate(pivot)
	return pivot
}

func exportListIndexRotateRight(root *exportListIndexNode) *exportListIndexNode {
	pivot := root.left
	root.left = pivot.right
	pivot.right = root
	exportListIndexUpdate(root)
	exportListIndexUpdate(pivot)
	return pivot
}

// exportListIndexCollectBefore appends at most limit entries in newest-first
// order whose key is strictly below before. Passing nil starts at the newest
// key. Boundary pruning makes each bounded page O(log N + page scan).
func exportListIndexCollectBefore(
	root *exportListIndexNode,
	before *exportListBoundary,
	result *[]retainedExportListEntry,
	limit int,
) {
	if root == nil || len(*result) >= limit {
		return
	}
	if before != nil && !exportListEntryComesBeforeBoundary(root.entry, *before) {
		exportListIndexCollectBefore(root.left, before, result, limit)
		return
	}
	exportListIndexCollectBefore(root.right, before, result, limit)
	if len(*result) >= limit {
		return
	}
	*result = append(*result, retainedExportListSnapshot(root.entry))
	exportListIndexCollectBefore(root.left, before, result, limit)
}

// exportListIndexPriority is SplitMix64's bijective mixer. Admission
// generations are unique, so priorities are unique and independent-looking
// without a mutable random source or attacker-controlled ordering.
func exportListIndexPriority(generation uint64) uint64 {
	value := generation + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
