package searchjobs

// jobListIndexNode is one node in a deterministic treap ordered by the
// immutable (CreatedAt, ID) list key. The splitmix priority derived from the
// globally unique admission generation makes mutation and keyset seek
// expected O(log N), including out-of-order concurrent admissions and bulk
// tombstone cleanup. Manager.mu protects every tree mutation and traversal.
type jobListIndexNode struct {
	entry    *jobEntry
	priority uint64
	left     *jobListIndexNode
	right    *jobListIndexNode
	size     int
}

func newJobListIndexNode(entry *jobEntry) *jobListIndexNode {
	return &jobListIndexNode{
		entry:    entry,
		priority: jobListIndexPriority(entry.generation),
		size:     1,
	}
}

func jobListIndexSize(node *jobListIndexNode) int {
	if node == nil {
		return 0
	}
	return node.size
}

func jobListIndexUpdate(node *jobListIndexNode) {
	node.size = 1 + jobListIndexSize(node.left) + jobListIndexSize(node.right)
}

func jobListIndexInsert(root, inserted *jobListIndexNode) *jobListIndexNode {
	if root == nil {
		return inserted
	}
	if jobListIndexComesBefore(inserted.entry.job, root.entry.job) {
		root.left = jobListIndexInsert(root.left, inserted)
		if root.left.priority > root.priority {
			root = jobListIndexRotateRight(root)
		}
	} else {
		root.right = jobListIndexInsert(root.right, inserted)
		if root.right.priority > root.priority {
			root = jobListIndexRotateLeft(root)
		}
	}
	jobListIndexUpdate(root)
	return root
}

func jobListIndexRemove(root *jobListIndexNode, entry *jobEntry) *jobListIndexNode {
	if root == nil {
		return nil
	}
	if root.entry == entry {
		return jobListIndexMerge(root.left, root.right)
	}
	if jobListIndexComesBefore(entry.job, root.entry.job) {
		root.left = jobListIndexRemove(root.left, entry)
	} else {
		root.right = jobListIndexRemove(root.right, entry)
	}
	jobListIndexUpdate(root)
	return root
}

func jobListIndexMerge(left, right *jobListIndexNode) *jobListIndexNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority > right.priority {
		left.right = jobListIndexMerge(left.right, right)
		jobListIndexUpdate(left)
		return left
	}
	right.left = jobListIndexMerge(left, right.left)
	jobListIndexUpdate(right)
	return right
}

func jobListIndexRotateLeft(root *jobListIndexNode) *jobListIndexNode {
	pivot := root.right
	root.right = pivot.left
	pivot.left = root
	jobListIndexUpdate(root)
	jobListIndexUpdate(pivot)
	return pivot
}

func jobListIndexRotateRight(root *jobListIndexNode) *jobListIndexNode {
	pivot := root.left
	root.left = pivot.right
	pivot.right = root
	jobListIndexUpdate(root)
	jobListIndexUpdate(pivot)
	return pivot
}

// jobListIndexCollectBefore appends at most limit entries in newest-first
// order whose key is strictly below before. Passing nil starts at the newest
// key. Boundary pruning makes each bounded page O(log N + page scan).
func jobListIndexCollectBefore(
	root *jobListIndexNode,
	before *jobListBoundary,
	result *[]retainedJobListEntry,
	limit int,
) {
	if root == nil || len(*result) >= limit {
		return
	}
	if before != nil && !jobListEntryComesBeforeBoundary(root.entry, *before) {
		jobListIndexCollectBefore(root.left, before, result, limit)
		return
	}
	jobListIndexCollectBefore(root.right, before, result, limit)
	if len(*result) >= limit {
		return
	}
	*result = append(*result, retainedJobListSnapshot(root.entry))
	jobListIndexCollectBefore(root.left, before, result, limit)
}

// jobListIndexPriority is SplitMix64's bijective mixer. Admission generations
// are unique, so priorities are unique and independent-looking without a
// mutable random source or attacker-controlled ordering.
func jobListIndexPriority(generation uint64) uint64 {
	value := generation + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
