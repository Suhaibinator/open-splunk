package searchjobs

import "github.com/Suhaibinator/open-splunk/internal/orderedindex"

// jobListIndexNode is protected by Manager.mu for every mutation and traversal.
type jobListIndexNode = orderedindex.Node[*jobEntry]

func newJobListIndexNode(entry *jobEntry) *jobListIndexNode {
	return orderedindex.NewNode(entry, entry.generation)
}

func jobListIndexSize(node *jobListIndexNode) int {
	return orderedindex.Size(node)
}

func jobListIndexInsert(root, inserted *jobListIndexNode) *jobListIndexNode {
	return orderedindex.Insert(root, inserted, jobListEntriesComeBefore)
}

func jobListIndexRemove(root *jobListIndexNode, entry *jobEntry) *jobListIndexNode {
	return orderedindex.Remove(root, entry, jobListEntriesComeBefore)
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
	orderedindex.CollectBefore(
		root, before, result, limit,
		jobListEntryComesBeforeBoundary,
		retainedJobListSnapshot,
	)
}
