package export

import "github.com/Suhaibinator/open-splunk/internal/orderedindex"

// exportListIndexNode is protected by Manager.mu for every mutation and traversal.
type exportListIndexNode = orderedindex.Node[*jobEntry]

func newExportListIndexNode(entry *jobEntry) *exportListIndexNode {
	return orderedindex.NewNode(entry, entry.generation)
}

func exportListIndexSize(node *exportListIndexNode) int {
	return orderedindex.Size(node)
}

func exportListIndexInsert(root, inserted *exportListIndexNode) *exportListIndexNode {
	return orderedindex.Insert(root, inserted, exportListEntriesComeBefore)
}

func exportListIndexRemove(root *exportListIndexNode, entry *jobEntry) *exportListIndexNode {
	return orderedindex.Remove(root, entry, exportListEntriesComeBefore)
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
	orderedindex.CollectBefore(
		root, before, result, limit,
		exportListEntryComesBeforeBoundary,
		retainedExportListSnapshot,
	)
}
