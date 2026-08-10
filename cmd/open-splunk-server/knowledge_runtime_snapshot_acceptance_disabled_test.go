//go:build !open_splunk_knowledge_runtime_acceptance || !open_splunk_knowledge_snapshot_acceptance

package main

import "testing"

func TestRuntimeKnowledgeResolverFailsClosedForWriterPublishedActiveObject(t *testing.T) {
	testRuntimeKnowledgeResolverFailsClosedForWriterPublishedActiveObject(t)
}
