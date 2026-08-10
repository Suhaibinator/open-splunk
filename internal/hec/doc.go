// Package hec implements the bounded, dependency-light wire contract for the
// Open Splunk HTTP Event Collector compatibility surface.
//
// The package deliberately stops at protocol adaptation. Authentication
// lookup, token lifecycle, ingestion policy, durable admission, ClickHouse
// reconciliation, and HTTP route registration belong to their owning
// packages. Keeping those concerns out of this package lets every HEC handler
// share one exact parser and response model without creating a second
// ingestion authority.
package hec
