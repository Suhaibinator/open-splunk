// Package collectoradmission composes collector credential revalidation,
// accepted-token telemetry, administrator fleet state, and durable stream
// lease allocation into one immediate SQLite transaction. It also revalidates
// credentials and exact enabled durable leases for each accepted stream
// operation in one consistent read transaction.
package collectoradmission
