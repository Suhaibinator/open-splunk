package export

import (
	"context"

	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Journal co-commits admission metadata, its successful audit event, and an
// optional request receipt before work is enqueued. Update never creates jobs.
// Implementations preserve target identity after metadata leaves the live cache.
type Journal interface {
	Admit(context.Context, searchjobs.AccessScope, Job) error
	Update(context.Context, DurableJob) error
	Get(context.Context, searchjobs.AccessScope, string) (DurableJob, error)
	Restore(context.Context, int) ([]DurableJob, error)
}

// DurableJob stores only an owned artifact basename and content digest; paths
// and download grants never cross the persistence boundary.
type DurableJob struct {
	Access         searchjobs.AccessScope
	Job            Job
	ArtifactName   string
	ArtifactSHA256 []byte
	MetadataBytes  uint64
}

// IdempotentJournal atomically binds an admission to an authenticated actor's
// immutable client intent. Lookup always rehydrates currently visible metadata.
type IdempotentJournal interface {
	Journal
	Lookup(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (Job, bool, error)
	AdmitIdempotent(context.Context, searchjobs.AccessScope, Job, requestidempotency.Intent) error
}
