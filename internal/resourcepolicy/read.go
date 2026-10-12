package resourcepolicy

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"slices"
	"sync"

	"gorm.io/gorm"
)

type Snapshot struct {
	Fingerprint          string
	SiteAdministrator    bool
	IndexNames           []string
	AvailableIndexNames  []string
	AppIDs               []string
	AppDefaultIndexNames map[string][]string
}
type Provider func(context.Context) (Snapshot, error)
type lazySnapshot struct {
	once     sync.Once
	provider Provider
	snapshot Snapshot
	err      error
}
type policyKey struct{}

func WithProvider(ctx context.Context, provider Provider) context.Context {
	return context.WithValue(ctx, policyKey{}, &lazySnapshot{provider: provider})
}
func FromContext(ctx context.Context) (Snapshot, bool, error) {
	if ctx == nil {
		return Snapshot{}, false, nil
	}
	lazy, ok := ctx.Value(policyKey{}).(*lazySnapshot)
	if !ok {
		return Snapshot{}, false, nil
	}
	lazy.once.Do(func() {
		if lazy.provider == nil {
			lazy.err = errors.New("source visibility unavailable")
			return
		}
		lazy.snapshot, lazy.err = lazy.provider(ctx)
	})
	return lazy.snapshot, true, lazy.err
}

// Apply filters materialized source authority before a metadata count or page.
// Table/column names are package constants, never request values.
func Apply(ctx context.Context, query *gorm.DB, table string) (*gorm.DB, error) {
	snapshot, present, err := FromContext(ctx)
	if err != nil {
		return nil, err
	}
	if !present || snapshot.SiteAdministrator {
		return query, nil
	}
	names, err := json.Marshal(append([]string{}, snapshot.IndexNames...))
	if err != nil {
		return nil, err
	}
	apps, err := json.Marshal(append([]string{}, snapshot.AppIDs...))
	if err != nil {
		return nil, err
	}
	prefix := "\"" + table + "\"."
	query = query.Where("("+prefix+"app_id = '' OR "+prefix+"app_id IN (SELECT jsonb_array_elements_text(?::jsonb)))", string(apps))
	// Wildcards are evaluated against the bounded current catalog for this org.
	// Escaping '_' keeps literal underscores distinct from SQL LIKE wildcards.
	predicate := `NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(` + prefix + `source_index_names) AS source(name) WHERE CASE WHEN source.name='kvstore_app_defaults_source' THEN EXISTS (SELECT 1 FROM app_default_indexes source_default JOIN indexes source_default_index ON source_default_index.index_id=source_default.index_id WHERE source_default.app_id=` + prefix + `app_id AND source_default_index.name NOT IN (SELECT jsonb_array_elements_text(?::jsonb))) WHEN position('*' IN source.name)>0 OR position(chr(63) IN source.name)>0 THEN EXISTS (SELECT 1 FROM indexes source_index JOIN os_index_organizations source_owner ON source_owner.index_id=source_index.index_id WHERE source_owner.organization_id=` + prefix + `organization_id AND source_index.name LIKE replace(replace(replace(source.name,'_',chr(92)||'_'),'*','%'),chr(63),'_') AND source_index.name NOT IN (SELECT jsonb_array_elements_text(?::jsonb))) ELSE source.name NOT IN (SELECT jsonb_array_elements_text(?::jsonb)) END)`
	return query.Where(predicate, string(names), string(names), string(names)), nil
}
func Fingerprint(ctx context.Context) (string, error) {
	snapshot, present, err := FromContext(ctx)
	if err != nil {
		return "", err
	}
	if !present {
		return "", nil
	}
	return snapshot.Fingerprint, nil
}
func AllowsSource(snapshot Snapshot, encoded []byte, appID string) bool {
	if snapshot.SiteAdministrator {
		return true
	}
	if appID != "" && !contains(snapshot.AppIDs, appID) {
		return false
	}
	var names []string
	if json.Unmarshal(encoded, &names) != nil {
		return false
	}
	for _, name := range names {
		if name == AppDefaultsSource {
			for _, index := range snapshot.AppDefaultIndexNames[appID] {
				if !contains(snapshot.IndexNames, index) {
					return false
				}
			}
			continue
		}
		if name == UnresolvedSource {
			return false
		}
		if contains(snapshot.IndexNames, name) {
			continue
		}
		wildcard := false
		for _, c := range name {
			wildcard = wildcard || c == '*' || c == '?'
		}
		if !wildcard {
			return false
		}
		for _, candidate := range snapshot.AvailableIndexNames {
			match, err := path.Match(name, candidate)
			if err != nil {
				return false
			}
			if match && !contains(snapshot.IndexNames, candidate) {
				return false
			}
		}
	}
	return true
}
func contains(values []string, value string) bool { return slices.Contains(values, value) }

// Filter preserves the GORM query shape while making provider failures stop
// reads instead of silently dropping the authorization predicate.
func Filter(query *gorm.DB, table string) *gorm.DB {
	filtered, err := Apply(query.Statement.Context, query, table)
	if err != nil {
		// AddError retains the joined error on the returned GORM query.
		_ = query.AddError(err)
		return query.Where("FALSE")
	}
	return filtered
}
