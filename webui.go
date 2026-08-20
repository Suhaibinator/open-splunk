// Package opensplunk exposes application assets shared by the server command.
package opensplunk

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/buildassets"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

// embeddedReleaseFiles contains the output of the root Next.js static export
// and the source contracts whose digests are bound by its build manifest.
// The supported release build regenerates all of them before compiling Go.
//
//go:embed all:out all:proto migrations/sqlite/*.sql migrations/clickhouse/*.sql
var embeddedReleaseFiles embed.FS

var embeddedReleaseCache struct {
	once    sync.Once
	release Release
	err     error
}

// Release is the verified, target-independent application payload embedded in
// an open-splunk-server binary.
type Release struct {
	WebUI    fs.FS
	Metadata ReleaseMetadata
}

// ComponentMetadata identifies one canonically hashed embedded source tree.
type ComponentMetadata struct {
	SHA256    string
	FileCount uint32
	ByteCount uint64
}

// MigrationMetadata adds the latest contiguous schema version to a component.
type MigrationMetadata struct {
	ComponentMetadata
	LatestVersion uint32
}

// ReleaseMetadata is the stable root-package view of the internal asset
// manifest. The per-file verification allowlist remains an implementation
// detail so callers cannot mutate or depend on it.
type ReleaseMetadata struct {
	FormatVersion        uint32
	SourceRevision       string
	UIBuildID            string
	UI                   ComponentMetadata
	ProtobufSchema       ComponentMetadata
	SQLiteMigrations     MigrationMetadata
	ClickHouseMigrations MigrationMetadata
}

// EmbeddedRelease verifies every embedded byte before exposing the UI or its
// build identity. Missing, stale, tampered, or extra generated files fail
// closed during server startup.
func EmbeddedRelease() (Release, error) {
	embeddedReleaseCache.once.Do(func() {
		identity, err := buildinfo.Current()
		if err != nil {
			embeddedReleaseCache.err = err
			return
		}
		embeddedReleaseCache.release, embeddedReleaseCache.err = loadRelease(embeddedReleaseFiles, identity)
	})
	return embeddedReleaseCache.release, embeddedReleaseCache.err
}

// WebUI returns the embedded Next.js export rooted at its public directory.
func WebUI() (fs.FS, error) {
	release, err := EmbeddedRelease()
	if err != nil {
		return nil, err
	}
	return release.WebUI, nil
}

func loadRelease(filesystem fs.FS, expectedIdentity buildinfo.Identity) (Release, error) {
	if filesystem == nil {
		return Release{}, errors.New("load embedded release: filesystem is required")
	}
	expectedIdentity, err := buildinfo.Parse(expectedIdentity.SourceRevision)
	if err != nil {
		return Release{}, fmt.Errorf("load embedded release: invalid compiled identity: %w", err)
	}
	encoded, err := fs.ReadFile(filesystem, "out/"+buildassets.ManifestFilename)
	if err != nil {
		return Release{}, fmt.Errorf("load embedded release: read build manifest: %w", err)
	}
	manifest, err := buildassets.Unmarshal(encoded)
	if err != nil {
		return Release{}, fmt.Errorf("load embedded release: %w", err)
	}
	manifestIdentity, err := buildinfo.Parse(manifest.SourceRevision)
	if err != nil {
		return Release{}, fmt.Errorf("load embedded release: invalid manifest identity: %w", err)
	}
	if !manifestIdentity.Equal(expectedIdentity) {
		return Release{}, fmt.Errorf(
			"load embedded release: manifest source revision %s does not match compiled source revision %s",
			manifestIdentity.SourceRevision,
			expectedIdentity.SourceRevision,
		)
	}
	if err := buildassets.Validate(filesystem, manifest); err != nil {
		return Release{}, fmt.Errorf("load embedded release: %w", err)
	}
	webUI, err := fs.Sub(filesystem, "out")
	if err != nil {
		return Release{}, fmt.Errorf("load embedded release: open web UI: %w", err)
	}
	return Release{
		WebUI: webUI,
		Metadata: ReleaseMetadata{
			FormatVersion:  manifest.FormatVersion,
			SourceRevision: manifest.SourceRevision,
			UIBuildID:      manifest.UIBuildID,
			UI: ComponentMetadata{
				SHA256: manifest.UI.SHA256, FileCount: manifest.UI.FileCount, ByteCount: manifest.UI.ByteCount,
			},
			ProtobufSchema: ComponentMetadata{
				SHA256:    manifest.ProtobufSchema.SHA256,
				FileCount: manifest.ProtobufSchema.FileCount,
				ByteCount: manifest.ProtobufSchema.ByteCount,
			},
			SQLiteMigrations: MigrationMetadata{
				ComponentMetadata: ComponentMetadata{
					SHA256:    manifest.SQLiteMigrations.SHA256,
					FileCount: manifest.SQLiteMigrations.FileCount,
					ByteCount: manifest.SQLiteMigrations.ByteCount,
				},
				LatestVersion: manifest.SQLiteMigrations.LatestVersion,
			},
			ClickHouseMigrations: MigrationMetadata{
				ComponentMetadata: ComponentMetadata{
					SHA256:    manifest.ClickHouseMigrations.SHA256,
					FileCount: manifest.ClickHouseMigrations.FileCount,
					ByteCount: manifest.ClickHouseMigrations.ByteCount,
				},
				LatestVersion: manifest.ClickHouseMigrations.LatestVersion,
			},
		},
	}, nil
}
