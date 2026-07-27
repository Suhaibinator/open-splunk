package buildmetadata

import (
	"errors"
	"fmt"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"google.golang.org/protobuf/proto"
)

// ErrVersionMismatch reports a contradiction between a legacy display version
// and the structured build identity.
var ErrVersionMismatch = errors.New("display version does not match structured build metadata")

// Clone returns an ownership-isolated copy of metadata.
func Clone(metadata *opensplunkv1.BuildMetadata) *opensplunkv1.BuildMetadata {
	if metadata == nil {
		return nil
	}
	return proto.Clone(metadata).(*opensplunkv1.BuildMetadata)
}

// Normalize clones and validates metadata, then resolves the display version
// carried by legacy protocol fields.
func Normalize(
	metadata *opensplunkv1.BuildMetadata,
	displayVersion string,
) (*opensplunkv1.BuildMetadata, string, error) {
	cloned := Clone(metadata)
	identity, err := Validate(cloned)
	if err != nil {
		return nil, "", err
	}
	expectedVersion := identity.DisplayVersion()
	if displayVersion != "" && displayVersion != expectedVersion {
		return nil, "", ErrVersionMismatch
	}
	return cloned, expectedVersion, nil
}

// ValidateMetadata verifies that structured server metadata describes one
// complete, internally consistent embedded release.
func Validate(metadata *opensplunkv1.BuildMetadata) (buildinfo.Identity, error) {
	if metadata == nil {
		return buildinfo.Identity{}, errors.New("build metadata is required")
	}
	if len(metadata.ProtoReflect().GetUnknown()) != 0 {
		return buildinfo.Identity{}, errors.New("build metadata contains unknown fields")
	}
	identity, err := buildinfo.Parse(metadata.GetApplicationVersion(), metadata.GetSourceRevision())
	if err != nil {
		return buildinfo.Identity{}, fmt.Errorf("invalid build identity: %w", err)
	}
	expectedUIBuildID, err := identity.UIBuildID()
	if err != nil {
		return buildinfo.Identity{}, err
	}
	if metadata.GetUiBuildId() != expectedUIBuildID {
		return buildinfo.Identity{}, errors.New("UI build ID does not match the build identity")
	}
	digests := []struct {
		name  string
		value string
	}{
		{name: "UI", value: metadata.GetUiSha256()},
		{name: "protobuf schema", value: metadata.GetProtobufSchemaSha256()},
		{name: "SQLite migrations", value: metadata.GetSqliteMigrationsSha256()},
		{name: "ClickHouse migrations", value: metadata.GetClickhouseMigrationsSha256()},
	}
	for _, digest := range digests {
		if !buildinfo.ValidSHA256(digest.value) {
			return buildinfo.Identity{}, fmt.Errorf("%s SHA-256 digest is invalid", digest.name)
		}
	}
	if metadata.GetSqliteMigrationVersion() == 0 {
		return buildinfo.Identity{}, errors.New("SQLite migration version must be positive")
	}
	if metadata.GetClickhouseMigrationVersion() == 0 {
		return buildinfo.Identity{}, errors.New("ClickHouse migration version must be positive")
	}
	if metadata.GetAssetManifestFormatVersion() != buildinfo.AssetManifestFormatVersion {
		return buildinfo.Identity{}, fmt.Errorf(
			"asset manifest format version is %d, want %d",
			metadata.GetAssetManifestFormatVersion(),
			buildinfo.AssetManifestFormatVersion,
		)
	}
	return identity, nil
}
