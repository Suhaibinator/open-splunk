package buildmetadata

import (
	"errors"
	"fmt"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"google.golang.org/protobuf/proto"
)

// Clone returns an ownership-isolated copy of metadata.
func Clone(metadata *opensplunk.BuildMetadata) *opensplunk.BuildMetadata {
	if metadata == nil {
		return nil
	}
	return proto.Clone(metadata).(*opensplunk.BuildMetadata)
}

// Normalize clones and validates metadata without exposing mutable caller
// ownership to a long-lived service.
func Normalize(metadata *opensplunk.BuildMetadata) (*opensplunk.BuildMetadata, error) {
	cloned := Clone(metadata)
	if _, err := Validate(cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

// ValidateMetadata verifies that structured server metadata describes one
// complete, internally consistent embedded release.
func Validate(metadata *opensplunk.BuildMetadata) (buildinfo.Identity, error) {
	if metadata == nil {
		return buildinfo.Identity{}, errors.New("build metadata is required")
	}
	if len(metadata.ProtoReflect().GetUnknown()) != 0 {
		return buildinfo.Identity{}, errors.New("build metadata contains unknown fields")
	}
	identity, err := buildinfo.Parse(metadata.GetSourceRevision())
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
	return identity, nil
}
