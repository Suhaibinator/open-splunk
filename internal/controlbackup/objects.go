//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/exportjournal"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

const maximumObjectCatalogBytes = uint64(64 << 20)

type objectReference struct {
	Identity searchartifacts.BlobIdentity `json:"identity"`
	SHA256   string                       `json:"sha256"`
	FileName string                       `json:"file_name"`
}

func openPinnedChild(parent *privatefs.Directory, name string) (*privatefs.Directory, error) {
	child, err := privatefs.OpenDirectory(filepath.Join(parent.Path(), name))
	if err != nil {
		return nil, err
	}
	if err := parent.RequirePinnedChildDirectory(name, child); err != nil {
		return nil, errors.Join(err, child.Close())
	}
	return child, nil
}
func validObjectName(name string) bool {
	return strings.HasSuffix(name, ".blob") && validLowerHex(strings.TrimSuffix(name, ".blob"), sha256.Size)
}
func inspectObjectDirectory(ctx context.Context, directory *privatefs.Directory, _ bool) (DirectoryIdentity, []FileIdentity, error) {
	entries, err := directory.List(int(maximumSearchArtifactFiles))
	if err != nil {
		return DirectoryIdentity{}, nil, err
	}
	slices.Sort(entries)
	identities := make([]FileIdentity, 0, len(entries))
	var size uint64
	hash := sha256.New()
	for _, name := range entries {
		if !validObjectName(name) {
			return DirectoryIdentity{}, nil, errors.New("object archive has an unexpected member")
		}
		member, err := inspectMember(ctx, directory, name, anySizePrivateFilePolicy, false)
		if err != nil {
			return DirectoryIdentity{}, nil, err
		}
		if member.identity.SHA256 != strings.TrimSuffix(name, ".blob") {
			return DirectoryIdentity{}, nil, errors.New("object archive content does not match its name")
		}
		if maximumSearchArtifactBytes-size < member.identity.SizeBytes {
			return DirectoryIdentity{}, nil, errors.New("object archive exceeds size bound")
		}
		size += member.identity.SizeBytes
		identities = append(identities, member.identity)
		_, _ = hash.Write([]byte(name + "\x00" + strconv.FormatUint(member.identity.SizeBytes, 10) + "\x00" + member.identity.SHA256 + "\n"))
	}
	return DirectoryIdentity{Name: searchArtifactsFilename, FileCount: uint64(len(identities)), SizeBytes: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, identities, nil
}
func removeObjectDirectory(parent *privatefs.Directory, name string, child *privatefs.Directory) (returnedErr error) {
	if err := parent.RequirePinnedChildDirectory(name, child); err != nil {
		return err
	}
	_, identities, err := inspectObjectDirectory(context.Background(), child, false)
	if err != nil {
		return err
	}
	files := make([]pinnedCleanupFile, 0, len(identities))
	defer func() { returnedErr = errors.Join(returnedErr, closePinnedCleanupFiles(files)) }()
	for _, identity := range identities {
		file, err := child.OpenRegular(identity.Name, mustPolicy(identity))
		if err != nil {
			return err
		}
		files = append(files, pinnedCleanupFile{name: identity.Name, file: file})
	}
	for _, file := range files {
		if err := child.RequirePinnedRegular(file.name, file.file); err != nil {
			return err
		}
	}
	for _, file := range files {
		if err := child.UnlinkPinnedRegular(file.name, file.file); err != nil {
			return err
		}
	}
	if err := child.Sync(); err != nil {
		return err
	}
	return parent.RemovePinnedEmptyDirectory(name, child)
}
func validateObjectCatalog(references []objectReference) error {
	if len(references) > int(maximumSearchArtifactFiles) {
		return errors.New("object reference count exceeds bound")
	}
	previous := ""
	sizes := map[string]int64{}
	for _, ref := range references {
		if ref.Identity.Key == "" || len(ref.Identity.Key) > 8192 || strings.ContainsRune(ref.Identity.Key, 0) || ref.Identity.ETag == "" || len(ref.Identity.ETag) > 1024 || ref.Identity.Version == "" || len(ref.Identity.Version) > 1024 || ref.Identity.Bytes < 0 || uint64(ref.Identity.Bytes) > maximumSearchArtifactBytes || !validLowerHex(ref.SHA256, sha256.Size) || ref.FileName != ref.SHA256+".blob" {
			return errors.New("object reference is invalid")
		}
		if previous != "" && ref.Identity.Key <= previous {
			return errors.New("object references are not unique and sorted")
		}
		if size, ok := sizes[ref.FileName]; ok && size != ref.Identity.Bytes {
			return errors.New("object content digest has conflicting sizes")
		}
		sizes[ref.FileName] = ref.Identity.Bytes
		previous = ref.Identity.Key
	}
	return nil
}
func sourceObjectReferences(ctx context.Context, database *control.DB) ([]objectReference, error) {
	captures, err := searchartifacts.ReferencedBlobs(ctx, database.SQLDB())
	if err != nil {
		return nil, err
	}
	exports, err := exportjournal.ReferencedBlobs(ctx, database.GORMDB())
	if err != nil {
		return nil, err
	}
	all := append(captures, exports...)
	byKey := make(map[string]objectReference, len(all))
	for _, ref := range all {
		if len(ref.SHA256) != sha256.Size {
			return nil, errors.New("SQL-published object digest is invalid")
		}
		digest := hex.EncodeToString(ref.SHA256)
		current := objectReference{Identity: ref.Identity, SHA256: digest, FileName: digest + ".blob"}
		if prior, ok := byKey[ref.Identity.Key]; ok && prior != current {
			return nil, errors.New("SQL-published object key has conflicting identities")
		}
		byKey[ref.Identity.Key] = current
	}
	references := make([]objectReference, 0, len(byKey))
	for _, ref := range byKey {
		references = append(references, ref)
	}
	slices.SortFunc(references, func(a, b objectReference) int { return strings.Compare(a.Identity.Key, b.Identity.Key) })
	return references, validateObjectCatalog(references)
}
func archiveObjects(ctx context.Context, stage *privatefs.Directory, blobs searchartifacts.BlobStore, references []objectReference) (DirectoryIdentity, error) {
	_, objects, err := stage.CreateTemporaryDirectory(fixedNameGenerator(searchArtifactsFilename))
	if err != nil {
		return DirectoryIdentity{}, err
	}
	defer objects.Close()
	seen := make(map[string]bool)
	for _, ref := range references {
		if err := ctx.Err(); err != nil {
			return DirectoryIdentity{}, err
		}
		if seen[ref.FileName] {
			continue
		}
		seen[ref.FileName] = true
		if blobs == nil {
			return DirectoryIdentity{}, errors.New("private object store is required for referenced objects")
		}
		reader, err := blobs.Open(ctx, ref.Identity)
		if err != nil {
			return DirectoryIdentity{}, fmt.Errorf("open SQL-published object for backup: %w", err)
		}
		err = writeObject(ctx, objects, ref, reader)
		err = errors.Join(err, reader.Close())
		if err != nil {
			return DirectoryIdentity{}, err
		}
	}
	if err := objects.Sync(); err != nil {
		return DirectoryIdentity{}, err
	}
	identity, _, err := inspectObjectDirectory(ctx, objects, false)
	return identity, err
}
func writeObject(ctx context.Context, directory *privatefs.Directory, ref objectReference, reader io.Reader) (returnedErr error) {
	name, file, err := directory.CreateTemporaryFile(privatefs.RandomName(".object-"))
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			returnedErr = errors.Join(returnedErr, directory.UnlinkPinnedRegular(name, file))
		}
		returnedErr = errors.Join(returnedErr, file.Close())
	}()
	hash := sha256.New()
	count, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(reader, ref.Identity.Bytes+1))
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if count != ref.Identity.Bytes || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return errors.New("object content differs from its SQL identity")
	}
	if err := privatefs.SyncFile(file); err != nil {
		return err
	}
	if err := directory.RenameNoReplace(name, directory, ref.FileName); err != nil {
		return err
	}
	published = true
	return directory.Sync()
}
func readObjectCatalog(ctx context.Context, bundle *privatefs.Directory, manifest Manifest) ([]objectReference, error) {
	member, err := inspectMember(ctx, bundle, blobCatalogFilename, mustPolicy(manifest.BlobCatalog), true)
	if err != nil {
		return nil, err
	}
	if err := requireFileIdentity(member, manifest.BlobCatalog); err != nil {
		return nil, err
	}
	return UnmarshalCanonicalJSON(member.contents, "object reference catalog", int(maximumObjectCatalogBytes), validateObjectCatalog)
}

// Current identifies the current private object version; Open still pins reads
// to that exact version. Recovery refuses a collision with different content.
type currentBlobStore interface {
	Current(context.Context, string) (searchartifacts.BlobIdentity, error)
}

func restoreObjects(ctx context.Context, bundle *privatefs.Directory, blobs searchartifacts.BlobStore, references []objectReference) (map[string]searchartifacts.BlobIdentity, error) {
	mapping := make(map[string]searchartifacts.BlobIdentity, len(references))
	if len(references) == 0 {
		return mapping, nil
	}
	current, ok := blobs.(currentBlobStore)
	if !ok {
		return nil, errors.New("private object store requires current-version inspection for safe recovery")
	}
	objects, err := openPinnedChild(bundle, searchArtifactsFilename)
	if err != nil {
		return nil, err
	}
	defer objects.Close()
	for _, ref := range references {
		identity, lookupErr := current.Current(ctx, ref.Identity.Key)
		if lookupErr == nil {
			if identity.Key != ref.Identity.Key || identity.Bytes != ref.Identity.Bytes {
				return nil, errors.New("recovery object target has conflicting content")
			}
			reader, err := blobs.Open(ctx, identity)
			if err != nil {
				return nil, err
			}
			hash := sha256.New()
			n, readErr := io.Copy(hash, io.LimitReader(reader, identity.Bytes+1))
			readErr = errors.Join(readErr, reader.Close())
			if readErr != nil {
				return nil, readErr
			}
			if n != identity.Bytes || !bytes.Equal(hash.Sum(nil), mustDigest(ref.SHA256)) {
				return nil, errors.New("recovery object target has conflicting content")
			}
			mapping[ref.Identity.Key] = identity
			continue
		}
		if !errors.Is(lookupErr, os.ErrNotExist) && !errors.Is(lookupErr, searchartifacts.ErrNotFound) {
			return nil, lookupErr
		}
		file, err := objects.OpenRegular(ref.FileName, privatefs.FilePolicy{AllowedModes: privateMode, MinimumSize: ref.Identity.Bytes, MaximumSize: ref.Identity.Bytes})
		if err != nil {
			return nil, err
		}
		hash := sha256.New()
		identity, putErr := blobs.Put(ctx, ref.Identity.Key, io.TeeReader(file, hash), ref.Identity.Bytes)
		putErr = errors.Join(putErr, file.Close())
		if putErr != nil {
			return nil, putErr
		}
		if identity.Key != ref.Identity.Key || identity.Bytes != ref.Identity.Bytes || identity.Version == "" || identity.ETag == "" || !bytes.Equal(hash.Sum(nil), mustDigest(ref.SHA256)) {
			_ = blobs.Delete(context.WithoutCancel(ctx), identity)
			return nil, errors.New("restored object identity or checksum is invalid")
		}
		mapping[ref.Identity.Key] = identity
	}
	return mapping, nil
}
func mustDigest(value string) []byte { digest, _ := hex.DecodeString(value); return digest }
