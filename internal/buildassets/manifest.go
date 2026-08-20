// Package buildassets defines and verifies the deterministic build-asset
// manifest shared by the frontend build and the embedded Go server.
package buildassets

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

const (
	ManifestFormatVersion = buildinfo.AssetManifestFormatVersion
	ManifestFilename      = "asset-manifest.json"

	maximumManifestBytes = 1 << 20
	maximumTreeFiles     = 10_000
	maximumTreeBytes     = 512 << 20
	maximumHTMLBytes     = 16 << 20
)

var (
	migrationNamePattern = regexp.MustCompile(`^([0-9]{4})_[a-z0-9][a-z0-9_]*\.sql$`)
	htmlAssetReference   = regexp.MustCompile(`(?i)(?:src|href)\s*=\s*["'](/_next/[^"'?#]+)`)
	windowsReservedNames = []string{
		"CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
	}
)

// FileDigest binds one UI-relative path to its exact byte representation.
type FileDigest struct {
	Path   string `json:"path"`
	Size   uint64 `json:"size"`
	SHA256 string `json:"sha256"`
}

// ComponentDigest summarizes a canonically framed, ordered source tree.
type ComponentDigest struct {
	SHA256    string `json:"sha256"`
	FileCount uint32 `json:"file_count"`
	ByteCount uint64 `json:"byte_count"`
}

// UIComponentDigest includes the complete file allowlist used by runtime
// verification and cross-platform serving proofs.
type UIComponentDigest struct {
	SHA256    string       `json:"sha256"`
	FileCount uint32       `json:"file_count"`
	ByteCount uint64       `json:"byte_count"`
	Files     []FileDigest `json:"files"`
}

// MigrationDigest records both corpus identity and its latest contiguous
// schema version. Sequence numbers remain independent of the source revision.
type MigrationDigest struct {
	SHA256        string `json:"sha256"`
	FileCount     uint32 `json:"file_count"`
	ByteCount     uint64 `json:"byte_count"`
	LatestVersion uint32 `json:"latest_version"`
}

// Manifest is target-independent. It deliberately excludes timestamps,
// absolute paths, GOOS/GOARCH, and other ambient build-machine data.
type Manifest struct {
	FormatVersion        uint32            `json:"format_version"`
	SourceRevision       string            `json:"source_revision"`
	UIBuildID            string            `json:"ui_build_id"`
	UI                   UIComponentDigest `json:"ui"`
	ProtobufSchema       ComponentDigest   `json:"protobuf_schema"`
	SQLiteMigrations     MigrationDigest   `json:"sqlite_migrations"`
	ClickHouseMigrations MigrationDigest   `json:"clickhouse_migrations"`
}

type inventoryOptions struct {
	include func(string) bool
	exclude func(string) bool
}

type treeInventory struct {
	component ComponentDigest
	files     []FileDigest
}

// Generate computes a manifest from a repository-rooted filesystem. Next's
// build ID must already match the source identity, binding every HTML/RSC
// reference to the same revision before hashes are computed.
func Generate(filesystem fs.FS, sourceRevision string) (Manifest, error) {
	if filesystem == nil {
		return Manifest{}, errors.New("generate build manifest: filesystem is required")
	}
	identity, err := buildinfo.Parse(sourceRevision)
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: %w", err)
	}
	buildIDBytes, err := fs.ReadFile(filesystem, ".next/BUILD_ID")
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: read Next build ID: %w", err)
	}
	if len(buildIDBytes) > 128 {
		return Manifest{}, errors.New("generate build manifest: Next build ID is too large")
	}
	buildID := strings.TrimSpace(string(buildIDBytes))
	expectedBuildID, err := identity.UIBuildID()
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: %w", err)
	}
	if buildID != expectedBuildID {
		return Manifest{}, fmt.Errorf(
			"generate build manifest: Next build ID %q does not match derived ID %q for source revision %q",
			buildID,
			expectedBuildID,
			sourceRevision,
		)
	}

	manifest, err := computeManifest(filesystem, identity, expectedBuildID)
	if err != nil {
		return Manifest{}, err
	}
	if _, err := validateManifestShape(manifest); err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: %w", err)
	}
	return manifest, nil
}

func computeManifest(
	filesystem fs.FS,
	identity buildinfo.Identity,
	uiBuildID string,
) (Manifest, error) {
	ui, err := inventoryTree(filesystem, "out", inventoryOptions{
		exclude: func(relativePath string) bool {
			return relativePath == ManifestFilename
		},
	})
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: inventory UI: %w", err)
	}
	if !slices.ContainsFunc(ui.files, func(file FileDigest) bool { return file.Path == "index.html" }) {
		return Manifest{}, errors.New("generate build manifest: UI index.html is missing")
	}
	if err := validateHTMLReferences(filesystem, ui.files); err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: %w", err)
	}

	protobuf, err := inventoryTree(filesystem, "proto", inventoryOptions{})
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: inventory protobuf schema: %w", err)
	}
	for _, file := range protobuf.files {
		if !strings.HasSuffix(file.Path, ".proto") {
			return Manifest{}, fmt.Errorf(
				"generate build manifest: protobuf schema contains non-.proto file %q",
				file.Path,
			)
		}
	}
	sqlite, sqliteVersion, err := inventoryMigrations(filesystem, "migrations/sqlite")
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: inventory SQLite migrations: %w", err)
	}
	clickHouse, clickHouseVersion, err := inventoryMigrations(filesystem, "migrations/clickhouse")
	if err != nil {
		return Manifest{}, fmt.Errorf("generate build manifest: inventory ClickHouse migrations: %w", err)
	}
	return Manifest{
		FormatVersion:  ManifestFormatVersion,
		SourceRevision: identity.SourceRevision,
		UIBuildID:      uiBuildID,
		UI: UIComponentDigest{
			SHA256:    ui.component.SHA256,
			FileCount: ui.component.FileCount,
			ByteCount: ui.component.ByteCount,
			Files:     ui.files,
		},
		ProtobufSchema: protobuf.component,
		SQLiteMigrations: MigrationDigest{
			SHA256:        sqlite.component.SHA256,
			FileCount:     sqlite.component.FileCount,
			ByteCount:     sqlite.component.ByteCount,
			LatestVersion: sqliteVersion,
		},
		ClickHouseMigrations: MigrationDigest{
			SHA256:        clickHouse.component.SHA256,
			FileCount:     clickHouse.component.FileCount,
			ByteCount:     clickHouse.component.ByteCount,
			LatestVersion: clickHouseVersion,
		},
	}, nil
}

// Validate recomputes every embedded component and rejects any missing,
// modified, or extra UI file. The manifest itself is excluded from the UI hash
// to avoid a recursive digest.
func Validate(filesystem fs.FS, manifest Manifest) error {
	if filesystem == nil {
		return errors.New("validate build manifest: filesystem is required")
	}
	identity, err := validateManifestShape(manifest)
	if err != nil {
		return fmt.Errorf("validate build manifest: %w", err)
	}
	recomputed, err := computeManifest(filesystem, identity, manifest.UIBuildID)
	if err != nil {
		return fmt.Errorf("validate build manifest: %w", err)
	}
	if recomputed.UI.SHA256 != manifest.UI.SHA256 ||
		recomputed.UI.FileCount != manifest.UI.FileCount ||
		recomputed.UI.ByteCount != manifest.UI.ByteCount ||
		!slices.Equal(recomputed.UI.Files, manifest.UI.Files) {
		return errors.New("validate build manifest: UI files do not match the manifest")
	}
	if recomputed.ProtobufSchema != manifest.ProtobufSchema {
		return errors.New("validate build manifest: protobuf schema does not match the manifest")
	}
	if recomputed.SQLiteMigrations != manifest.SQLiteMigrations {
		return errors.New("validate build manifest: SQLite migrations do not match the manifest")
	}
	if recomputed.ClickHouseMigrations != manifest.ClickHouseMigrations {
		return errors.New("validate build manifest: ClickHouse migrations do not match the manifest")
	}
	return nil
}

// Marshal returns the one accepted JSON representation of a manifest.
func Marshal(manifest Manifest) ([]byte, error) {
	if _, err := validateManifestShape(manifest); err != nil {
		return nil, fmt.Errorf("marshal build manifest: %w", err)
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal build manifest: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maximumManifestBytes {
		return nil, fmt.Errorf(
			"marshal build manifest: size %d exceeds %d bytes",
			len(encoded),
			maximumManifestBytes,
		)
	}
	return encoded, nil
}

// Unmarshal accepts only canonical JSON, rejects unknown fields and trailing
// values, and validates all bounded field formats before returning.
func Unmarshal(encoded []byte) (Manifest, error) {
	if len(encoded) == 0 || len(encoded) > maximumManifestBytes {
		return Manifest{}, fmt.Errorf(
			"unmarshal build manifest: size must be between 1 and %d bytes",
			maximumManifestBytes,
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("unmarshal build manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, errors.New("unmarshal build manifest: trailing JSON value")
		}
		return Manifest{}, fmt.Errorf("unmarshal build manifest: trailing data: %w", err)
	}
	canonical, err := Marshal(manifest)
	if err != nil {
		return Manifest{}, fmt.Errorf("unmarshal build manifest: %w", err)
	}
	if !bytes.Equal(encoded, canonical) {
		return Manifest{}, errors.New("unmarshal build manifest: JSON is not canonical")
	}
	return manifest, nil
}

func inventoryTree(filesystem fs.FS, root string, options inventoryOptions) (treeInventory, error) {
	if !fs.ValidPath(root) || root == "." {
		return treeInventory{}, fmt.Errorf("invalid tree root %q", root)
	}
	rootInfo, err := fs.Lstat(filesystem, root)
	if err != nil {
		return treeInventory{}, fmt.Errorf("inspect tree root %q: %w", root, err)
	}
	if rootInfo.Mode()&fs.ModeSymlink != 0 {
		return treeInventory{}, fmt.Errorf("tree root %q is a symbolic link", root)
	}
	if !rootInfo.IsDir() {
		return treeInventory{}, fmt.Errorf("tree root %q is not a directory", root)
	}
	var paths []string
	err = fs.WalkDir(filesystem, root, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if filePath == root || entry.IsDir() {
			return nil
		}
		relativePath := strings.TrimPrefix(filePath, root+"/")
		if relativePath == filePath || !fs.ValidPath(relativePath) {
			return fmt.Errorf("invalid relative path %q", filePath)
		}
		if err := validateGoEmbedPath(relativePath); err != nil {
			return fmt.Errorf("%q is not eligible for Go embedding: %w", filePath, err)
		}
		if entry.Type()&fs.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("%q is not a regular file", filePath)
		}
		if options.exclude != nil && options.exclude(relativePath) {
			return nil
		}
		if options.include != nil && !options.include(relativePath) {
			return nil
		}
		paths = append(paths, relativePath)
		if len(paths) > maximumTreeFiles {
			return fmt.Errorf("tree contains more than %d files", maximumTreeFiles)
		}
		return nil
	})
	if err != nil {
		return treeInventory{}, err
	}
	slices.Sort(paths)
	if len(paths) == 0 {
		return treeInventory{}, errors.New("tree contains no files")
	}

	aggregate := sha256.New()
	_, _ = aggregate.Write([]byte("open-splunk-tree-v1\x00"))
	files := make([]FileDigest, 0, len(paths))
	var totalBytes uint64
	for _, relativePath := range paths {
		file, err := filesystem.Open(path.Join(root, relativePath))
		if err != nil {
			return treeInventory{}, fmt.Errorf("open %q: %w", path.Join(root, relativePath), err)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return treeInventory{}, fmt.Errorf("stat %q: %w", path.Join(root, relativePath), statErr)
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			_ = file.Close()
			return treeInventory{}, fmt.Errorf("%q is not a regular file", path.Join(root, relativePath))
		}
		// #nosec G115 -- the negative-size case is rejected immediately above.
		size := uint64(info.Size())
		if size > maximumTreeBytes || totalBytes > maximumTreeBytes-size {
			_ = file.Close()
			return treeInventory{}, fmt.Errorf("tree exceeds %d bytes", maximumTreeBytes)
		}
		totalBytes += size

		if err := writeFramedPath(aggregate, relativePath, size); err != nil {
			_ = file.Close()
			return treeInventory{}, err
		}
		fileHash := sha256.New()
		written, copyErr := io.Copy(
			io.MultiWriter(aggregate, fileHash),
			io.LimitReader(file, int64(size)+1),
		)
		closeErr := file.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return treeInventory{}, fmt.Errorf("hash %q: %w", path.Join(root, relativePath), err)
		}
		if written != info.Size() {
			return treeInventory{}, fmt.Errorf(
				"hash %q: read %d bytes, expected %d",
				path.Join(root, relativePath),
				written,
				info.Size(),
			)
		}
		files = append(files, FileDigest{
			Path:   relativePath,
			Size:   size,
			SHA256: hex.EncodeToString(fileHash.Sum(nil)),
		})
	}
	return treeInventory{
		component: ComponentDigest{
			SHA256: hex.EncodeToString(aggregate.Sum(nil)),
			// #nosec G115 -- paths is bounded by maximumTreeFiles before files is populated.
			FileCount: uint32(len(files)),
			ByteCount: totalBytes,
		},
		files: files,
	}, nil
}

// validateGoEmbedPath mirrors the filename exclusions applied by Go 1.26's
// embed resolver. Rejecting these paths is safer than letting the manifest
// inventory bytes that //go:embed will silently omit at a VCS, nested-module,
// or invalid-directory boundary.
func validateGoEmbedPath(relativePath string) error {
	for component := range strings.SplitSeq(relativePath, "/") {
		if strings.EqualFold(component, "go.mod") {
			return errors.New("nested go.mod creates an embed module boundary")
		}
		switch component {
		case ".bzr", ".git", ".hg", ".svn":
			return fmt.Errorf("version-control path component %q is excluded", component)
		}
		if err := validateGoEmbedName(component); err != nil {
			return err
		}
	}
	return nil
}

func validateGoEmbedName(name string) error {
	if name == "" || !utf8.ValidString(name) {
		return fmt.Errorf("invalid UTF-8 or empty path component %q", name)
	}
	if strings.Count(name, ".") == len(name) || strings.HasSuffix(name, ".") {
		return fmt.Errorf("invalid dot path component %q", name)
	}
	const allowedASCIIPunctuation = "!#$%&()+,-.=@[]^_{}~ "
	for _, character := range name {
		if character >= utf8.RuneSelf {
			if unicode.IsLetter(character) {
				continue
			}
			return fmt.Errorf("invalid character %q in path component %q", character, name)
		}
		if character >= '0' && character <= '9' ||
			character >= 'A' && character <= 'Z' ||
			character >= 'a' && character <= 'z' ||
			strings.ContainsRune(allowedASCIIPunctuation, character) {
			continue
		}
		return fmt.Errorf("invalid character %q in path component %q", character, name)
	}
	shortName := name
	if dot := strings.IndexByte(shortName, '.'); dot >= 0 {
		shortName = shortName[:dot]
	}
	for _, reserved := range windowsReservedNames {
		if strings.EqualFold(shortName, reserved) {
			return fmt.Errorf("reserved path component %q", name)
		}
	}
	return nil
}

func writeFramedPath(writer io.Writer, relativePath string, size uint64) error {
	if len(relativePath) == 0 || len(relativePath) > 1<<20 {
		return fmt.Errorf("invalid framed path length for %q", relativePath)
	}
	var length [4]byte
	// #nosec G115 -- relativePath is explicitly bounded to at most 1 MiB above.
	binary.BigEndian.PutUint32(length[:], uint32(len(relativePath)))
	var byteLength [8]byte
	binary.BigEndian.PutUint64(byteLength[:], size)
	if _, err := writer.Write(length[:]); err != nil {
		return fmt.Errorf("hash path length: %w", err)
	}
	if _, err := io.WriteString(writer, relativePath); err != nil {
		return fmt.Errorf("hash path: %w", err)
	}
	if _, err := writer.Write(byteLength[:]); err != nil {
		return fmt.Errorf("hash file length: %w", err)
	}
	return nil
}

func inventoryMigrations(filesystem fs.FS, root string) (treeInventory, uint32, error) {
	inventory, err := inventoryTree(filesystem, root, inventoryOptions{
		include: func(relativePath string) bool {
			return strings.HasSuffix(relativePath, ".sql")
		},
	})
	if err != nil {
		return treeInventory{}, 0, err
	}
	for index, file := range inventory.files {
		if strings.Contains(file.Path, "/") {
			return treeInventory{}, 0, fmt.Errorf("migration %q must be at the migration root", file.Path)
		}
		matches := migrationNamePattern.FindStringSubmatch(file.Path)
		if matches == nil {
			return treeInventory{}, 0, fmt.Errorf("migration filename %q is invalid", file.Path)
		}
		version, err := strconv.ParseUint(matches[1], 10, 32)
		if err != nil {
			return treeInventory{}, 0, fmt.Errorf("parse migration version in %q: %w", file.Path, err)
		}
		want := uint64(index + 1)
		if version != want {
			return treeInventory{}, 0, fmt.Errorf(
				"migration versions are not contiguous: %q is version %d, want %d",
				file.Path,
				version,
				want,
			)
		}
	}
	// #nosec G115 -- migration inventory length is bounded by maximumTreeFiles.
	return inventory, uint32(len(inventory.files)), nil
}

func validateHTMLReferences(filesystem fs.FS, files []FileDigest) error {
	available := make(map[string]struct{}, len(files))
	for _, file := range files {
		available[file.Path] = struct{}{}
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Path, ".html") {
			continue
		}
		if file.Size > maximumHTMLBytes {
			return fmt.Errorf("UI HTML %q exceeds %d bytes", file.Path, maximumHTMLBytes)
		}
		document, err := readBoundedFile(filesystem, path.Join("out", file.Path), maximumHTMLBytes)
		if err != nil {
			return fmt.Errorf("read UI HTML %q: %w", file.Path, err)
		}
		for _, match := range htmlAssetReference.FindAllSubmatch(document, -1) {
			reference := html.UnescapeString(string(match[1]))
			decoded, err := url.PathUnescape(reference)
			if err != nil {
				return fmt.Errorf("UI HTML %q references asset %q with invalid escaping", file.Path, reference)
			}
			relativePath := strings.TrimPrefix(decoded, "/")
			if !fs.ValidPath(relativePath) || !strings.HasPrefix(relativePath, "_next/") {
				return fmt.Errorf("UI HTML %q references asset %q with an invalid path", file.Path, reference)
			}
			if _, exists := available[relativePath]; !exists {
				return fmt.Errorf("UI HTML %q referenced asset %q is missing", file.Path, reference)
			}
		}
	}
	return nil
}

func readBoundedFile(filesystem fs.FS, name string, maximumBytes uint64) ([]byte, error) {
	file, err := filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	// #nosec G115 -- all callers pass manifest limits far below math.MaxInt64.
	contents, readErr := io.ReadAll(io.LimitReader(file, int64(maximumBytes)+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if uint64(len(contents)) > maximumBytes {
		return nil, fmt.Errorf("file exceeds %d bytes", maximumBytes)
	}
	return contents, nil
}

func validateManifestShape(manifest Manifest) (buildinfo.Identity, error) {
	if manifest.FormatVersion != ManifestFormatVersion {
		return buildinfo.Identity{}, fmt.Errorf(
			"manifest format version is %d, want %d",
			manifest.FormatVersion,
			ManifestFormatVersion,
		)
	}
	identity, err := buildinfo.Parse(manifest.SourceRevision)
	if err != nil {
		return buildinfo.Identity{}, err
	}
	expectedBuildID, err := identity.UIBuildID()
	if err != nil {
		return buildinfo.Identity{}, err
	}
	if manifest.UIBuildID != expectedBuildID {
		return buildinfo.Identity{}, errors.New("UI build ID does not match the source identity's derived ID")
	}
	if err := validateUIComponent(manifest.UI); err != nil {
		return buildinfo.Identity{}, err
	}
	if err := validateComponent("protobuf schema", manifest.ProtobufSchema); err != nil {
		return buildinfo.Identity{}, err
	}
	if err := validateMigrationComponent("SQLite migrations", manifest.SQLiteMigrations); err != nil {
		return buildinfo.Identity{}, err
	}
	if err := validateMigrationComponent("ClickHouse migrations", manifest.ClickHouseMigrations); err != nil {
		return buildinfo.Identity{}, err
	}
	return identity, nil
}

func validateUIComponent(component UIComponentDigest) error {
	if err := validateComponent("UI", ComponentDigest{
		SHA256: component.SHA256, FileCount: component.FileCount, ByteCount: component.ByteCount,
	}); err != nil {
		return err
	}
	if len(component.Files) != int(component.FileCount) {
		return errors.New("UI file list length does not match its file count")
	}
	var total uint64
	previous := ""
	for _, file := range component.Files {
		if !fs.ValidPath(file.Path) || file.Path == ManifestFilename {
			return fmt.Errorf("UI file path %q is invalid", file.Path)
		}
		if previous != "" && file.Path <= previous {
			return errors.New("UI files are not in strict canonical path order")
		}
		if !buildinfo.ValidSHA256(file.SHA256) {
			return fmt.Errorf("UI file %q has an invalid SHA-256 digest", file.Path)
		}
		if file.Size > maximumTreeBytes || total > maximumTreeBytes-file.Size {
			return errors.New("UI file bytes exceed the manifest bound")
		}
		total += file.Size
		previous = file.Path
	}
	if total != component.ByteCount {
		return errors.New("UI file sizes do not match its byte count")
	}
	return nil
}

func validateComponent(name string, component ComponentDigest) error {
	if !buildinfo.ValidSHA256(component.SHA256) {
		return fmt.Errorf("%s has an invalid SHA-256 digest", name)
	}
	if component.FileCount == 0 || component.FileCount > maximumTreeFiles {
		return fmt.Errorf("%s file count is outside the supported bound", name)
	}
	if component.ByteCount > maximumTreeBytes {
		return fmt.Errorf("%s byte count is outside the supported bound", name)
	}
	return nil
}

func validateMigrationComponent(name string, component MigrationDigest) error {
	if err := validateComponent(name, ComponentDigest{
		SHA256: component.SHA256, FileCount: component.FileCount, ByteCount: component.ByteCount,
	}); err != nil {
		return err
	}
	if component.LatestVersion == 0 || component.LatestVersion != component.FileCount {
		return fmt.Errorf("%s latest version does not match its contiguous file count", name)
	}
	return nil
}
