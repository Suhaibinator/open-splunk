package splcompataudit

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

const (
	maximumAuditObjects          = savedobjects.MaximumCompatibilityAuditSavedSearches
	maximumRepositoryFiles       = 100_000
	maximumRepositoryDirectories = 100_000
	maximumRepositoryEntries     = 200_000
	maximumRepositoryFileBytes   = 2 << 20
	maximumRepositoryTotalBytes  = 256 << 20
)

var repositoryExtensions = map[string]struct{}{
	".go":   {},
	".json": {},
	".md":   {},
	".spl":  {},
	".ts":   {},
	".tsx":  {},
	".yaml": {},
	".yml":  {},
}

// Audit composes the optional control-database and repository inventories. At
// least one explicit source is required; neither source is inferred from the
// running server configuration.
func Audit(ctx context.Context, controlDatabasePath, repositoryRoot string) (Report, error) {
	if err := validateContext(ctx); err != nil {
		return Report{}, err
	}
	if controlDatabasePath == "" && repositoryRoot == "" {
		return Report{}, errors.New("SPL compatibility audit requires a control database or repository")
	}
	reports := make([]Report, 0, 2)
	if controlDatabasePath != "" {
		report, err := AuditControlDatabase(ctx, controlDatabasePath)
		if err != nil {
			return Report{}, err
		}
		reports = append(reports, report)
	}
	if repositoryRoot != "" {
		report, err := AuditRepository(ctx, repositoryRoot)
		if err != nil {
			return Report{}, err
		}
		reports = append(reports, report)
	}
	return mergeReports(reports...)
}

// AuditControlDatabase opens the explicit SQLite database through the
// query-only control-plane path and inventories saved searches without
// mutating, checkpointing, or migrating the source file.
func AuditControlDatabase(ctx context.Context, path string) (Report, error) {
	if err := validateContext(ctx); err != nil {
		return Report{}, err
	}
	database, err := control.OpenReadOnly(ctx, path)
	if err != nil {
		return Report{}, fmt.Errorf("open SPL compatibility audit database: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()

	store, err := savedobjects.New(database, savedobjects.Options{
		CursorKey: bytes.Repeat([]byte{0x5a}, 32),
	})
	if err != nil {
		return Report{}, errors.New("create SPL compatibility audit saved-search reader")
	}
	report := newReport()
	var sourceAuditErr error
	scanned, scanErr := store.ScanAllForCompatibilityAudit(
		ctx,
		func(savedSearchID, source string) error {
			sourceRanges, err := auditSource(
				source,
				MaximumAuditFindings-len(report.Findings),
			)
			if err != nil {
				sourceAuditErr = err
				return err
			}
			for _, sourceRange := range sourceRanges {
				report.Findings = append(report.Findings, finding(
					savedSearchID,
					"control-db/saved_searches/"+savedSearchID,
					sourceRange,
				))
			}
			return nil
		},
	)
	if sourceAuditErr != nil {
		return Report{}, sourceAuditErr
	}
	if scanErr != nil {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		if errors.Is(scanErr, savedobjects.ErrCompatibilityAuditLimitExceeded) {
			return Report{}, fmt.Errorf(
				"SPL compatibility audit exceeds %d saved searches",
				maximumAuditObjects,
			)
		}
		return Report{}, errors.New("read SPL compatibility audit saved searches")
	}
	report.ScannedObjects = scanned
	if err := database.Close(); err != nil {
		return Report{}, fmt.Errorf("close SPL compatibility audit database: %w", err)
	}
	closed = true
	return mergeReports(report)
}

// AuditRepository inventories explicit SPL examples and fixtures under root.
// It never follows symbolic links and skips generated, dependency, VCS, cache,
// and build trees. Ordinary source files are considered only on lines that
// visibly contain an eval/where pipeline; .spl files are parsed as whole SPL.
func AuditRepository(ctx context.Context, root string) (Report, error) {
	if err := validateContext(ctx); err != nil {
		return Report{}, err
	}
	absolute, err := filepath.Abs(root)
	if err != nil || absolute == "" {
		return Report{}, errors.New("resolve SPL compatibility audit repository")
	}
	rootInfo, err := os.Lstat(absolute)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return Report{}, errors.New("SPL compatibility audit repository must be a non-symlink directory")
	}
	rootHandle, err := os.OpenRoot(absolute)
	if err != nil {
		return Report{}, errors.New("open SPL compatibility audit repository")
	}
	closed := false
	defer func() {
		if !closed {
			_ = rootHandle.Close()
		}
	}()
	openedRoot, err := rootHandle.Stat(".")
	if err != nil || !os.SameFile(rootInfo, openedRoot) || !openedRoot.IsDir() {
		return Report{}, errors.New("SPL compatibility audit repository changed while opening")
	}
	report, err := auditRepositoryRoot(ctx, rootHandle)
	if err != nil {
		return Report{}, err
	}
	if err := rootHandle.Close(); err != nil {
		return Report{}, errors.New("close SPL compatibility audit repository")
	}
	closed = true
	return mergeReports(report)
}

// auditRepositoryRoot traverses exclusively through os.Root and opened file
// descriptors. Root prevents a concurrent symlink swap from escaping the
// declared tree; Lstat plus SameFile additionally enforces this audit's stricter
// policy of following no symlink at all.
func auditRepositoryRoot(ctx context.Context, root *os.Root) (Report, error) {
	if root == nil {
		return Report{}, errors.New("SPL compatibility audit repository is unavailable")
	}
	state := repositoryAuditState{
		report:      newReport(),
		directories: 1,
		pending:     []string{"."},
	}
	for len(state.pending) > 0 {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		directoryPath := state.pending[len(state.pending)-1]
		state.pending = state.pending[:len(state.pending)-1]
		if err := auditRepositoryDirectory(ctx, root, directoryPath, &state); err != nil {
			return Report{}, err
		}
	}
	return state.report, nil
}

type repositoryAuditState struct {
	report      Report
	files       int
	directories int
	entries     int
	totalBytes  uint64
	pending     []string
}

// auditRepositoryDirectory owns exactly one opened directory descriptor. Its
// named return lets the deferred close preserve a more specific traversal
// error while still making an otherwise-successful close failure visible.
func auditRepositoryDirectory(
	ctx context.Context,
	root *os.Root,
	directoryPath string,
	state *repositoryAuditState,
) (returnedErr error) {
	before, err := root.Lstat(directoryPath)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return errors.New("inspect SPL compatibility audit repository directory")
	}
	directory, err := root.Open(directoryPath)
	if err != nil {
		return errors.New("open SPL compatibility audit repository directory")
	}
	defer func() {
		if closeErr := directory.Close(); returnedErr == nil && closeErr != nil {
			returnedErr = errors.New("close SPL compatibility audit repository directory")
		}
	}()
	opened, err := directory.Stat()
	if err != nil || !opened.IsDir() || !os.SameFile(before, opened) {
		return errors.New("SPL compatibility audit repository directory changed while opening")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, readErr := directory.ReadDir(256)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return errors.New("read SPL compatibility audit repository directory")
		}
		for _, entry := range batch {
			if err := auditRepositoryEntry(root, directoryPath, entry, state); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
	}
}

func auditRepositoryEntry(
	root *os.Root,
	directoryPath string,
	entry os.DirEntry,
	state *repositoryAuditState,
) error {
	if state == nil {
		return errors.New("SPL compatibility audit repository state is unavailable")
	}
	state.entries++
	if state.entries > maximumRepositoryEntries {
		return fmt.Errorf("SPL compatibility audit exceeds %d repository entries", maximumRepositoryEntries)
	}
	name := entry.Name()
	relative := filepath.Join(directoryPath, name)
	info, err := root.Lstat(relative)
	if err != nil {
		return errors.New("inspect SPL compatibility audit repository entry")
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	if info.IsDir() {
		if repositoryDirectoryExcluded(name) {
			return nil
		}
		state.directories++
		if state.directories > maximumRepositoryDirectories {
			return fmt.Errorf("SPL compatibility audit exceeds %d repository directories", maximumRepositoryDirectories)
		}
		state.pending = append(state.pending, relative)
		return nil
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	if _, ok := repositoryExtensions[strings.ToLower(filepath.Ext(name))]; !ok {
		return nil
	}
	if state.files == maximumRepositoryFiles {
		return fmt.Errorf("SPL compatibility audit exceeds %d repository files", maximumRepositoryFiles)
	}
	encoded, err := readRepositoryFile(root, relative, info)
	if err != nil || !utf8.Valid(encoded) {
		return errors.New("read SPL compatibility audit repository file")
	}
	if state.totalBytes > maximumRepositoryTotalBytes ||
		uint64(len(encoded)) > maximumRepositoryTotalBytes-state.totalBytes {
		return fmt.Errorf("SPL compatibility audit exceeds %d repository bytes", maximumRepositoryTotalBytes)
	}
	state.files++
	state.totalBytes += uint64(len(encoded))
	fileReport, err := auditRepositoryFile(relative, encoded)
	if err != nil {
		return err
	}
	if fileReport.ScannedObjects != 0 {
		state.report.ScannedObjects++
	}
	if len(state.report.Findings) > MaximumAuditFindings ||
		len(fileReport.Findings) > MaximumAuditFindings-len(state.report.Findings) {
		return fmt.Errorf("SPL compatibility audit findings exceed %d", MaximumAuditFindings)
	}
	state.report.Findings = append(state.report.Findings, fileReport.Findings...)
	return nil
}

func readRepositoryFile(root *os.Root, relative string, expected os.FileInfo) ([]byte, error) {
	if expected == nil || !expected.Mode().IsRegular() || expected.Size() < 0 ||
		expected.Size() > maximumRepositoryFileBytes {
		return nil, errors.New("repository file metadata is invalid")
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(expected, opened) {
		return nil, errors.New("repository file changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maximumRepositoryFileBytes+1))
	if err != nil || len(encoded) > maximumRepositoryFileBytes {
		return nil, errors.New("repository file exceeds read bound")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(opened, after) || after.Size() != int64(len(encoded)) {
		return nil, errors.New("repository file changed while reading")
	}
	return encoded, nil
}

func repositoryDirectoryExcluded(name string) bool {
	switch name {
	case ".git", ".cache", "build", "dist", "gen", "node_modules", "vendor":
		return true
	default:
		return false
	}
}

func auditRepositoryFile(relative string, encoded []byte) (Report, error) {
	report := newReport()
	if strings.EqualFold(filepath.Ext(relative), ".spl") {
		report.ScannedObjects = 1
		sourceRanges, err := auditSource(string(encoded), MaximumAuditFindings)
		if err != nil {
			return Report{}, err
		}
		for _, sourceRange := range sourceRanges {
			report.Findings = append(report.Findings, finding(
				"repository:"+filepath.ToSlash(relative),
				"repository/"+relative,
				sourceRange,
			))
		}
		return report, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(encoded))
	scanner.Buffer(make([]byte, 64<<10), maximumRepositoryFileBytes)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		candidate, column, ok := repositorySPLCandidate(line)
		if !ok {
			continue
		}
		report.ScannedObjects = 1
		sourceRanges, err := auditSource(
			candidate,
			MaximumAuditFindings-len(report.Findings),
		)
		if err != nil {
			return Report{}, err
		}
		for _, sourceRange := range sourceRanges {
			sourceRange.Start.Line = lineNumber
			sourceRange.End.Line = lineNumber
			sourceRange.Start.Column += column - 1
			sourceRange.End.Column += column - 1
			report.Findings = append(report.Findings, finding(
				"repository:"+filepath.ToSlash(relative),
				"repository/"+relative,
				sourceRange,
			))
		}
	}
	if err := scanner.Err(); err != nil {
		return Report{}, errors.New("scan SPL compatibility audit repository file")
	}
	return report, nil
}

func repositorySPLCandidate(line string) (string, int, bool) {
	lower := strings.ToLower(line)
	if !strings.Contains(lower, "| eval ") && !strings.Contains(lower, "| where ") {
		return "", 0, false
	}
	start := strings.Index(lower, "index=")
	if start < 0 {
		eval := strings.Index(lower, "| eval ")
		where := strings.Index(lower, "| where ")
		start = eval
		if start < 0 || (where >= 0 && where < start) {
			start = where
		}
	}
	if start < 0 {
		return "", 0, false
	}
	candidate := strings.TrimRightFunc(line[start:], func(character rune) bool {
		return unicode.IsSpace(character) || strings.ContainsRune("`\";,", character)
	})
	if candidate == "" {
		return "", 0, false
	}
	return candidate, utf8.RuneCountInString(line[:start]) + 1, true
}
