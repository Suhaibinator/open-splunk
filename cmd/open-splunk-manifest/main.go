// Command open-splunk-manifest generates the deterministic release-asset
// manifest after the protobuf and Next.js build steps have completed.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/Suhaibinator/open-splunk/internal/buildassets"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("open-splunk-manifest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	rootFlag := flags.String("root", ".", "repository root")
	revision := flags.String("source-revision", "", "full lowercase Git source revision")
	productVersion := flags.String("product-version", "", "optional canonical 0.MINOR.PATCH product version")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse manifest flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if stdout == nil {
		return errors.New("manifest output writer is required")
	}

	root, err := filepath.Abs(*rootFlag)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("inspect repository root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("repository root %q is not a directory", root)
	}
	repository := os.DirFS(root)
	manifest, err := buildassets.GenerateRelease(repository, *revision, *productVersion)
	if err != nil {
		return err
	}
	encoded, err := buildassets.Marshal(manifest)
	if err != nil {
		return err
	}
	outputDirectory := filepath.Join(root, "out")
	outputPath := filepath.Join(outputDirectory, buildassets.ManifestFilename)
	if writeErr := writeAtomic(outputPath, encoded); writeErr != nil {
		return writeErr
	}
	_, err = fmt.Fprintf(
		stdout,
		"wrote %s (revision %s, UI %s, %d files)\n",
		outputPath,
		manifest.SourceRevision,
		manifest.UI.SHA256,
		manifest.UI.FileCount,
	)
	if err != nil {
		return fmt.Errorf("report generated build manifest: %w", err)
	}
	return nil
}

func writeAtomic(destination string, contents []byte) (returnedErr error) {
	directory := filepath.Dir(destination)
	temporary, err := os.CreateTemp(directory, ".asset-manifest-")
	if err != nil {
		return fmt.Errorf("create temporary build manifest: %w", err)
	}
	temporaryPath := temporary.Name()
	closed := false
	defer func() {
		if !closed {
			if closeErr := temporary.Close(); closeErr != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("close temporary build manifest: %w", closeErr))
			}
		}
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("remove temporary build manifest: %w", removeErr))
		}
	}()
	if chmodErr := temporary.Chmod(0o644); chmodErr != nil {
		return fmt.Errorf("set build manifest permissions: %w", chmodErr)
	}
	if _, writeErr := temporary.Write(contents); writeErr != nil {
		return fmt.Errorf("write build manifest: %w", writeErr)
	}
	if syncErr := temporary.Sync(); syncErr != nil {
		return fmt.Errorf("sync build manifest: %w", syncErr)
	}
	if closeErr := temporary.Close(); closeErr != nil {
		return fmt.Errorf("close build manifest: %w", closeErr)
	}
	closed = true
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("publish build manifest: %w", err)
	}
	directoryHandle, openErr := os.Open(directory)
	if openErr != nil {
		return fmt.Errorf("open build manifest directory for sync: %w", openErr)
	}
	if syncErr := directoryHandle.Sync(); syncErr != nil {
		_ = directoryHandle.Close()
		return fmt.Errorf("sync build manifest directory: %w", syncErr)
	}
	if closeErr := directoryHandle.Close(); closeErr != nil {
		return fmt.Errorf("close build manifest directory: %w", closeErr)
	}
	return nil
}
