package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

const (
	maximumDeploymentHealthCABundleBytes = 1 << 20
	maximumDeploymentHealthBodyBytes     = 16
	deploymentHealthcheckTimeout         = 5 * time.Second
	deploymentHealthcheckIOTimeout       = 2 * time.Second
)

var errProvisioningDestinationLinkCount = errors.New(
	"provisioned administrator token must have exactly one hard link",
)

type deploymentHealthcheckOptions struct {
	URL        string
	CACertFile string
	ServerName string
}

type provisioningTokenReadHooks = stablePathFileReadHooks

type provisioningPublishHooks struct {
	beforeRename func()
	afterRename  func() error
}

// runDeploymentSubcommand handles isolated, shell-free container lifecycle
// tasks. It is called before normal server flag parsing, so each subcommand
// opens only the explicit resources it owns; no SQLite database or application
// listener is opened.
func runDeploymentSubcommand(arguments []string) (bool, error) {
	if len(arguments) == 0 {
		return false, nil
	}
	switch arguments[0] {
	case "version":
		return true, runVersionSubcommand(arguments[1:], os.Stdout)
	case "healthcheck":
		return true, runDeploymentHealthcheckSubcommand(arguments[1:])
	case "migrate-clickhouse":
		return true, runDeploymentClickHouseMigrationSubcommand(arguments[1:])
	case "prepare-clickhouse-recovery-volume":
		return true, runPrepareClickHouseRecoveryVolumeSubcommand(arguments[1:])
	case "delete-deployment-recovery-archive":
		return true, runDeleteDeploymentRecoveryArchiveSubcommand(arguments[1:])
	case "backup-deployment-recovery-set":
		return true, runBackupDeploymentRecoverySetSubcommand(arguments[1:])
	case "verify-deployment-recovery-set":
		return true, runVerifyDeploymentRecoverySetSubcommand(arguments[1:])
	case "restore-deployment-recovery-set":
		return true, runRestoreDeploymentRecoverySetSubcommand(arguments[1:])
	case "reconcile-deployment-recovery-marker":
		return true, runDeploymentRecoveryMarkerReconcileSubcommand(arguments[1:])
	case "provision-administrator-token":
		return true, runProvisionAdministratorTokenSubcommand(arguments[1:])
	case "backup-control-plane":
		return true, runBackupControlPlaneSubcommand(arguments[1:])
	case "verify-control-plane-backup":
		return true, runVerifyControlPlaneBackupSubcommand(arguments[1:])
	case "restore-control-plane":
		return true, runRestoreControlPlaneSubcommand(arguments[1:])
	default:
		return false, nil
	}
}

func runVersionSubcommand(arguments []string, output io.Writer) error {
	if len(arguments) != 0 {
		return errors.New("version does not accept arguments")
	}
	identity, err := buildinfo.Current()
	if err != nil {
		return err
	}
	return buildinfo.WriteIdentity(output, identity)
}

func runDeploymentHealthcheckSubcommand(arguments []string) error {
	flags := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options deploymentHealthcheckOptions
	flags.StringVar(&options.URL, "url", "", "strict loopback HTTP or HTTPS liveness or readiness URL")
	flags.StringVar(&options.CACertFile, "ca-cert", "", "explicit certificate-only CA bundle for HTTPS")
	flags.StringVar(&options.ServerName, "server-name", "", "explicit TLS certificate name for HTTPS")
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("deployment healthcheck: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return errors.New("deployment healthcheck: positional arguments are not allowed")
	}
	return runDeploymentHealthcheck(options)
}

func runProvisionAdministratorTokenSubcommand(arguments []string) error {
	flags := flag.NewFlagSet("provision-administrator-token", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var source string
	var destination string
	flags.StringVar(&source, "source", "", "read-only generated administrator token source")
	flags.StringVar(&destination, "destination", "", "private runtime administrator token destination")
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("provision administrator token: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return errors.New("provision administrator token: positional arguments are not allowed")
	}
	return provisionAdministratorToken(source, destination)
}

func runDeploymentHealthcheck(options deploymentHealthcheckOptions) error {
	endpoint, err := validateDeploymentHealthURL(options.URL)
	if err != nil {
		return err
	}
	tlsConfig, err := deploymentHealthTLSConfig(endpoint, options)
	if err != nil {
		return err
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: deploymentHealthcheckIOTimeout}).DialContext,
		DisableCompression:     true,
		DisableKeepAlives:      true,
		TLSClientConfig:        tlsConfig,
		TLSHandshakeTimeout:    deploymentHealthcheckIOTimeout,
		ResponseHeaderTimeout:  deploymentHealthcheckIOTimeout,
		MaxResponseHeaderBytes: 8 << 10,
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("deployment healthcheck: redirects are not allowed")
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), deploymentHealthcheckTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("deployment healthcheck: create request: %w", err)
	}
	request.Header.Set("Accept", "text/plain")
	response, err := client.Do(request)
	if err != nil {
		if endpoint.Scheme == "https" {
			return fmt.Errorf("deployment healthcheck: TLS request failed: %w", err)
		}
		return fmt.Errorf("deployment healthcheck: request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"deployment healthcheck: status is %d, want %d",
			response.StatusCode,
			http.StatusOK,
		)
	}
	body, err := io.ReadAll(io.LimitReader(
		response.Body,
		maximumDeploymentHealthBodyBytes+1,
	))
	if err != nil {
		return fmt.Errorf("deployment healthcheck: read bounded response: %w", err)
	}
	if !bytes.Equal(body, []byte("ok\n")) {
		return errors.New("deployment healthcheck: response body is not exactly the expected health marker")
	}
	return nil
}

func validateDeploymentHealthURL(rawURL string) (*url.URL, error) {
	if rawURL == "" || rawURL != strings.TrimSpace(rawURL) ||
		strings.IndexByte(rawURL, 0) >= 0 {
		return nil, errors.New("deployment healthcheck: -url must be an exact HTTP or HTTPS loopback URL")
	}
	endpoint, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("deployment healthcheck: parse -url: %w", err)
	}
	if (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.Opaque != "" ||
		endpoint.Host == "" || endpoint.User != nil ||
		(endpoint.Path != "/healthz" && endpoint.Path != "/readyz") ||
		endpoint.RawPath != "" ||
		endpoint.ForceQuery || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" || endpoint.RawFragment != "" {
		return nil, errors.New(
			"deployment healthcheck: -url must be exactly an HTTP or HTTPS loopback /healthz or /readyz endpoint without credentials, query, or fragment",
		)
	}
	host := net.ParseIP(endpoint.Hostname())
	if host == nil || !host.IsLoopback() {
		return nil, errors.New(
			"deployment healthcheck: -url host must be a loopback IP literal",
		)
	}
	return endpoint, nil
}

func deploymentHealthTLSConfig(
	endpoint *url.URL,
	options deploymentHealthcheckOptions,
) (*tls.Config, error) {
	if endpoint.Scheme == "http" {
		if options.CACertFile != "" {
			return nil, errors.New("deployment healthcheck: -ca-cert is only valid with HTTPS")
		}
		if options.ServerName != "" {
			return nil, errors.New("deployment healthcheck: -server-name is only valid with HTTPS")
		}
		return nil, nil
	}
	return loadDeploymentHealthTLSConfig(options.CACertFile, options.ServerName)
}

func loadDeploymentHealthTLSConfig(
	caCertificateFile string,
	serverName string,
) (*tls.Config, error) {
	if caCertificateFile == "" || caCertificateFile != strings.TrimSpace(caCertificateFile) ||
		strings.IndexByte(caCertificateFile, 0) >= 0 {
		return nil, errors.New("deployment healthcheck: -ca-cert is required")
	}
	if err := validateDeploymentHealthServerName(serverName); err != nil {
		return nil, err
	}
	bundle, err := readBoundedDeploymentHealthCABundle(caCertificateFile)
	if err != nil {
		return nil, err
	}
	roots, err := parseClickHouseCABundle(bundle)
	if err != nil {
		return nil, fmt.Errorf(
			"deployment healthcheck: CA must contain only valid CERTIFICATE PEM blocks: %w",
			err,
		)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: serverName,
	}, nil
}

func validateDeploymentHealthServerName(serverName string) error {
	if err := validateExactDeploymentTLSServerName(serverName); err != nil {
		return fmt.Errorf("deployment healthcheck: %w", err)
	}
	return nil
}

func validateExactDeploymentTLSServerName(serverName string) error {
	if serverName == "" || serverName != strings.TrimSpace(serverName) ||
		!validExplicitTLSServerName(serverName) {
		return errors.New("-server-name must be an explicit valid bounded DNS name or IP address")
	}
	return nil
}

func readBoundedDeploymentHealthCABundle(path string) ([]byte, error) {
	return readBoundedCABundleFile(
		path,
		"deployment healthcheck",
		"CA certificate",
		maximumDeploymentHealthCABundleBytes,
		sameAdministratorTokenFileState,
	)
}

func provisionAdministratorToken(sourcePath, destinationPath string) error {
	return provisionAdministratorTokenWithHooks(
		sourcePath,
		destinationPath,
		provisioningPublishHooks{},
	)
}

func provisionAdministratorTokenWithHooks(
	sourcePath string,
	destinationPath string,
	hooks provisioningPublishHooks,
) error {
	sourceToken, err := readProvisioningAdministratorToken(sourcePath)
	if err != nil {
		return err
	}
	defer clear(sourceToken)

	destination, err := resolveProvisioningPath(
		destinationPath,
		"-destination",
	)
	if err != nil {
		return err
	}
	destinationDirectory := filepath.Dir(destination)
	destinationName := filepath.Base(destination)
	if destinationName == "." || destinationName == string(filepath.Separator) {
		return errors.New("provision administrator token: -destination must name a file")
	}

	before, err := os.Lstat(destinationDirectory)
	if err != nil {
		return fmt.Errorf("provision administrator token: inspect destination parent: %w", err)
	}
	if err := validateProvisioningDestinationDirectory(before, os.Geteuid()); err != nil {
		return err
	}

	// read-only, without following its final path component, then pinned by fd.
	directoryFD, err := unix.Open(
		destinationDirectory,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return fmt.Errorf("provision administrator token: open destination parent: %w", err)
	}

	// native file descriptor.
	directory := os.NewFile(uintptr(directoryFD), destinationDirectory)
	if directory == nil {
		_ = unix.Close(directoryFD)
		return errors.New("provision administrator token: invalid destination parent descriptor")
	}
	defer directory.Close()
	opened, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("provision administrator token: inspect open destination parent: %w", err)
	}
	if !os.SameFile(before, opened) {
		return errors.New("provision administrator token: destination parent changed while opening")
	}
	if err := validateProvisioningDestinationDirectory(opened, os.Geteuid()); err != nil {
		return err
	}
	if err := privatefs.ValidateNoExtendedACL(directory); err != nil {
		return fmt.Errorf("provision administrator token: destination parent ACL is unsafe: %w", err)
	}
	if err := ensureProvisioningDestinationDirectoryStable(
		destinationDirectory,
		opened,
	); err != nil {
		return err
	}

	err = provisionAdministratorTokenInDirectory(
		directoryFD,
		directory,
		destinationDirectory,
		destinationName,
		sourceToken,
		hooks,
	)
	if err != nil {
		return err
	}
	if err := privatefs.ValidateNoExtendedACL(directory); err != nil {
		return fmt.Errorf("provision administrator token: destination parent ACL became unsafe: %w", err)
	}
	return ensureProvisioningDestinationDirectoryStable(
		destinationDirectory,
		opened,
	)
}

func readProvisioningAdministratorToken(path string) ([]byte, error) {
	return readProvisioningAdministratorTokenWithHooks(
		path,
		provisioningTokenReadHooks{},
	)
}

func readProvisioningAdministratorTokenWithHooks(
	path string,
	hooks provisioningTokenReadHooks,
) ([]byte, error) {
	absolutePath, err := resolveProvisioningPath(path, "-source")
	if err != nil {
		return nil, err
	}
	contents, err := readStablePathFile(stablePathFileReadConfig{
		path:             absolutePath,
		maximumReadBytes: maximumAdministratorTokenFileBytes,
		hooks:            hooks,
		validateBefore:   validateProvisioningTokenSource,
		validateOpen: func(_ *os.File, info os.FileInfo) error {
			if err := validateProvisioningTokenSource(info); err != nil {
				return err
			}
			if info.Size() < auth.MinimumBrowserBearerTokenBytes ||
				info.Size() > maximumAdministratorTokenFileBytes {
				return errors.New(
					"provision administrator token: source has an invalid size",
				)
			}
			return nil
		},
		validateAfterPath: validateProvisioningTokenSource,
		sameState:         sameAdministratorTokenFileState,
		messages: stablePathFileReadMessages{
			inspectPath:         "provision administrator token: inspect source",
			openPath:            "provision administrator token: open source",
			invalidDescriptor:   "provision administrator token: invalid source descriptor",
			inspectOpen:         "provision administrator token: inspect open source",
			changedWhileOpening: "provision administrator token: source changed while opening",
			read:                "provision administrator token: read source",
			overflow:            "provision administrator token: source changed while reading",
			changedWhileReading: "provision administrator token: source changed while reading",
			reinspectOpen:       "provision administrator token: reinspect open source",
			reinspectPath:       "provision administrator token: reinspect source",
			close:               "provision administrator token: close source",
		},
	})
	if err != nil {
		return nil, err
	}
	defer clear(contents)

	token := stripAdministratorTokenTerminator(contents)
	if err := auth.ValidateBrowserBearerToken(token); err != nil {
		return nil, errors.New("provision administrator token: source contains an invalid bearer token")
	}
	return append([]byte(nil), token...), nil
}

func resolveProvisioningPath(path, flagName string) (string, error) {
	if path == "" || strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("provision administrator token: %s is required", flagName)
	}
	if strings.IndexByte(path, 0) >= 0 {
		return "", fmt.Errorf("provision administrator token: %s contains a NUL byte", flagName)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("provision administrator token: resolve %s: %w", flagName, err)
	}
	return absolutePath, nil
}

func validateProvisioningTokenSource(info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("provision administrator token: source must be a regular file")
	}
	mode := info.Mode()
	if mode.Perm() != 0o444 && mode.Perm() != 0o644 {
		return errors.New("provision administrator token: source permissions must be exactly 0444 or 0644")
	}
	if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("provision administrator token: source must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("provision administrator token: source ownership is unavailable")
	}
	if stat.Nlink != 1 {
		return errors.New("provision administrator token: source must have exactly one hard link")
	}
	return nil
}

func validateProvisioningDestinationDirectory(
	info os.FileInfo,
	effectiveUID int,
) error {
	if info == nil || !info.IsDir() {
		return errors.New("provision administrator token: destination parent must be an existing directory")
	}
	mode := info.Mode()
	if mode.Perm() != 0o700 {
		return errors.New("provision administrator token: destination parent permissions must be exactly 0700")
	}
	if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("provision administrator token: destination parent must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("provision administrator token: destination parent ownership is unavailable")
	}
	if effectiveUID < 0 || int64(stat.Uid) != int64(effectiveUID) {
		return errors.New("provision administrator token: destination parent must be owned by the server user")
	}
	return nil
}

func ensureProvisioningDestinationDirectoryStable(
	path string,
	opened os.FileInfo,
) error {
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("provision administrator token: reinspect destination parent: %w", err)
	}
	if !os.SameFile(opened, current) || opened.Mode() != current.Mode() {
		return errors.New("provision administrator token: destination parent changed during provisioning")
	}
	if err := validateProvisioningDestinationDirectory(current, os.Geteuid()); err != nil {
		return err
	}
	return nil
}

func provisionAdministratorTokenInDirectory(
	directoryFD int,
	directory *os.File,
	directoryPath string,
	destinationName string,
	sourceToken []byte,
	hooks provisioningPublishHooks,
) error {
	existing, err := readProvisionedAdministratorToken(
		directoryFD,
		directoryPath,
		destinationName,
	)
	if err == nil {
		defer clear(existing)
		return compareProvisionedAdministratorTokens(sourceToken, existing)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	published, err := createProvisionedAdministratorToken(
		directoryFD,
		directory,
		destinationName,
		sourceToken,
		hooks,
	)
	if err != nil {
		return err
	}
	if !published {
		existing, readErr := readProvisionedAdministratorToken(
			directoryFD,
			directoryPath,
			destinationName,
		)
		if readErr != nil {
			return readErr
		}
		defer clear(existing)
		return compareProvisionedAdministratorTokens(sourceToken, existing)
	}

	created, err := readProvisionedAdministratorToken(
		directoryFD,
		directoryPath,
		destinationName,
	)
	if err != nil {
		return fmt.Errorf("provision administrator token: verify published destination: %w", err)
	}
	defer clear(created)
	return compareProvisionedAdministratorTokens(sourceToken, created)
}

func createProvisionedAdministratorToken(
	directoryFD int,
	directory *os.File,
	destinationName string,
	token []byte,
	hooks provisioningPublishHooks,
) (bool, error) {
	temporaryName, temporary, err := createProvisioningTemporaryFile(directoryFD)
	if err != nil {
		return false, err
	}
	temporaryExists := true
	defer func() {
		if temporaryExists {
			_ = unix.Unlinkat(directoryFD, temporaryName, 0)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("provision administrator token: secure temporary destination: %w", err)
	}
	if err := writeCompleteToken(temporary, token); err != nil {
		_ = temporary.Close()
		return false, err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("provision administrator token: fsync temporary destination: %w", err)
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("provision administrator token: inspect temporary destination: %w", err)
	}
	if err := validateProvisionedAdministratorTokenFile(temporaryInfo, os.Geteuid()); err != nil {
		_ = temporary.Close()
		return false, err
	}
	if temporaryInfo.Size() != int64(len(token)) {
		_ = temporary.Close()
		return false, errors.New("provision administrator token: temporary destination has an invalid size")
	}
	if err := privatefs.ValidateNoExtendedACL(temporary); err != nil {
		_ = temporary.Close()
		return false, err
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("provision administrator token: close temporary destination: %w", err)
	}

	if hooks.beforeRename != nil {
		hooks.beforeRename()
	}
	if err := renameProvisioningTokenNoReplace(
		directoryFD,
		temporaryName,
		destinationName,
	); err != nil {
		if errors.Is(err, unix.EEXIST) {
			if unlinkErr := unix.Unlinkat(directoryFD, temporaryName, 0); unlinkErr != nil {
				return false, fmt.Errorf("provision administrator token: clean competing temporary destination: %w", unlinkErr)
			}
			temporaryExists = false
			if syncErr := directory.Sync(); syncErr != nil {
				return false, fmt.Errorf("provision administrator token: fsync destination parent after contention: %w", syncErr)
			}
			return false, nil
		}
		return false, fmt.Errorf("provision administrator token: publish destination without overwrite: %w", err)
	}
	temporaryExists = false
	if hooks.afterRename != nil {
		if err := hooks.afterRename(); err != nil {
			return false, err
		}
	}
	if err := directory.Sync(); err != nil {
		return false, fmt.Errorf("provision administrator token: fsync destination parent: %w", err)
	}
	return true, nil
}

func createProvisioningTemporaryFile(directoryFD int) (string, *os.File, error) {
	for range 16 {
		var randomBytes [16]byte
		if _, err := io.ReadFull(rand.Reader, randomBytes[:]); err != nil {
			return "", nil, fmt.Errorf("provision administrator token: generate temporary name: %w", err)
		}
		name := ".administrator-token.tmp-" + hex.EncodeToString(randomBytes[:])
		clear(randomBytes[:])
		fd, err := unix.Openat(
			directoryFD,
			name,
			unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0o600,
		)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("provision administrator token: create temporary destination: %w", err)
		}

		// file descriptor.
		file := os.NewFile(uintptr(fd), name)
		if file == nil {
			_ = unix.Close(fd)
			_ = unix.Unlinkat(directoryFD, name, 0)
			return "", nil, errors.New("provision administrator token: invalid temporary destination descriptor")
		}
		return name, file, nil
	}
	return "", nil, errors.New("provision administrator token: could not reserve a temporary destination name")
}

func writeCompleteToken(file *os.File, token []byte) error {
	remaining := token
	for len(remaining) != 0 {
		written, err := file.Write(remaining)
		if err != nil {
			return fmt.Errorf("provision administrator token: write temporary destination: %w", err)
		}
		if written <= 0 || written > len(remaining) {
			return errors.New("provision administrator token: incomplete temporary destination write")
		}
		remaining = remaining[written:]
	}
	return nil
}

func readProvisionedAdministratorToken(
	directoryFD int,
	directoryPath string,
	destinationName string,
) ([]byte, error) {
	fd, err := unix.Openat(
		directoryFD,
		destinationName,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("provision administrator token: open existing destination: %w", err)
	}

	// file descriptor.
	file := os.NewFile(uintptr(fd), destinationName)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("provision administrator token: invalid destination descriptor")
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("provision administrator token: inspect existing destination: %w", err)
	}
	if err := validateProvisionedAdministratorTokenFile(opened, os.Geteuid()); err != nil {
		return nil, err
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return nil, err
	}
	if opened.Size() < auth.MinimumBrowserBearerTokenBytes ||
		opened.Size() > maximumAdministratorTokenFileBytes {
		return nil, errors.New("provision administrator token: existing destination has an invalid size")
	}
	contents, err := io.ReadAll(io.LimitReader(
		file,
		maximumAdministratorTokenFileBytes+1,
	))
	if err != nil {
		return nil, fmt.Errorf("provision administrator token: read existing destination: %w", err)
	}
	defer clear(contents)
	if len(contents) > maximumAdministratorTokenFileBytes ||
		int64(len(contents)) != opened.Size() {
		return nil, errors.New("provision administrator token: existing destination changed while reading")
	}
	afterOpen, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("provision administrator token: reinspect open destination: %w", err)
	}
	if !sameAdministratorTokenFileState(opened, afterOpen) {
		return nil, errors.New("provision administrator token: existing destination changed while reading")
	}
	if err := validateProvisionedAdministratorTokenFile(afterOpen, os.Geteuid()); err != nil {
		return nil, err
	}
	if err := privatefs.ValidateNoExtendedACL(file); err != nil {
		return nil, err
	}
	pathInfo, err := os.Lstat(filepath.Join(directoryPath, destinationName))
	if err != nil {
		return nil, fmt.Errorf("provision administrator token: reinspect destination path: %w", err)
	}
	if !sameAdministratorTokenFileState(afterOpen, pathInfo) {
		return nil, errors.New("provision administrator token: existing destination changed while reading")
	}
	if err := validateProvisionedAdministratorTokenFile(pathInfo, os.Geteuid()); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("provision administrator token: close existing destination: %w", err)
	}
	closed = true

	token := stripAdministratorTokenTerminator(contents)
	if err := auth.ValidateBrowserBearerToken(token); err != nil {
		return nil, errors.New("provision administrator token: existing destination contains an invalid bearer token")
	}
	return append([]byte(nil), token...), nil
}

func validateProvisionedAdministratorTokenFile(
	info os.FileInfo,
	effectiveUID int,
) error {
	if info == nil || !info.Mode().IsRegular() {
		return errors.New("provision administrator token: destination must be a regular file")
	}
	mode := info.Mode()
	if mode.Perm() != 0o600 {
		return errors.New("provision administrator token: destination permissions must be exactly 0600")
	}
	if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("provision administrator token: destination must not have special permission bits")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return errors.New("provision administrator token: destination ownership is unavailable")
	}
	if effectiveUID < 0 || int64(stat.Uid) != int64(effectiveUID) {
		return errors.New("provision administrator token: destination must be owned by the server user")
	}
	if stat.Nlink != 1 {
		return errProvisioningDestinationLinkCount
	}
	return nil
}

func compareProvisionedAdministratorTokens(source, destination []byte) error {
	sourceDigest := sha256.Sum256(source)
	destinationDigest := sha256.Sum256(destination)
	matched := subtle.ConstantTimeCompare(
		sourceDigest[:],
		destinationDigest[:],
	)
	clear(sourceDigest[:])
	clear(destinationDigest[:])
	if matched != 1 {
		return errors.New("provision administrator token: destination contains a different valid bearer token")
	}
	return nil
}
