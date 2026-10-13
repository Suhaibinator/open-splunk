package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
)

const (
	maximumDeploymentHealthCABundleBytes = 1 << 20
	maximumDeploymentHealthBodyBytes     = 16
	deploymentHealthcheckTimeout         = 5 * time.Second
	deploymentHealthcheckIOTimeout       = 2 * time.Second
)

type deploymentHealthcheckOptions struct {
	URL        string
	CACertFile string
	ServerName string
}

// runDeploymentSubcommand handles isolated, shell-free container lifecycle
// tasks. It is called before normal server flag parsing, so each subcommand
// opens only the explicit resources it owns; no application database or application
// listener is opened.
func runDeploymentSubcommand(arguments []string) (bool, error) {
	return runDeploymentSubcommandWithOutput(arguments, os.Stdout)
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
		sameStableFileState,
	)
}
