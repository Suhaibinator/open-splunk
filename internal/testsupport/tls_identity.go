package testsupport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ServerTLSIdentity is a short-lived self-signed server identity for local
// integration tests. RootCAs trusts only the generated certificate.
type ServerTLSIdentity struct {
	CertificateFile string
	PrivateKeyFile  string
	RootCAs         *x509.CertPool
}

// WriteServerTLSIdentity writes an ECDSA P-256 certificate and PKCS#8 private key
// whose SANs cover the supplied DNS names and IP addresses.
func WriteServerTLSIdentity(
	directory string,
	hosts ...string,
) (*ServerTLSIdentity, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, errors.New("server TLS identity directory is required")
	}
	if len(hosts) == 0 {
		return nil, errors.New("at least one server TLS identity host is required")
	}
	dnsNames := make([]string, 0, len(hosts))
	ipAddresses := make([]net.IP, 0, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, errors.New("server TLS identity hosts must be non-empty")
		}
		if ip := net.ParseIP(host); ip != nil {
			ipAddresses = append(ipAddresses, ip)
		} else {
			dnsNames = append(dnsNames, host)
		}
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create server TLS identity directory: %w", err)
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate server TLS private key: %w", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, fmt.Errorf("generate server TLS certificate serial: %w", err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: "Open Splunk integration test",
		},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddresses,
	}
	certificateDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		return nil, fmt.Errorf("create server TLS certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return nil, fmt.Errorf("parse generated server TLS certificate: %w", err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("marshal server TLS private key: %w", err)
	}
	certificateFile := filepath.Join(directory, "server.crt")
	privateKeyFile := filepath.Join(directory, "server.key")
	if err := os.WriteFile(certificateFile, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certificateDER,
	}), 0o600); err != nil {
		return nil, fmt.Errorf("write server TLS certificate: %w", err)
	}
	if err := os.WriteFile(privateKeyFile, pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privateKeyDER,
	}), 0o600); err != nil {
		return nil, fmt.Errorf("write server TLS private key: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &ServerTLSIdentity{
		CertificateFile: certificateFile,
		PrivateKeyFile:  privateKeyFile,
		RootCAs:         roots,
	}, nil
}
