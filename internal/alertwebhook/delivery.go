package alertwebhook

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Deliverer struct {
	resolver          Resolver
	dialer            Dialer
	clientFactory     ClientFactory
	clock             func() time.Time
	timeout           time.Duration
	responseReadLimit int64
	tlsRootCAs        *x509.CertPool
	destinationPolicy DestinationPolicy
}

func NewDeliverer(options DeliveryOptions) (*Deliverer, error) {
	destinationPolicy := DestinationPolicy{
		PrivateHostAllowlist: append([]string(nil), options.DestinationPolicy.PrivateHostAllowlist...),
	}
	if err := ValidateDestinationPolicy(destinationPolicy); err != nil {
		return nil, err
	}
	resolver := options.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	dialer := options.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: DefaultDeliveryTimeout}
	}
	clientFactory := options.ClientFactory
	if clientFactory == nil {
		clientFactory = defaultClientFactory
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultDeliveryTimeout
	}
	if timeout < 0 || timeout > DefaultDeliveryTimeout {
		return nil, fmt.Errorf("%w: delivery timeout must be positive and at most %s", ErrInvalidArgument, DefaultDeliveryTimeout)
	}
	readLimit := options.ResponseReadLimit
	if readLimit == 0 {
		readLimit = DefaultResponseReadLimit
	}
	if readLimit < 0 || readLimit > MaximumResponseReadLimit {
		return nil, fmt.Errorf("%w: response read limit must be positive and at most %d bytes", ErrInvalidArgument, MaximumResponseReadLimit)
	}
	var tlsRootCAs *x509.CertPool
	if options.TLSRootCAs != nil {
		tlsRootCAs = options.TLSRootCAs.Clone()
	}
	return &Deliverer{
		resolver: resolver, dialer: dialer, clientFactory: clientFactory,
		clock: clock, timeout: timeout, responseReadLimit: readLimit,
		tlsRootCAs:        tlsRootCAs,
		destinationPolicy: destinationPolicy,
	}, nil
}

func (deliverer *Deliverer) Deliver(ctx context.Context, rawURL string, signed SignedPayload) (DeliveryResult, error) {
	attemptedAt := deliverer.clock().UTC()
	deliveryContext, cancel := context.WithTimeout(ctx, deliverer.timeout)
	defer cancel()
	if len(signed.Body) > MaximumPayloadBytes {
		return deliveryFailure(attemptedAt, &DeliveryError{Category: DeliveryDestinationRejected})
	}
	destination, err := ResolveDestination(deliveryContext, deliverer.resolver, rawURL, deliverer.destinationPolicy)
	if err != nil {
		if errors.Is(err, ErrInvalidArgument) {
			err = &DeliveryError{Category: DeliveryDestinationRejected}
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			err = classifyTransportError(err)
		}
		return deliveryFailure(attemptedAt, err)
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   deliverer.timeout,
		ResponseHeaderTimeout: deliverer.timeout,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    deliverer.tlsRootCAs,
			ServerName: destination.Hostname,
		},
	}
	transport.DialContext = pinnedDialContext(deliverer.dialer, destination)
	defer transport.CloseIdleConnections()

	request, err := http.NewRequestWithContext(deliveryContext, http.MethodPost, destination.URL.String(), bytes.NewReader(signed.Body))
	if err != nil {
		return deliveryFailure(attemptedAt, &DeliveryError{Category: DeliveryDestinationRejected})
	}
	request.Header.Set("Content-Type", "application/json")
	for _, name := range []string{HeaderAlertID, HeaderDeliveryID, HeaderTimestamp, HeaderSignature} {
		if value := signed.Headers[name]; value != "" {
			request.Header.Set(name, value)
		}
	}
	response, err := deliverer.clientFactory(transport, deliverer.timeout).Do(request)
	if err != nil {
		return deliveryFailure(attemptedAt, classifyTransportError(err))
	}
	if response == nil || response.Body == nil {
		return deliveryFailure(attemptedAt, &DeliveryError{Category: DeliveryTransportFailure})
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, deliverer.responseReadLimit))
	// Stop the request before closing a response body that may still be
	// producing an unbounded stream. This makes the body limit effective even
	// for peers that never send EOF.
	cancel()
	_ = response.Body.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return deliveryFailure(attemptedAt, classifyTransportError(readErr))
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		result := DeliveryResult{Category: DeliveryHTTPFailure, StatusCode: response.StatusCode, AttemptedAt: attemptedAt}
		return result, &DeliveryError{Category: DeliveryHTTPFailure}
	}
	return DeliveryResult{Category: DeliverySucceeded, StatusCode: response.StatusCode, Delivered: true, AttemptedAt: attemptedAt}, nil
}

func defaultClientFactory(transport http.RoundTripper, timeout time.Duration) HTTPDoer {
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func pinnedDialContext(dialer Dialer, destination ResolvedDestination) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.New("alert webhook: transport requested malformed address")
		}
		canonical, err := canonicalHostname(host)
		validNetwork := network == "tcp" || network == "tcp4" || network == "tcp6"
		if err != nil || canonical != destination.Hostname || port != destination.Port || !validNetwork {
			return nil, errors.New("alert webhook: transport attempted an unvalidated destination")
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(destination.Address.String(), destination.Port))
	}
}

func classifyTransportError(err error) error {
	if errors.Is(err, context.Canceled) {
		return &DeliveryError{Category: DeliveryCanceled}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &DeliveryError{Category: DeliveryTimeout}
	}
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Timeout() {
		return &DeliveryError{Category: DeliveryTimeout}
	}
	if _, ok := errors.AsType[tls.RecordHeaderError](err); ok {
		return &DeliveryError{Category: DeliveryTLSFailure}
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return &DeliveryError{Category: DeliveryTLSFailure}
	}
	var unknownAuthorityError x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	var invalidCertificateError x509.CertificateInvalidError
	if errors.As(err, &unknownAuthorityError) || errors.As(err, &hostnameError) || errors.As(err, &invalidCertificateError) {
		return &DeliveryError{Category: DeliveryTLSFailure}
	}
	return &DeliveryError{Category: DeliveryTransportFailure}
}

func deliveryFailure(attemptedAt time.Time, err error) (DeliveryResult, error) {
	var deliveryError *DeliveryError
	if !errors.As(err, &deliveryError) {
		deliveryError = &DeliveryError{Category: DeliveryTransportFailure}
	}
	return DeliveryResult{Category: deliveryError.Category, AttemptedAt: attemptedAt}, deliveryError
}
