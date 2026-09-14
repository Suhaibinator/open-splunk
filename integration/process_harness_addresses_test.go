//go:build !windows

package integration_test

import (
	"context"
	"errors"
	"net"
	"testing"
)

type reusableLoopbackAllocator struct {
	active        map[string]bool
	closeErrors   map[string]error
	closeCalls    int
	failListenAt  int
	listenError   error
	listenCalls   int
	maximumActive int
}

func newReusableLoopbackAllocator() *reusableLoopbackAllocator {
	return &reusableLoopbackAllocator{
		active:      make(map[string]bool),
		closeErrors: make(map[string]error),
	}
}

func (allocator *reusableLoopbackAllocator) listen(
	context.Context,
	string,
	string,
) (net.Listener, error) {
	allocator.listenCalls++
	if allocator.listenCalls == allocator.failListenAt {
		return nil, allocator.listenError
	}
	address := "127.0.0.1:35083"
	if allocator.active[address] {
		address = "127.0.0.1:35084"
	}
	allocator.active[address] = true
	allocator.maximumActive = max(allocator.maximumActive, len(allocator.active))
	return &trackedLoopbackListener{
		address: address,
		close: func() error {
			allocator.closeCalls++
			delete(allocator.active, address)
			return allocator.closeErrors[address]
		},
	}, nil
}

type trackedLoopbackListener struct {
	address string
	close   func() error
}

func (*trackedLoopbackListener) Accept() (net.Conn, error) {
	return nil, errors.New("test listener does not accept connections")
}

func (listener *trackedLoopbackListener) Addr() net.Addr {
	return trackedLoopbackAddress(listener.address)
}

func (listener *trackedLoopbackListener) Close() error {
	return listener.close()
}

type trackedLoopbackAddress string

func (trackedLoopbackAddress) Network() string        { return "tcp" }
func (address trackedLoopbackAddress) String() string { return string(address) }

func TestUnusedLoopbackAddressPairReservesBothBeforeRelease(t *testing.T) {
	t.Parallel()
	allocator := newReusableLoopbackAllocator()
	httpAddress, collectorAddress, err := unusedLoopbackAddressPairWith(
		t.Context(),
		allocator.listen,
	)
	if err != nil {
		t.Fatal(err)
	}
	if httpAddress == collectorAddress {
		t.Fatalf("paired addresses reused released port %q", httpAddress)
	}
	if allocator.maximumActive != 2 {
		t.Fatalf("maximum overlapping reservations = %d, want 2", allocator.maximumActive)
	}
	if len(allocator.active) != 0 || allocator.closeCalls != 2 {
		t.Fatalf("released reservations = active %v closes %d", allocator.active, allocator.closeCalls)
	}
}

func TestUnusedLoopbackAddressPairCleansUpAfterSecondBindFailure(t *testing.T) {
	t.Parallel()
	allocator := newReusableLoopbackAllocator()
	allocator.failListenAt = 2
	bindErr := errors.New("injected loopback bind failure")
	closeErr := errors.New("injected cleanup failure")
	allocator.listenError = bindErr
	allocator.closeErrors["127.0.0.1:35083"] = closeErr
	httpAddress, collectorAddress, err := unusedLoopbackAddressPairWith(
		t.Context(),
		allocator.listen,
	)
	if !errors.Is(err, bindErr) || !errors.Is(err, closeErr) ||
		httpAddress != "" || collectorAddress != "" {
		t.Fatalf("second bind = (%q, %q, %v)", httpAddress, collectorAddress, err)
	}
	if len(allocator.active) != 0 || allocator.closeCalls != 1 {
		t.Fatalf("failed-bind cleanup = active %v closes %d", allocator.active, allocator.closeCalls)
	}
}

func TestUnusedLoopbackAddressPairJoinsCloseErrorsAndReleasesBoth(t *testing.T) {
	t.Parallel()
	firstCloseErr := errors.New("injected first close failure")
	secondCloseErr := errors.New("injected second close failure")
	allocator := newReusableLoopbackAllocator()
	allocator.closeErrors["127.0.0.1:35083"] = firstCloseErr
	allocator.closeErrors["127.0.0.1:35084"] = secondCloseErr
	httpAddress, collectorAddress, err := unusedLoopbackAddressPairWith(
		t.Context(),
		allocator.listen,
	)
	if !errors.Is(err, firstCloseErr) || !errors.Is(err, secondCloseErr) {
		t.Fatalf("joined close error = %v", err)
	}
	if httpAddress != "" || collectorAddress != "" {
		t.Fatalf("addresses returned after close failure = (%q, %q)", httpAddress, collectorAddress)
	}
	if len(allocator.active) != 0 || allocator.closeCalls != 2 {
		t.Fatalf("close-error cleanup = active %v closes %d", allocator.active, allocator.closeCalls)
	}
}
