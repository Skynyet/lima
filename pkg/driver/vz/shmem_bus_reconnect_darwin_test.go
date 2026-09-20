//go:build darwin && !no_vz

// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package vz

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lima-vm/lima/v2/pkg/driver/vz/shmembus/dataplane"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type readEvt struct {
	data []byte
	eof  bool
}

func startCoordinator(t *testing.T, bin, sock string) (*exec.Cmd, *lockedBuffer) {
	t.Helper()
	cmd := exec.Command(bin, "--socket", sock)
	log := &lockedBuffer{}
	cmd.Stderr = log
	cmd.Stdout = log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		if st, err := os.Lstat(sock); err == nil && st.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("coordinator socket %s never appeared; log:\n%s", sock, log.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cmd, log
}

func waitFor(t *testing.T, log *lockedBuffer, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(log.String(), want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in coordinator log:\n%s", want, log.String())
}

// edgeDown reports whether the VZ end detects that its Darwin datagram peer
// has closed.

func edgeDown(vzEnd *os.File) bool {
	probe := make([]byte, 16)
	_, err := syscall.Write(int(vzEnd.Fd()), probe)
	if err == nil {
		return false
	}
	return err == syscall.ECONNRESET || err == syscall.ECONNREFUSED || err == syscall.EPIPE
}

// TestDialShmemBusSurvivesCoordinatorRestart kills the coordinator under a
// live participant and requires the supervisor to rejoin a fresh coordinator
// on the same path, keeping the VZ edge open, and to move real frames on the
// new bus. Needs LIMA_BUS_COORDINATOR_BIN (a standalone build of the embedded
// coordinator) and LIMA_BUS_TESTDIR; skipped without them.
func TestDialShmemBusSurvivesCoordinatorRestart(t *testing.T) {
	bin := os.Getenv("LIMA_BUS_COORDINATOR_BIN")
	base := os.Getenv("LIMA_BUS_TESTDIR")
	if bin == "" || base == "" {
		t.Skip("set LIMA_BUS_COORDINATOR_BIN and LIMA_BUS_TESTDIR to run")
	}
	dir, err := os.MkdirTemp(base, "bus-reconnect-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "bus.sock")

	cmd1, log1 := startCoordinator(t, bin, sock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vzEnd, err := DialShmemBus(ctx, sock, "02:00:00:00:00:0a")
	if err != nil {
		t.Fatalf("DialShmemBus: %v", err)
	}
	defer vzEnd.Close()
	events := make(chan readEvt, 16)
	go func() {
		for {
			buf := make([]byte, 2048)
			n, rerr := vzEnd.Read(buf)
			if rerr != nil {
				events <- readEvt{eof: true}
				return
			}
			events <- readEvt{data: buf[:n]}
		}
	}()
	waitFor(t, log1, "live at epoch", 5*time.Second)

	if err := cmd1.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = cmd1.Process.Wait()

	// The VZ edge must not observe shutdown while the daemon is gone: sends on
	// it keep succeeding (frames queue or drop, exactly like a congested port).
	for i := 0; i < 4; i++ {
		if edgeDown(vzEnd) {
			t.Fatal("VZ edge died during coordinator restart")
		}
		time.Sleep(150 * time.Millisecond)
	}

	cmd2, log2 := startCoordinator(t, bin, sock)
	defer func() {
		_ = cmd2.Process.Kill()
		_, _ = cmd2.Process.Wait()
	}()
	waitFor(t, log2, "live at epoch", 10*time.Second)

	// A second participant joins the new bus; a frame it emits for the first
	// MAC must cross the rejoined participant and reach the VZ edge.
	edge, inject, err := createSockPair()
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()
	p2, err := dataplane.Open(dataplane.Config{
		Control:  sock,
		EdgeFD:   int(edge.Fd()),
		MAC:      0x02000000000b,
		MaxFrame: shmemBusMaxFrame,
	})
	if err != nil {
		t.Fatalf("second participant join: %v", err)
	}
	go func() { _, _ = p2.Run() }()
	defer p2.Close()
	waitFor(t, log2, "port 1 generation", 5*time.Second)

	frame := make([]byte, 64)
	copy(frame[0:6], []byte{0x02, 0, 0, 0, 0, 0x0a})
	copy(frame[6:12], []byte{0x02, 0, 0, 0, 0, 0x0b})
	frame[12], frame[13] = 0x08, 0x00
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := syscall.Write(int(inject.Fd()), frame); err != nil {
			t.Fatalf("inject: %v", err)
		}
		select {
		case evt := <-events:
			if evt.eof {
				t.Fatal("VZ edge closed during frame phase")
			}
			if len(evt.data) == len(frame) && bytes.Equal(evt.data, frame) {
				return
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("no frame crossed the rejoined bus; coordinator log:\n%s", log2.String())
}

// blackhole accepts control connections and never answers them: the join
// handshake blocks exactly as against a coordinator that accepted and wedged
// before READY.
type blackhole struct {
	net.Listener
	held     []net.Conn
	accepted chan struct{}
	once     sync.Once
	mu       sync.Mutex
}

func startBlackhole(t *testing.T, sock string) *blackhole {
	t.Helper()
	_ = os.Remove(sock)
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("blackhole listen: %v", err)
	}
	b := &blackhole{Listener: l, accepted: make(chan struct{})}
	go func() {
		for {
			c, err := b.Listener.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.held = append(b.held, c)
			b.mu.Unlock()
			b.once.Do(func() { close(b.accepted) })
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		b.mu.Lock()
		for _, c := range b.held {
			_ = c.Close()
		}
		b.mu.Unlock()
	})
	return b
}

// TestDialShmemBusFirstJoinCancellable requires that a VM start blocked in the
// join handshake against a wedged acceptor is aborted by context cancellation
// instead of hanging.
func TestDialShmemBusFirstJoinCancellable(t *testing.T) {
	base := os.Getenv("LIMA_BUS_TESTDIR")
	if base == "" {
		t.Skip("set LIMA_BUS_TESTDIR to run")
	}
	dir, err := os.MkdirTemp(base, "bus-stuck-join-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "bus.sock")
	startBlackhole(t, sock)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		file *os.File
		err  error
	}
	done := make(chan result, 1)
	go func() {
		f, err := DialShmemBus(ctx, sock, "02:00:00:00:00:0a")
		done <- result{f, err}
	}()

	time.Sleep(300 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("DialShmemBus returned before cancellation: %v", r.err)
	default:
	}
	cancel()
	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("DialShmemBus succeeded against a wedged acceptor")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("context cancellation did not abort the stuck join")
	}
}

// TestSuperviseBusStuckRejoinCancellable requires that when a rejoin attempt is
// blocked inside the handshake of an acceptor that never answers, VM-context
// cancellation still completes teardown: every descriptor of the peer end of
// the VZ socketpair is closed, which the VZ end observes as ECONNREFUSED.
func TestSuperviseBusStuckRejoinCancellable(t *testing.T) {
	bin := os.Getenv("LIMA_BUS_COORDINATOR_BIN")
	base := os.Getenv("LIMA_BUS_TESTDIR")
	if bin == "" || base == "" {
		t.Skip("set LIMA_BUS_COORDINATOR_BIN and LIMA_BUS_TESTDIR to run")
	}
	dir, err := os.MkdirTemp(base, "bus-stuck-rejoin-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "bus.sock")

	cmd1, log1 := startCoordinator(t, bin, sock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vzEnd, err := DialShmemBus(ctx, sock, "02:00:00:00:00:0a")
	if err != nil {
		t.Fatalf("DialShmemBus: %v", err)
	}
	defer vzEnd.Close()
	waitFor(t, log1, "live at epoch", 5*time.Second)

	if err := cmd1.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = cmd1.Process.Wait()

	bh := startBlackhole(t, sock)
	select {
	case <-bh.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor never connected to the replacement acceptor")
	}
	// Let the blocked handshake settle before cancelling.
	time.Sleep(300 * time.Millisecond)
	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if edgeDown(vzEnd) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("stuck rejoin blocked VM teardown; VZ edge never went down")
}
