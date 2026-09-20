//go:build darwin && !no_vz

// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package vz

import (
	"bytes"
	"context"
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

	cmd2, log2 := startCoordinator(t, bin, sock)
	defer func() {
		_ = cmd2.Process.Kill()
		_, _ = cmd2.Process.Wait()
	}()
	waitFor(t, log2, "live at epoch", 10*time.Second)

	// The VZ edge must not have observed shutdown while the daemon was gone,
	// and no frame may appear out of nothing.
	select {
	case evt := <-events:
		t.Fatalf("VZ edge produced %d bytes or EOF during coordinator restart", len(evt.data))
	case <-time.After(600 * time.Millisecond):
	}

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
