//go:build darwin && !no_vz

// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package vz

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/lima-vm/lima/v2/pkg/networks"
)

func TestShmemBusControlForNetwork(t *testing.T) {
	// A short directory keeps the Unix socket path below Darwin's sun_path
	// limit even when the test runner's own working directory is deep.
	dir, err := os.MkdirTemp("", "bp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	cfg := networks.Config{Paths: networks.Paths{VarRun: dir}}
	name := "shared"
	bus := cfg.SockShm(name)

	path, enabled, err := shmemBusControlForNetwork(&cfg, name)
	if err != nil || enabled || path != bus {
		t.Fatalf("missing bus: path=%q enabled=%t err=%v", path, enabled, err)
	}

	listener, err := net.Listen("unix", bus)
	if err != nil {
		t.Fatal(err)
	}
	path, enabled, err = shmemBusControlForNetwork(&cfg, name)
	if err != nil || !enabled || path != bus {
		t.Fatalf("existing socket: path=%q enabled=%t err=%v", path, enabled, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(bus, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := shmemBusControlForNetwork(&cfg, name); err == nil {
		t.Fatal("regular file at bus path was accepted")
	}
	if err := os.Remove(bus); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "other"), bus); err != nil {
		t.Fatal(err)
	}
	if _, _, err := shmemBusControlForNetwork(&cfg, name); err == nil {
		t.Fatal("symlink at bus path was accepted")
	}
}
