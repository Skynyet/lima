//go:build darwin && !no_vz

// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package vz

import "testing"

func TestShmemBusMAC(t *testing.T) {
	got, err := shmemBusMAC("02:00:00:00:00:0a")
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(0x02000000000a); got != want {
		t.Fatalf("parsed MAC %#x, want %#x", got, want)
	}
	if _, err := shmemBusMAC("02:00:00:00:00:0a:01"); err == nil {
		t.Fatal("accepted an eight-octet MAC")
	}
}
