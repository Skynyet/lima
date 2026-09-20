//go:build darwin && !no_vz

// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package vz

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/lima-vm/lima/v2/pkg/driver/vz/shmembus/dataplane"
)

const shmemBusMaxFrame = 16 * 1024

const (
	rejoinBackoffInitial = 100 * time.Millisecond
	rejoinBackoffMax     = 2 * time.Second
)

// DialShmemBus connects VZ's datagram attachment to an in-process bus
// participant. The socketpair is only the VZ file-handle edge: all payload
// between participants crosses the shared mappings managed by dataplane.Peer.
//
// The peer-side descriptor is kept as the VM-lifetime anchor: when the control
// lease ends while the VM is still running -- the daemon was restarted -- a
// supervisor tears the participant fully down and rejoins through a fresh dup
// of the anchor, and the guest's device never drops. Only the final close of
// the anchor at VM teardown is what VZ observes as edge shutdown.
func DialShmemBus(ctx context.Context, controlSock, macText string) (*os.File, error) {
	peerEnd, vzEnd, err := createSockPair()
	if err != nil {
		return nil, fmt.Errorf("create VZ/shared-memory bus socketpair: %w", err)
	}
	fail := func(cause error) (*os.File, error) {
		_ = peerEnd.Close()
		_ = vzEnd.Close()
		return nil, cause
	}

	mac, err := shmemBusMAC(macText)
	if err != nil {
		return fail(err)
	}
	cfg := dataplane.Config{
		Control:  controlSock,
		EdgeFD:   int(peerEnd.Fd()),
		MAC:      mac,
		MaxFrame: shmemBusMaxFrame,
	}
	peer, err := dataplane.Open(cfg)
	if err != nil {
		return fail(fmt.Errorf("join shared-memory bus %q: %w", controlSock, err))
	}
	go superviseBus(ctx, peerEnd, peer, cfg)

	return vzEnd, nil
}

// superviseBus owns the participant and the anchor. Every join is a fresh
// dataplane.Peer built on a fresh dup of the anchor, so no per-join state --
// control channels, kqueue registrations, membership cache, mapped regions --
// survives from a dead coordinator into a new one. The old peer is fully
// closed before the new one opens; the edge never has two readers at once.
func superviseBus(ctx context.Context, anchor *os.File, first *dataplane.Peer, cfg dataplane.Config) {
	defer func() { _ = anchor.Close() }()
	peer := first
	for {
		p := peer
		stopped := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				p.Stop()
			case <-stopped:
			}
		}()
		stats, runErr := peer.Run()
		close(stopped)
		peer.Close()
		if runErr == nil || ctx.Err() != nil {
			logrus.WithFields(logrus.Fields{
				"ingress_frames": stats.IngressFrames,
				"egress_frames":  stats.EgressFrames,
				"reserve_drops":  stats.ReserveDrops,
			}).Debug("Shared-memory bus participant stopped")
			return
		}
		logrus.WithError(runErr).Warn("Shared-memory bus lease lost; rejoining")
		next, ok := rejoin(ctx, cfg)
		if !ok {
			logrus.Debug("Shared-memory bus participant stopped")
			return
		}
		peer = next
	}
}

// rejoin retries dataplane.Open with capped exponential backoff until a
// restarted coordinator has rebound its control path -- while it is down,
// connect fails with ENOENT or ECONNREFUSED -- or the VM context ends.
func rejoin(ctx context.Context, cfg dataplane.Config) (*dataplane.Peer, bool) {
	backoff := rejoinBackoffInitial
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(backoff):
		}
		peer, err := dataplane.Open(cfg)
		if err == nil {
			return peer, true
		}
		logrus.WithError(err).Debug("Shared-memory bus rejoin attempt failed")
		backoff *= 2
		if backoff > rejoinBackoffMax {
			backoff = rejoinBackoffMax
		}
	}
}

func shmemBusMAC(text string) (uint64, error) {
	mac, err := net.ParseMAC(text)
	if err != nil || len(mac) != 6 {
		if err == nil {
			err = fmt.Errorf("want six octets")
		}
		return 0, fmt.Errorf("parse shared-memory bus MAC %q: %w", text, err)
	}
	var value uint64
	for _, octet := range mac {
		value = value<<8 | uint64(octet)
	}
	return value, nil
}
