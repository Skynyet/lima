//go:build darwin && !no_vz

// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package vz

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/lima-vm/lima/v2/pkg/driver/vz/shmembus/dataplane"
)

const shmemBusMaxFrame = 16 * 1024

// DialShmemBus connects VZ's datagram attachment to an in-process bus
// participant. The socketpair is only the VZ file-handle edge: all payload
// between participants crosses the shared mappings managed by dataplane.Peer.
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
	peer, err := dataplane.Open(dataplane.Config{
		Control:  controlSock,
		EdgeFD:   int(peerEnd.Fd()),
		MAC:      mac,
		MaxFrame: shmemBusMaxFrame,
	})
	if err != nil {
		return fail(fmt.Errorf("join shared-memory bus %q: %w", controlSock, err))
	}
	// dataplane.Open duplicates EdgeFD. The local descriptor must not remain a
	// second owner, otherwise VZ cannot observe edge shutdown.
	_ = peerEnd.Close()

	var closeOnce sync.Once
	closePeer := func() { closeOnce.Do(peer.Close) }
	done := make(chan struct{})
	go func() {
		stats, runErr := peer.Run()
		if runErr != nil && ctx.Err() == nil {
			logrus.WithError(runErr).Error("Shared-memory bus participant stopped")
		} else {
			logrus.WithFields(logrus.Fields{
				"ingress_frames": stats.IngressFrames,
				"egress_frames":  stats.EgressFrames,
				"reserve_drops":  stats.ReserveDrops,
			}).Debug("Shared-memory bus participant stopped")
		}
		closePeer()
		close(done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			peer.Stop()
		case <-done:
		}
	}()

	return vzEnd, nil
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
