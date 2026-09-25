// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package limayaml

import (
	"fmt"

	"github.com/lima-vm/lima/v2/pkg/limatype"
	"github.com/lima-vm/lima/v2/pkg/networks"
)

// ResolveNetworkMTU resolves one VM attachment's explicit MTU declaration.
// An omitted value is the ordinary 1500-MTU attachment. A reference is
// deliberately explicit: "cc.MTU" means the named segment must itself carry
// an explicit MTU and that the attachment must name `lima: cc`.
func ResolveNetworkMTU(nw limatype.Network, configured map[string]networks.Network) (uint32, error) {
	segment, ok := configured[nw.Lima]
	if !ok {
		return 0, fmt.Errorf("lima network %q is not defined in networks.yaml", nw.Lima)
	}
	segmentMTU := segment.MTU
	if segmentMTU == 0 {
		segmentMTU = networks.DefaultMTU
	}
	if nw.MTU == nil {
		return networks.DefaultMTU, nil
	}
	// Neither mode can carry an explicit attachment MTU. vmnet rejects one
	// for bridged networks, while user-v2's gvisor netstack stays at 1500.
	switch segment.Mode {
	case networks.ModeBridged, networks.ModeUserV2:
		return 0, fmt.Errorf("mtu is not supported on %q network %q", segment.Mode, nw.Lima)
	}

	if value, ok := nw.MTU.Reference(); ok {
		want := nw.Lima + ".MTU"
		if value != want {
			return 0, fmt.Errorf("mtu reference %q must be %q for lima network %q", value, want, nw.Lima)
		}
		if segment.MTU == 0 {
			return 0, fmt.Errorf("mtu reference %q is unresolved: network %q has no explicit mtu", value, nw.Lima)
		}
		return segment.MTU, nil
	}

	mtu, ok := nw.MTU.Numeric()
	if !ok {
		return 0, fmt.Errorf("mtu must be a number or %q", nw.Lima+".MTU")
	}

	if mtu < networks.MinMTU || mtu > networks.MaxMTU {
		return 0, fmt.Errorf("numeric mtu %d must be between %d and %d", mtu, networks.MinMTU, networks.MaxMTU)
	}
	if mtu > segmentMTU {
		return 0, fmt.Errorf("numeric mtu %d exceeds segment mtu %d for network %q", mtu, segmentMTU, nw.Lima)
	}
	return mtu, nil
}
