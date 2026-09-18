// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package networks

import "net"

type Config struct {
	Paths    Paths              `yaml:"paths" json:"paths"`
	Group    string             `yaml:"group,omitempty" json:"group,omitempty"` // default: "admin"
	Networks map[string]Network `yaml:"networks" json:"networks"`
}

type Paths struct {
	SocketVMNet string `yaml:"socketVMNet" json:"socketVMNet"`
	VarRun      string `yaml:"varRun" json:"varRun"`
	Sudoers     string `yaml:"sudoers,omitempty" json:"sudoers,omitempty"`
}

const (
	ModeUserV2  = "user-v2"
	ModeHost    = "host"
	ModeShared  = "shared"
	ModeBridged = "bridged"
)

var Modes = []string{
	ModeUserV2,
	ModeHost,
	ModeShared,
	ModeBridged,
}

type Network struct {
	Mode      string `yaml:"mode" json:"mode"`                               // "user-v2", "host", "shared", or "bridged"
	Interface string `yaml:"interface,omitempty" json:"interface,omitempty"` // only used by "bridged" networks
	Gateway   net.IP `yaml:"gateway,omitempty" json:"gateway,omitempty"`     // only used by "user-v2", "host" and "shared" networks
	DHCPEnd   net.IP `yaml:"dhcpEnd,omitempty" json:"dhcpEnd,omitempty"`     // default: same as Gateway, last byte is 254
	NetMask   net.IP `yaml:"netmask,omitempty" json:"netmask,omitempty"`     // default: 255.255.255.0
	// MTU of the segment, passed to socket_vmnet as --vmnet-mtu. Only "host"
	// and "shared" modes; vmnet rejects it for "bridged". Unset leaves vmnet at
	// its default of 1500.
	MTU uint32 `yaml:"mtu,omitempty" json:"mtu,omitempty"`
}
