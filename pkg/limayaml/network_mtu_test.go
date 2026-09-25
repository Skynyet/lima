// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package limayaml

import (
	"testing"

	"github.com/goccy/go-yaml"
	"gotest.tools/v3/assert"

	"github.com/lima-vm/lima/v2/pkg/limatype"
	"github.com/lima-vm/lima/v2/pkg/networks"
)

func TestResolveNetworkMTU(t *testing.T) {
	configured := map[string]networks.Network{
		"cc":   {Mode: networks.ModeShared, MTU: 9000},
		"prod": {Mode: networks.ModeHost, MTU: 4096},
	}

	mtu, err := ResolveNetworkMTU(limatype.Network{Lima: "cc"}, configured)
	assert.NilError(t, err)
	assert.Equal(t, mtu, uint32(networks.DefaultMTU))

	mtu, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTU(1500)}, configured)
	assert.NilError(t, err)
	assert.Equal(t, mtu, uint32(1500))

	mtu, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTUReference("cc.MTU")}, configured)
	assert.NilError(t, err)
	assert.Equal(t, mtu, uint32(9000))

	mtu, err = ResolveNetworkMTU(limatype.Network{Lima: "prod", MTU: limatype.NewNetworkMTUReference("prod.MTU")}, configured)
	assert.NilError(t, err)
	assert.Equal(t, mtu, uint32(4096))

	_, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTUReference("other.MTU")}, configured)
	assert.ErrorContains(t, err, "must be \"cc.MTU\"")

	_, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTUReference("cc.MTU")}, map[string]networks.Network{"cc": {Mode: networks.ModeShared}})
	assert.ErrorContains(t, err, "has no explicit mtu")

	_, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTU(9000)}, map[string]networks.Network{"cc": {Mode: networks.ModeShared}})
	assert.ErrorContains(t, err, "exceeds segment mtu 1500")

	_, err = ResolveNetworkMTU(limatype.Network{Lima: "missing", MTU: limatype.NewNetworkMTUReference("missing.MTU")}, configured)
	assert.ErrorContains(t, err, "not defined in networks.yaml")

	_, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTU(67)}, configured)
	assert.ErrorContains(t, err, "must be between 68 and 65535")
	_, err = ResolveNetworkMTU(limatype.Network{Lima: "cc", MTU: limatype.NewNetworkMTU(65536)}, configured)
	assert.ErrorContains(t, err, "must be between 68 and 65535")
	_, err = ResolveNetworkMTU(limatype.Network{Lima: "bridged", MTU: limatype.NewNetworkMTU(9000)}, map[string]networks.Network{"bridged": {Mode: networks.ModeBridged}})
	assert.ErrorContains(t, err, "not supported")
	_, err = ResolveNetworkMTU(limatype.Network{Lima: "user-v2", MTU: limatype.NewNetworkMTU(9000)}, map[string]networks.Network{"user-v2": {Mode: networks.ModeUserV2}})
	assert.ErrorContains(t, err, "not supported")
}

func TestResolveNetworkMTUFromYAMLScalars(t *testing.T) {
	var cfg limatype.LimaYAML
	err := yaml.Unmarshal([]byte(`networks:
- lima: numeric
  mtu: 2048
- lima: referenced
  mtu: referenced.MTU
`), &cfg)
	assert.NilError(t, err)
	configured := map[string]networks.Network{
		"numeric":    {Mode: networks.ModeShared, MTU: 9000},
		"referenced": {Mode: networks.ModeShared, MTU: 4096},
	}
	mtu, err := ResolveNetworkMTU(cfg.Networks[0], configured)
	assert.NilError(t, err)
	assert.Equal(t, mtu, uint32(2048))
	mtu, err = ResolveNetworkMTU(cfg.Networks[1], configured)
	assert.NilError(t, err)
	assert.Equal(t, mtu, uint32(4096))
}
