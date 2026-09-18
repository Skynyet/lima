// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package networks

import (
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestValidateRejectsInjectableNetworkDefinitions(t *testing.T) {
	// A newline in a network name adds a directive to the generated sudoers file.
	err := (&Config{Networks: map[string]Network{
		"evil\n%staff ALL=(root) NOPASSWD: ALL": {Mode: ModeShared},
	}}).Validate()
	assert.ErrorContains(t, err, "invalid network name")

	// A space in the interface injects an extra argument into the sudo command.
	err = (&Config{Networks: map[string]Network{
		"bridged": {Mode: ModeBridged, Interface: "en0 --extra-root-flag"},
	}}).Validate()
	assert.ErrorContains(t, err, "invalid interface")

	// Whitespace in the mode injects an extra argument into the sudo command.
	err = (&Config{Networks: map[string]Network{
		"shared": {Mode: "shared --extra"},
	}}).Validate()
	assert.ErrorContains(t, err, "invalid mode")

	// Whitespace in the group breaks the sudoers header line.
	err = (&Config{Group: "admin bad", Networks: map[string]Network{}}).Validate()
	assert.ErrorContains(t, err, "invalid group")

	// A path-traversal network name would redirect the pidfile/socket path.
	err = (&Config{Networks: map[string]Network{
		"../../etc/foo": {Mode: ModeShared},
	}}).Validate()
	assert.ErrorContains(t, err, "invalid network name")
}

func TestValidateMTU(t *testing.T) {
	// Out of range at both ends. socket_vmnet would refuse these too, but only
	// once the daemon is started under sudo, well away from the file at fault.
	err := validateMTU("shared", Network{Mode: ModeShared, MTU: 67})
	assert.ErrorContains(t, err, "must be between 68 and 65535")
	err = validateMTU("shared", Network{Mode: ModeShared, MTU: 65536})
	assert.ErrorContains(t, err, "must be between 68 and 65535")

	// The silent ones. StartCmd emits --vmnet-mtu only for host and shared, so
	// a bridged network drops the segment half without a word, and a user-v2
	// network never reaches socket_vmnet at all.
	err = validateMTU("bridged", Network{Mode: ModeBridged, MTU: 9000})
	assert.ErrorContains(t, err, "not supported")
	err = validateMTU("user-v2", Network{Mode: ModeUserV2, MTU: 9000})
	assert.ErrorContains(t, err, "not supported")

	// What the field is for, and the condition removed: the same modes with no
	// MTU set must pass, or the checks above would be measuring the mode.
	assert.NilError(t, validateMTU("shared", Network{Mode: ModeShared, MTU: 9000}))
	assert.NilError(t, validateMTU("host", Network{Mode: ModeHost, MTU: MinMTU}))
	assert.NilError(t, validateMTU("bridged", Network{Mode: ModeBridged}))
	assert.NilError(t, validateMTU("user-v2", Network{Mode: ModeUserV2}))
}

func TestValidateRejectsMTUOnBridged(t *testing.T) {
	// Config.Validate must actually call it. A helper with passing tests that
	// nothing invokes is the failure this test exists to exclude.
	err := (&Config{Networks: map[string]Network{
		"bridged": {Mode: ModeBridged, Interface: "en0", MTU: 9000},
	}}).Validate()
	assert.ErrorContains(t, err, "`mtu` is not supported")

	// Condition removed: the same network without the MTU gets past this check.
	// It goes on to fail on paths, which is a different message.
	err = (&Config{Networks: map[string]Network{
		"bridged": {Mode: ModeBridged, Interface: "en0"},
	}}).Validate()
	if err != nil {
		assert.Assert(t, !strings.Contains(err.Error(), "mtu"), "unexpected mtu error: %v", err)
	}
}
