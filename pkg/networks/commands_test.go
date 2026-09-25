// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package networks

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/lima-vm/lima/v2/pkg/limatype/dirnames"
)

func TestCheck(t *testing.T) {
	config, err := DefaultConfig()
	assert.NilError(t, err)

	for _, name := range []string{"bridged", "shared", "host"} {
		err = config.Check(name)
		assert.NilError(t, err)
	}
	err = config.Check("unknown")
	assert.ErrorContains(t, err, "not defined")
}

func TestLogFile(t *testing.T) {
	config, err := DefaultConfig()
	assert.NilError(t, err)

	logFile := config.LogFile("name", "daemon", "stream")
	networksDir, err := dirnames.LimaNetworksDir()
	assert.NilError(t, err)
	assert.Equal(t, logFile, filepath.Join(networksDir, "name_daemon.stream.log"))
}

func TestSockShm(t *testing.T) {
	config := Config{Paths: Paths{VarRun: filepath.Join("private", "var", "run", "lima")}}
	assert.Equal(t, config.Sock("shared"), filepath.Join("private", "var", "run", "lima", "socket_vmnet.shared"))
	assert.Equal(t, config.SockShm("shared"), filepath.Join("private", "var", "run", "lima", "socket_vmnet_shm.shared"))
	assert.Equal(t, config.SockShm("secure-strict"), filepath.Join("private", "var", "run", "lima", "socket_vmnet_shm.secure-strict"))
}

func TestSockShmFromLegacy(t *testing.T) {
	for _, tc := range []struct {
		legacy string
		want   string
	}{
		{"/opt/homebrew/var/run/socket_vmnet", "/opt/homebrew/var/run/socket_vmnet_shm"},
		{"/private/var/run/lima/socket_vmnet.shared", "/private/var/run/lima/socket_vmnet_shm.shared"},
		{"/run/custom.net", "/run/custom_shm.net"},
	} {
		got, err := SockShmFromLegacy(tc.legacy)
		assert.NilError(t, err)
		assert.Equal(t, got, tc.want)
	}
	_, err := SockShmFromLegacy("/run/")
	assert.ErrorContains(t, err, "no filename")
}

func TestUser(t *testing.T) {
	config, err := DefaultConfig()
	assert.NilError(t, err)
	if runtime.GOOS == "windows" {
		// unimplemented
		t.Skip()
	}

	t.Run("socket_vmnet", func(t *testing.T) {
		if ok, _ := config.IsDaemonInstalled(SocketVMNet); !ok {
			t.Skip("socket_vmnet is not installed")
		}
		user, err := config.User(SocketVMNet)
		assert.NilError(t, err)
		assert.Equal(t, user.User, "root")
		if runtime.GOOS == "darwin" {
			assert.Equal(t, user.Group, "wheel")
		} else {
			assert.Equal(t, user.Group, "root")
		}
		assert.Equal(t, user.Uid, uint32(0))
		assert.Equal(t, user.Gid, uint32(0))
	})
}

func TestMkdirCmd(t *testing.T) {
	config, err := DefaultConfig()
	assert.NilError(t, err)

	cmd := config.MkdirCmd()
	assert.Equal(t, cmd, "/bin/mkdir -m 775 -p /private/var/run/lima")
}

func TestStartCmd(t *testing.T) {
	config, err := DefaultConfig()
	assert.NilError(t, err)

	varRunDir := filepath.Join("/", "private", "var", "run", "lima")

	t.Run("socket_vmnet", func(t *testing.T) {
		if ok, _ := config.IsDaemonInstalled(SocketVMNet); !ok {
			t.Skip("socket_vmnet is not installed")
		}

		cmd := config.StartCmd("shared", SocketVMNet)
		assert.Equal(t, cmd, "/opt/socket_vmnet/bin/socket_vmnet --pidfile="+filepath.Join(varRunDir, "shared_socket_vmnet.pid")+" --socket-group=admin --vmnet-mode=shared "+
			"--vmnet-gateway=192.168.105.1 --vmnet-dhcp-end=192.168.105.254 --vmnet-mask=255.255.255.0 "+filepath.Join(varRunDir, "socket_vmnet.shared"))
		shared := config.Networks["shared"]
		shared.MTU = 9000
		config.Networks["shared"] = shared
		cmd = config.StartCmd("shared", SocketVMNet)
		assert.Assert(t, strings.Contains(cmd, " --vmnet-mtu=9000 "))

		cmd = config.StartCmd("bridged", SocketVMNet)
		assert.Equal(t, cmd, "/opt/socket_vmnet/bin/socket_vmnet --pidfile="+filepath.Join(varRunDir, "bridged_socket_vmnet.pid")+" --socket-group=admin --vmnet-mode=bridged "+
			"--vmnet-interface=en0 "+filepath.Join(varRunDir, "socket_vmnet.bridged"))
	})
}

func TestStopCmd(t *testing.T) {
	config, err := DefaultConfig()
	assert.NilError(t, err)

	varRunDir := filepath.Join("/", "private", "var", "run", "lima")

	cmd := config.StopCmd("name", "daemon")
	assert.Equal(t, cmd, "/usr/bin/pkill -F "+filepath.Join(varRunDir, "name_daemon.pid"))
}
