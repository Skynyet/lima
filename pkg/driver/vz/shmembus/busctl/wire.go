// Package busctl implements the shared-memory bus control protocol. Ring data
// layout and access are handled by the ring package.
package busctl

import (
	"encoding/binary"
	"fmt"
	"syscall"
)

const (
	Version  = 1
	MaxPorts = 64
	PortNone = ^uint32(0)

	MsgBytes = 32
)

type MsgType uint32

const (
	MsgHello           MsgType = 1
	MsgWelcome         MsgType = 2
	MsgOwnData         MsgType = 3
	MsgOwnState        MsgType = 4
	MsgPeerData        MsgType = 5
	MsgPeerState       MsgType = 6
	MsgDoorbellRx      MsgType = 7
	MsgDoorbellTx      MsgType = 8
	MsgReady           MsgType = 9
	MsgRetired         MsgType = 10
	MsgEndCapabilities MsgType = 11
	MsgWake            MsgType = 12
	MsgAborted         MsgType = 13
)

func (t MsgType) String() string {
	switch t {
	case MsgHello:
		return "HELLO"
	case MsgWelcome:
		return "WELCOME"
	case MsgOwnData:
		return "OWN_DATA"
	case MsgOwnState:
		return "OWN_STATE"
	case MsgPeerData:
		return "PEER_DATA"
	case MsgPeerState:
		return "PEER_STATE"
	case MsgDoorbellRx:
		return "DOORBELL_RX"
	case MsgDoorbellTx:
		return "DOORBELL_TX"
	case MsgReady:
		return "READY"
	case MsgRetired:
		return "RETIRED"
	case MsgEndCapabilities:
		return "END_CAPABILITIES"
	case MsgWake:
		return "WAKE"
	case MsgAborted:
		return "ABORTED"
	}
	return fmt.Sprintf("type(%d)", uint32(t))
}

// Fixed width, native little-endian, no pointers. The same 32 bytes the C side
// writes; laid out by hand rather than by a struct so Go's field packing is
// never part of the protocol.
type Msg struct {
	Type       MsgType
	Version    uint32
	Port       uint32
	Generation uint32
	Bytes      uint64
	Epoch      uint64
}

func (m Msg) marshal() []byte {
	b := make([]byte, MsgBytes)
	binary.LittleEndian.PutUint32(b[0:], uint32(m.Type))
	binary.LittleEndian.PutUint32(b[4:], m.Version)
	binary.LittleEndian.PutUint32(b[8:], m.Port)
	binary.LittleEndian.PutUint32(b[12:], m.Generation)
	binary.LittleEndian.PutUint64(b[16:], m.Bytes)
	binary.LittleEndian.PutUint64(b[24:], m.Epoch)
	return b
}

func unmarshal(b []byte) Msg {
	return Msg{
		Type:       MsgType(binary.LittleEndian.Uint32(b[0:])),
		Version:    binary.LittleEndian.Uint32(b[4:]),
		Port:       binary.LittleEndian.Uint32(b[8:]),
		Generation: binary.LittleEndian.Uint32(b[12:]),
		Bytes:      binary.LittleEndian.Uint64(b[16:]),
		Epoch:      binary.LittleEndian.Uint64(b[24:]),
	}
}

// reader accumulates one message across as many reads as the stream takes, and
// holds a descriptor that arrived with any piece of it until the message is
// whole. A stream does not preserve message boundaries, and Darwin has no
// AF_UNIX SOCK_SEQPACKET to borrow them from -- socket() and socketpair() both
// return EPROTONOSUPPORT there. Returning early on a short read would
// desynchronise the stream and drop the capability in the same breath.
type reader struct {
	buf [MsgBytes]byte
	got int
	fd  int
}

func newReader() *reader { return &reader{fd: -1} }

func (r *reader) close() {
	if r.fd >= 0 {
		syscall.Close(r.fd)
		r.fd = -1
	}
	r.got = 0
}

// read returns (msg, fd, true, nil) when a whole message is available, and
// (_, _, false, nil) when more bytes are needed. fd is -1 when the message
// carried no capability. io.EOF means the lease ended, which is how a
// participant learns the coordinator is gone.
func (r *reader) read(fd int) (Msg, int, bool, error) {
	oob := make([]byte, syscall.CmsgSpace(4))
	for r.got < MsgBytes {
		n, oobn, flags, _, err := syscall.Recvmsg(fd, r.buf[r.got:], oob, 0)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			if err == syscall.EAGAIN {
				return Msg{}, -1, false, nil
			}
			return Msg{}, -1, false, err
		}
		if n == 0 && oobn == 0 {
			return Msg{}, -1, false, errEOF
		}
		// MSG_CTRUNC means the kernel dropped a descriptor nothing can recover.
		// Continuing would leave the coordinator believing we hold a capability
		// we do not.
		if flags&syscall.MSG_CTRUNC != 0 {
			return Msg{}, -1, false, fmt.Errorf("control message truncated: a capability was lost")
		}
		if oobn > 0 {
			scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
			if err != nil {
				return Msg{}, -1, false, err
			}
			for _, scm := range scms {
				fds, err := syscall.ParseUnixRights(&scm)
				if err != nil {
					continue
				}
				for _, got := range fds {
					if r.fd >= 0 {
						// Two descriptors in one message is not this protocol.
						syscall.Close(got)
						return Msg{}, -1, false, fmt.Errorf("message carried more than one capability")
					}
					r.fd = got
				}
			}
		}
		r.got += n
	}
	m := unmarshal(r.buf[:])
	got := r.fd
	r.fd = -1
	r.got = 0
	return m, got, true, nil
}

func send(sock int, m Msg) error {
	b := m.marshal()
	for len(b) > 0 {
		n, err := syscall.Write(sock, b)
		if err != nil {
			if err == syscall.EINTR || err == syscall.EAGAIN {
				continue
			}
			return err
		}
		b = b[n:]
	}
	return nil
}

type eofError struct{}

func (eofError) Error() string { return "lease closed" }

var errEOF = eofError{}

// IsLeaseClosed reports whether an error is the lease ending rather than
// failing. The distinction matters: a closed lease is how this design proves a
// process is gone, not a malfunction.
func IsLeaseClosed(err error) bool {
	_, ok := err.(eofError)
	return ok
}
