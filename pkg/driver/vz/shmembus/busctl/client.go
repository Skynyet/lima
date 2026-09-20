package busctl

import (
	"context"
	"encoding/binary"
	"fmt"
	"syscall"

	"github.com/lima-vm/lima/v2/pkg/driver/vz/shmembus/ring"
)

// A peer's capabilities as this participant holds them. Data and State are
// read-only mappings; the coordinator hands out no writable capability for
// anyone else's region, and attempting to map one writable fails.
type Peer struct {
	Port       uint32
	Generation uint32
	Data       []byte
	State      []byte
	DoorbellTx int
}

type Client struct {
	Port       uint32
	Generation uint32
	Epoch      uint64

	Directory []byte // read-only membership directory
	OwnData   []byte // this port's producer ring, writable
	OwnState  []byte // this port's cursors and active claims, writable

	DoorbellRx int
	Peers      map[uint32]*Peer

	// Superseded mappings remain valid until Close, when data-plane goroutines
	// have stopped using them.
	retained []*Peer

	sock   int
	reader *reader
}

// Join connects to a coordinator and returns once it is READY -- every
// capability received and mapped, and the directory says this port is live.
//
// The coordinator publishes the port only after the participant acknowledges
// that its capabilities are installed.

func Join(path string, mac uint64) (*Client, error) {
	return JoinWithFlags(path, mac, 0)
}

// JoinWithFlags joins with explicit port-role metadata. The HELLO epoch word
// carries flags because a HELLO names no directory epoch yet; message width and
// descriptor framing remain unchanged.
func JoinWithFlags(path string, mac uint64, flags uint32) (*Client, error) {
	return JoinWithContext(context.Background(), path, mac, flags)
}

// JoinWithContext is JoinWithFlags whose handshake the caller's context can
// abort. An acceptor that answers the connection but never completes the
// handshake would otherwise block the caller with no way out; cancelling the
// context shuts the lease down, which is the same wakeup Interrupt uses on a
// joined client, so the HELLO send and the read loop fail at once.
func JoinWithContext(ctx context.Context, path string, mac uint64, flags uint32) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if flags&^ring.PortFlagsMask != 0 {
		return nil, fmt.Errorf("unknown port flags %#x", flags)
	}
	sock, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	if err := syscall.Connect(sock, &syscall.SockaddrUnix{Name: path}); err != nil {
		syscall.Close(sock)
		return nil, fmt.Errorf("connect %s: %w", path, err)
	}
	stop := make(chan struct{})
	stopDone := make(chan bool)
	go func() {
		select {
		case <-ctx.Done():
			_ = syscall.Shutdown(sock, syscall.SHUT_RDWR)
			stopDone <- true
		case <-stop:
			stopDone <- false
		}
	}()
	// The watchdog owns the one shutdown of this lease: every return path
	// waits for it, and a cancellation that won the race against a completed
	// handshake is reported instead of returning a shut-down lease.
	stopWatchdog := func() bool {
		close(stop)
		return <-stopDone
	}
	c := &Client{sock: sock, reader: newReader(), Peers: map[uint32]*Peer{}, DoorbellRx: -1}

	if err := send(sock, Msg{Type: MsgHello, Version: Version, Port: PortNone, Bytes: mac,
		Epoch: uint64(flags)}); err != nil {
		stopWatchdog()
		c.Close()
		return nil, handshakeError(ctx, path, "hello", err)
	}
	for {
		m, fd, ok, err := c.reader.read(sock)
		if err != nil {
			stopWatchdog()
			c.Close()
			return nil, handshakeError(ctx, path, "handshake", err)
		}
		if !ok {
			continue // blocking socket, so this only happens on a partial read
		}
		if m.Type == MsgReady {
			if stopWatchdog() {
				c.Close()
				return nil, handshakeError(ctx, path, "handshake", context.Canceled)
			}
			c.Epoch = m.Epoch
			return c, nil
		}
		if err := c.install(m, fd); err != nil {
			stopWatchdog()
			c.Close()
			return nil, handshakeError(ctx, path, "handshake", err)
		}
	}
}

func handshakeError(ctx context.Context, path, stage string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s %s: %w: %w", stage, path, ctx.Err(), err)
	}
	return fmt.Errorf("%s %s: %w", stage, path, err)
}

// Event is something the coordinator told us after READY. The directory is
// always the authority; these save a poll interval and nothing more.
type Event struct {
	Msg  Msg
	Peer *Peer // for Retired and Aborted, the peer as it was
}

// Next blocks until the coordinator says something, handling installation and
// acknowledgement itself. A late joiner arrives as exactly the messages this
// participant received during its own join, which is why there is no separate
// late-join path to get wrong.
func (c *Client) Next() (Event, error) {
	for {
		m, fd, ok, err := c.reader.read(c.sock)
		if err != nil {
			return Event{}, err
		}
		if !ok {
			continue
		}
		before := c.Peers[m.Port]
		if err := c.install(m, fd); err != nil {
			return Event{}, err
		}
		return Event{Msg: m, Peer: before}, nil
	}
}

func (c *Client) install(m Msg, fd int) error {
	// Close the descriptor on every return unless ownership is transferred.
	owned := fd
	defer func() {
		if owned >= 0 {
			syscall.Close(owned)
		}
	}()
	keep := func() { owned = -1 }

	switch m.Type {
	case MsgWelcome:
		c.Port, c.Generation = m.Port, m.Generation
		if c.Directory != nil {
			return nil
		}
		mem, err := mapRO(fd, m.Bytes)
		if err != nil {
			return fmt.Errorf("directory: %w", err)
		}
		c.Directory = mem

	case MsgOwnData:
		if c.OwnData != nil {
			return nil
		}
		mem, err := mapRW(fd, m.Bytes)
		if err != nil {
			return fmt.Errorf("own ring: %w", err)
		}
		c.OwnData = mem

	case MsgOwnState:
		if c.OwnState != nil {
			return nil
		}
		mem, err := mapRW(fd, m.Bytes)
		if err != nil {
			return fmt.Errorf("own state region: %w", err)
		}
		c.OwnState = mem

	case MsgDoorbellRx:
		if c.DoorbellRx >= 0 {
			return nil
		}
		c.DoorbellRx = fd
		keep()

	// Capability records are idempotent. Keep an installed mapping and close any
	// duplicate descriptor rather than replacing a mapping in use.
	case MsgPeerData:
		p := c.peer(m.Port, m.Generation)
		if p.Data != nil {
			return nil
		}
		mem, err := mapRO(fd, m.Bytes)
		if err != nil {
			return fmt.Errorf("peer %d ring: %w", m.Port, err)
		}
		p.Data = mem

	case MsgPeerState:
		p := c.peer(m.Port, m.Generation)
		if p.State != nil {
			return nil
		}
		mem, err := mapRO(fd, m.Bytes)
		if err != nil {
			return fmt.Errorf("peer %d state region: %w", m.Port, err)
		}
		p.State = mem

	case MsgDoorbellTx:
		p := c.peer(m.Port, m.Generation)
		if p.DoorbellTx >= 0 {
			return nil
		}
		p.DoorbellTx = fd
		keep()

	case MsgEndCapabilities:
		// Everything for m.Port arrived before this message on the same stream,
		// so by the time we read it, it is installed. Recording that in the
		// ledger is what lets the coordinator publish.
		if err := c.markInstalled(m.Port, m.Generation); err != nil {
			return err
		}
		// A hint, and only that: the coordinator's poll finds the word anyway,
		// this only means sooner. Its failure is not this participant's problem.
		_ = send(c.sock, Msg{Type: MsgWake, Version: Version, Port: m.Port,
			Generation: m.Generation})

	case MsgRetired:
		// Retain the mapping and doorbell until Close; data-plane goroutines may
		// still be using either resource.
		if p := c.Peers[m.Port]; p != nil && p.Generation == m.Generation {
			c.retained = append(c.retained, p)
			delete(c.Peers, m.Port)
		}

	case MsgAborted:
		// A data-plane goroutine may still hold this generation's mapping when
		// the join is aborted. Retain it until Close to avoid unmapping in use.
		if p := c.Peers[m.Port]; p != nil && p.Generation == m.Generation {
			c.retained = append(c.retained, p)
			delete(c.Peers, m.Port)
		}
	}
	return nil
}

// Return the peer for this generation, retaining any superseded mapping until
// Close because a RETIRED hint may have been lost.

// The ledger. A word is stored only once the thing it claims is true, and only
// for the concrete generation it is about -- these are facts about a pair of
// generations, never permissions.
//
// Installing a source belongs to the bundle rather than being a separate act:
// the cursor starts at that source's current head, which is what stops an old
// slot from acquiring a new participant's bit retroactively.
func (c *Client) markInstalled(port, generation uint32) error {
	state, err := ring.OpenConsumerState(c.OwnState)
	if err != nil {
		return err
	}
	if port == c.Port {
		return state.MarkPrepared(generation)
	}
	p := c.Peers[port]
	if p == nil || p.Generation != generation || p.Data == nil || p.State == nil ||
		p.DoorbellTx < 0 {
		return fmt.Errorf("told port %d generation %d is complete, and it is not", port, generation)
	}
	r, err := ring.OpenRing(p.Data)
	if err != nil {
		return fmt.Errorf("peer %d ring: %w", port, err)
	}
	if err := state.InstallSource(port, generation, r.Head()); err != nil {
		return err
	}
	return state.MarkSourceInstalled(port, generation)
}

func (c *Client) peer(port, generation uint32) *Peer {
	p := c.Peers[port]
	if p != nil && p.Generation == generation {
		return p
	}
	if p != nil {
		c.retained = append(c.retained, p)
	}
	p = &Peer{Port: port, Generation: generation, DoorbellTx: -1}
	c.Peers[port] = p
	return p
}

// Ring a peer's doorbell. A hint, never a fact: correctness lives in head and
// cursor, so a full buffer or a closed peer is not an error worth propagating.
func (p *Peer) Ring(sourcePort uint32) {
	if p.DoorbellTx < 0 {
		return
	}
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], sourcePort)
	syscall.Write(p.DoorbellTx, b[:])
}

func (p *Peer) unmap() {
	if p.Data != nil {
		syscall.Munmap(p.Data)
		p.Data = nil
	}
	if p.State != nil {
		syscall.Munmap(p.State)
		p.State = nil
	}
}

func (p *Peer) closeDoorbell() {
	if p.DoorbellTx >= 0 {
		syscall.Close(p.DoorbellTx)
		p.DoorbellTx = -1
	}
}

// MembershipEpochWord atomically reads the directory epoch. It is not a
// membership snapshot; use the ring package to read a validated snapshot.
func (c *Client) MembershipEpochWord() (uint64, error) {
	return ring.LoadSeqCst(c.Directory, ring.DirectoryMembershipOffset)
}

// Interrupt wakes a goroutine blocked in Next without unmapping any
// capability. The caller must wait for that goroutine to return before Close.
func (c *Client) Interrupt() {
	if c.sock >= 0 {
		_ = syscall.Shutdown(c.sock, syscall.SHUT_RDWR)
	}
}

// Close ends the lease, which is what tells the coordinator this process is
// gone. Nothing else does.
func (c *Client) Close() {
	if c.sock >= 0 {
		syscall.Close(c.sock)
		c.sock = -1
	}
	c.reader.close()
	if c.DoorbellRx >= 0 {
		syscall.Close(c.DoorbellRx)
		c.DoorbellRx = -1
	}
	for _, p := range c.Peers {
		p.unmap()
		p.closeDoorbell()
	}
	for _, p := range c.retained {
		p.unmap()
		p.closeDoorbell()
	}
	c.retained = nil
	if c.Directory != nil {
		syscall.Munmap(c.Directory)
		c.Directory = nil
	}
	if c.OwnData != nil {
		syscall.Munmap(c.OwnData)
		c.OwnData = nil
	}
	if c.OwnState != nil {
		syscall.Munmap(c.OwnState)
		c.OwnState = nil
	}
}

func mapRO(fd int, bytes uint64) ([]byte, error) {
	return syscall.Mmap(fd, 0, int(bytes), syscall.PROT_READ, syscall.MAP_SHARED)
}

func mapRW(fd int, bytes uint64) ([]byte, error) {
	return syscall.Mmap(fd, 0, int(bytes), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

// MapWritableFails reports whether a descriptor refuses a writable mapping.
// This is the capability claim itself: if a read-only descriptor could be
// promoted, "read-only consumer" would be a comment rather than a property.
func MapWritableFails(fd int, bytes uint64) bool {
	mem, err := mapRW(fd, bytes)
	if err != nil {
		return true
	}
	syscall.Munmap(mem)
	return false
}
