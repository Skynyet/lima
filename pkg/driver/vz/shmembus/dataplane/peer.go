// Package dataplane connects a datagram Ethernet edge to the shared-memory bus.
package dataplane

import (
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lima-vm/lima/v2/pkg/driver/vz/shmembus/busctl"
	"github.com/lima-vm/lima/v2/pkg/driver/vz/shmembus/ring"
)

const claimRetries = 8

type Config struct {
	Control    string
	EdgeLocal  string
	EdgeRemote string
	// EdgeFD is an already-connected SOCK_DGRAM edge, normally the relay end
	// of a VZ socketpair. Set it to -1 when using filesystem edge paths.
	EdgeFD    int
	MAC       uint64
	PortFlags uint32
	MaxFrame  int
	Sockbuf   int
	Duration  time.Duration
}

type Stats struct {
	Port                uint32 `json:"port"`
	IngressFrames       uint64 `json:"ingress_frames"`
	IngressBytes        uint64 `json:"ingress_bytes"`
	PublishedSlots      uint64 `json:"published_slots"`
	PublishedEmpty      uint64 `json:"published_empty"`
	ReserveDrops        uint64 `json:"reserve_drops"`
	Doorbells           uint64 `json:"doorbells"`
	EgressFrames        uint64 `json:"egress_frames"`
	EgressBytes         uint64 `json:"egress_bytes"`
	EgressFiltered      uint64 `json:"egress_filtered"`
	EgressWouldBlock    uint64 `json:"egress_would_block"`
	ClaimRetryExhausted uint64 `json:"claim_retry_exhausted"`
}

type source struct {
	generation uint32
	peer       *busctl.Peer
	ring       *ring.Ring
	state      *ring.ConsumerState
}

type controlResult struct {
	event busctl.Event
	err   error
}

type Peer struct {
	cfg            Config
	client         *busctl.Client
	ownRing        *ring.Ring
	ownState       *ring.ConsumerState
	directory      *ring.Directory
	edge           int
	kqueue         int
	wakeR          int
	wakeW          int
	sources        [ring.MaxPorts]*source
	control        chan controlResult
	controlDone    chan struct{}
	controlStarted bool
	membership     ring.MembershipSnapshot
	membershipOK   bool
	dropBuf        []byte
	stopping       atomic.Bool
	stats          Stats
}

func Open(cfg Config) (*Peer, error) {
	if cfg.Control == "" {
		return nil, fmt.Errorf("control is required")
	}
	pathEdge := cfg.EdgeLocal != "" || cfg.EdgeRemote != ""
	if cfg.EdgeFD >= 0 && pathEdge {
		return nil, fmt.Errorf("edge-fd and filesystem edge paths are mutually exclusive")
	}
	if cfg.EdgeFD < 0 && (cfg.EdgeLocal == "" || cfg.EdgeRemote == "") {
		return nil, fmt.Errorf("edge-fd or both edge-local and edge-remote are required")
	}
	if cfg.MaxFrame < 64 {
		return nil, fmt.Errorf("max-frame %d is too small", cfg.MaxFrame)
	}
	if cfg.Sockbuf <= 0 {
		cfg.Sockbuf = 4 << 20
	}

	edge, err := openEdge(cfg)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Peer, error) {
		syscall.Close(edge)
		_ = os.Remove(cfg.EdgeLocal)
		return nil, err
	}
	c, err := busctl.JoinWithFlags(cfg.Control, cfg.MAC, cfg.PortFlags)
	if err != nil {
		return fail(err)
	}
	ownRing, err := ring.OpenRing(c.OwnData)
	if err != nil {
		c.Close()
		return fail(fmt.Errorf("own ring: %w", err))
	}
	ownState, err := ring.OpenConsumerState(c.OwnState)
	if err != nil {
		c.Close()
		return fail(fmt.Errorf("own state: %w", err))
	}
	directory, err := ring.OpenDirectory(c.Directory)
	if err != nil {
		c.Close()
		return fail(fmt.Errorf("directory: %w", err))
	}

	p := &Peer{cfg: cfg, client: c, ownRing: ownRing, ownState: ownState,
		directory: directory, edge: edge, kqueue: -1, wakeR: -1, wakeW: -1,
		control:     make(chan controlResult, ring.MaxPorts*2),
		controlDone: make(chan struct{}), dropBuf: make([]byte, cfg.MaxFrame)}
	p.stats.Port = c.Port
	for port, capability := range c.Peers {
		if err := p.install(port, capability); err != nil {
			p.Close()
			return nil, err
		}
	}
	if err := p.openEvents(); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func openEdge(cfg Config) (int, error) {
	if cfg.EdgeFD >= 0 {
		fd, err := syscall.Dup(cfg.EdgeFD)
		if err != nil {
			return -1, fmt.Errorf("duplicate edge fd %d: %w", cfg.EdgeFD, err)
		}
		syscall.CloseOnExec(fd)
		if err := configureEdge(fd, cfg.Sockbuf); err != nil {
			syscall.Close(fd)
			return -1, fmt.Errorf("edge fd %d: %w", cfg.EdgeFD, err)
		}
		return fd, nil
	}
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		return -1, err
	}
	bad := func(err error) (int, error) {
		syscall.Close(fd)
		_ = os.Remove(cfg.EdgeLocal)
		return -1, err
	}
	_ = os.Remove(cfg.EdgeLocal)
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: cfg.EdgeLocal}); err != nil {
		return bad(fmt.Errorf("bind edge %s: %w", cfg.EdgeLocal, err))
	}
	if err := syscall.Connect(fd, &syscall.SockaddrUnix{Name: cfg.EdgeRemote}); err != nil {
		return bad(fmt.Errorf("connect edge %s: %w", cfg.EdgeRemote, err))
	}
	if err := configureEdge(fd, cfg.Sockbuf); err != nil {
		return bad(err)
	}
	return fd, nil
}

func configureEdge(fd, sockbuf int) error {
	typeValue, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_TYPE)
	if err != nil {
		return err
	}
	if typeValue != syscall.SOCK_DGRAM {
		return fmt.Errorf("socket type %d, want SOCK_DGRAM", typeValue)
	}
	if _, err := syscall.Getpeername(fd); err != nil {
		return fmt.Errorf("datagram socket is not connected: %w", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, sockbuf); err != nil {
		return err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, sockbuf); err != nil {
		return err
	}
	return syscall.SetNonblock(fd, true)
}

func (p *Peer) openEvents() error {
	fds := make([]int, 2)
	if err := syscall.Pipe(fds); err != nil {
		return err
	}
	p.wakeR, p.wakeW = fds[0], fds[1]
	if err := syscall.SetNonblock(p.wakeR, true); err != nil {
		return err
	}
	if err := syscall.SetNonblock(p.wakeW, true); err != nil {
		return err
	}
	kq, err := syscall.Kqueue()
	if err != nil {
		return err
	}
	p.kqueue = kq
	changes := make([]syscall.Kevent_t, 3)
	syscall.SetKevent(&changes[0], p.edge, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_ENABLE)
	syscall.SetKevent(&changes[1], p.client.DoorbellRx, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_ENABLE)
	syscall.SetKevent(&changes[2], p.wakeR, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_ENABLE)
	if _, err := syscall.Kevent(kq, changes, nil, nil); err != nil {
		return err
	}
	return nil
}

func (p *Peer) install(port uint32, capability *busctl.Peer) error {
	if port >= ring.MaxPorts {
		return fmt.Errorf("peer port %d exceeds max %d", port, ring.MaxPorts)
	}
	if capability == nil || capability.Data == nil || capability.State == nil || capability.DoorbellTx < 0 {
		return fmt.Errorf("port %d has incomplete capability bundle", port)
	}
	r, err := ring.OpenRing(capability.Data)
	if err != nil {
		return fmt.Errorf("source %d ring: %w", port, err)
	}
	state, err := ring.OpenConsumerState(capability.State)
	if err != nil {
		return fmt.Errorf("source %d state: %w", port, err)
	}
	p.sources[port] = &source{generation: capability.Generation, peer: capability,
		ring: r, state: state}
	return nil
}

func (p *Peer) Run() (Stats, error) {
	p.controlStarted = true
	go p.controlLoop()
	var deadline time.Time
	if p.cfg.Duration > 0 {
		deadline = time.Now().Add(p.cfg.Duration)
	}
	events := make([]syscall.Kevent_t, 8)
	edgeReady := false
	for !p.stopping.Load() {
		if err := p.applyControl(); err != nil {
			return p.stats, err
		}
		if err := p.consume(); err != nil {
			return p.stats, err
		}
		if edgeReady {
			if err := p.produce(); err != nil {
				return p.stats, err
			}
			edgeReady = false
		}
		if p.stopping.Load() {
			break
		}
		var timeout *syscall.Timespec
		var remaining syscall.Timespec
		if !deadline.IsZero() {
			d := time.Until(deadline)
			if d <= 0 {
				break
			}
			remaining = syscall.NsecToTimespec(d.Nanoseconds())
			timeout = &remaining
		}
		n, err := syscall.Kevent(p.kqueue, nil, events, timeout)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return p.stats, err
		}
		if n == 0 && !deadline.IsZero() {
			break
		}
		for i := 0; i < n; i++ {
			switch int(events[i].Ident) {
			case p.edge:
				edgeReady = true
			case p.client.DoorbellRx:
				p.drainFD(p.client.DoorbellRx)
			case p.wakeR:
				p.drainFD(p.wakeR)
			}
		}
	}
	return p.stats, nil
}

func (p *Peer) controlLoop() {
	defer close(p.controlDone)
	for {
		event, err := p.client.Next()
		p.control <- controlResult{event: event, err: err}
		p.wake()
		if err != nil {
			return
		}
	}
}

func (p *Peer) wake() {
	if p.wakeW >= 0 {
		_, _ = syscall.Write(p.wakeW, []byte{1})
	}
}

func (p *Peer) applyControl() error {
	for {
		select {
		case result := <-p.control:
			if result.err != nil {
				if busctl.IsLeaseClosed(result.err) && p.stopping.Load() {
					return nil
				}
				return result.err
			}
			switch result.event.Msg.Type {
			case busctl.MsgEndCapabilities:
				if result.event.Msg.Port != p.client.Port {
					if err := p.install(result.event.Msg.Port, result.event.Peer); err != nil {
						return err
					}
				}
			case busctl.MsgRetired, busctl.MsgAborted:
				if s := p.sources[result.event.Msg.Port]; s != nil &&
					s.generation == result.event.Msg.Generation {
					p.sources[result.event.Msg.Port] = nil
				}
			}
		default:
			return nil
		}
	}
}

func (p *Peer) produce() error {
	snapshot, err := p.snapshot()
	if err != nil {
		return nil // membership is changing; leave the readable edge untouched
	}
	targets, err := snapshot.TargetsFor(p.client.Port)
	if err != nil {
		return err
	}
	if targets == 0 {
		return p.discardIngress()
	}
	var states [ring.MaxPorts]*ring.ConsumerState
	for mask := targets; mask != 0; mask &= mask - 1 {
		port := uint32(bits.TrailingZeros64(mask))
		s := p.sources[port]
		if s == nil || s.generation != snapshot.Ports[port].Generation {
			return nil // control notification has not reached this loop yet
		}
		states[port] = s.state
	}
	var reservation ring.Reservation
	ok, err := p.ownRing.TryReserveInto(&reservation, states[:], p.pinMask)
	if err != nil {
		return err
	}
	if !ok {
		// TryReserve advanced head past an unpublished sequence. A consumer
		// asleep at that sequence needs a hint before it can classify the hole
		// and advance; this rare failure path deliberately rings every target.
		if err := p.ringTargets(targets); err != nil {
			return err
		}
		return p.discardIngress()
	}

	frames := uint64(0)
	bytes := uint64(0)
	for {
		buf, err := reservation.FrameBuffer()
		if err != nil || len(buf) < p.cfg.MaxFrame {
			break
		}
		n, err := syscall.Read(p.edge, buf)
		if isWouldBlock(err) {
			break
		}
		if err != nil {
			if publishErr := reservation.PublishDirect(snapshot.Epoch); publishErr != nil {
				return publishErr
			}
			return err
		}
		if n < 14 {
			if err := reservation.CommitFrame(uint32(n), 0, 0); err != nil {
				return err
			}
			continue
		}
		if err := reservation.CommitFrame(uint32(n), 0, targets); err != nil {
			return err
		}
		frames++
		bytes += uint64(n)
	}
	if err := reservation.PublishDirect(snapshot.Epoch); err != nil {
		return err
	}
	p.stats.PublishedSlots++
	if frames == 0 {
		p.stats.PublishedEmpty++
	}
	p.stats.IngressFrames += frames
	p.stats.IngressBytes += bytes

	// Check after publishing. If a consumer's cursor still names this sequence,
	// it may have observed the old head and gone to sleep, so ring it. A cursor
	// behind this sequence belongs to a consumer that must walk through the
	// backlog; a cursor ahead belongs to one that already saw the publication.
	caughtUp := uint64(0)
	for mask := targets; mask != 0; mask &= mask - 1 {
		port := uint32(bits.TrailingZeros64(mask))
		cursor, err := states[port].Cursor(p.client.Port)
		if err != nil {
			return err
		}
		if cursor == reservation.Sequence() {
			caughtUp |= uint64(1) << port
		}
	}
	return p.ringTargets(caughtUp)
}

func (p *Peer) ringTargets(targets uint64) error {
	var rings uint64
	for mask := targets; mask != 0; mask &= mask - 1 {
		port := uint32(bits.TrailingZeros64(mask))
		p.sources[port].peer.Ring(p.client.Port)
		rings++
	}
	if rings != 0 {
		if err := p.ownRing.RecordDoorbells(rings); err != nil {
			return err
		}
		p.stats.Doorbells += rings
	}
	return nil
}

func (p *Peer) discardIngress() error {
	for {
		n, err := syscall.Read(p.edge, p.dropBuf)
		if isWouldBlock(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if n > 0 {
			p.stats.ReserveDrops++
		}
	}
}

func (p *Peer) consume() error {
	snapshot, err := p.snapshot()
	if err != nil {
		return nil
	}
	for port := uint32(0); port < ring.MaxPorts; port++ {
		s := p.sources[port]
		identity := snapshot.Ports[port]
		if s == nil || s.generation != identity.Generation ||
			(identity.State != ring.PortLive && identity.State != ring.PortDraining) {
			continue
		}
		if err := p.consumeSource(port, s, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (p *Peer) consumeSource(port uint32, source *source, snapshot *ring.MembershipSnapshot) error {
	var claim ring.Claim
	for {
		cursor, err := p.ownState.Cursor(port)
		if err != nil {
			return err
		}
		if cursor >= source.ring.Head() {
			return nil
		}
		var status ring.ClaimStatus
		for retry := 0; retry <= claimRetries; retry++ {
			status, err = source.ring.TryClaimInto(&claim, p.ownState, p.client.Port, cursor,
				func(epoch uint64) bool {
					self := snapshot.Ports[p.client.Port]
					return self.Generation == p.client.Generation && self.ChangedAtEpoch != 0 &&
						self.ChangedAtEpoch <= epoch &&
						(self.State == ring.PortLive || self.State == ring.PortDraining)
				})
			if err != nil || status != ring.ClaimReclaiming {
				break
			}
			runtime.Gosched()
		}
		if err != nil {
			return err
		}
		switch status {
		case ring.ClaimReady:
			visitErr := claim.Records(p.deliver)
			releaseErr := claim.Release(true)
			if visitErr != nil {
				return visitErr
			}
			if releaseErr != nil {
				return releaseErr
			}
		case ring.ClaimReclaiming:
			if err := p.ownState.SkipClaimRetry(port, cursor); err != nil {
				return err
			}
			p.stats.ClaimRetryExhausted++
		case ring.ClaimNotTargeted, ring.ClaimStaleTarget, ring.ClaimNotPublished, ring.ClaimLapped:
			// TryClaim already advanced the cursor.
		case ring.ClaimNotInstalled, ring.ClaimSourceChanged:
			return nil
		default:
			return fmt.Errorf("source %d sequence %d: claim status %d", port, cursor, status)
		}
	}
}

// snapshot turns the directory seqlock into a one-word steady-state check.
// Membership changes are rare; packet events are not. Copying all 64 entries
// on every publication would make the control-plane directory a hot path.
func (p *Peer) snapshot() (*ring.MembershipSnapshot, error) {
	epoch, err := p.directory.Epoch()
	if err != nil {
		return nil, err
	}
	if p.membershipOK && epoch != 0 && epoch&1 == 0 && epoch == p.membership.Epoch {
		return &p.membership, nil
	}
	snapshot, err := p.directory.Snapshot(8)
	if err != nil {
		return nil, err
	}
	p.membership = snapshot
	p.membershipOK = true
	return &p.membership, nil
}

func (p *Peer) pinMask(publishEpoch, targets uint64) (uint64, error) {
	snapshot, err := p.snapshot()
	if err != nil {
		return 0, err
	}
	return snapshot.Pins(publishEpoch, targets), nil
}

func (p *Peer) deliver(frame ring.Frame) error {
	if !accepts(frame.Bytes, p.cfg.MAC) {
		p.stats.EgressFiltered++
		return nil
	}
	n, err := syscall.Write(p.edge, frame.Bytes)
	if isWouldBlock(err) || err == syscall.ENOBUFS {
		p.stats.EgressWouldBlock++
		return nil
	}
	if err != nil {
		return err
	}
	if n != len(frame.Bytes) {
		return fmt.Errorf("short datagram send: %d of %d", n, len(frame.Bytes))
	}
	p.stats.EgressFrames++
	p.stats.EgressBytes += uint64(n)
	return nil
}

func accepts(frame []byte, mac uint64) bool {
	if len(frame) < 6 {
		return false
	}
	if frame[0]&1 != 0 { // broadcast is included in multicast
		return true
	}
	for i := 0; i < 6; i++ {
		if frame[i] != byte(mac>>uint(8*(5-i))) {
			return false
		}
	}
	return true
}

func isWouldBlock(err error) bool {
	return err == syscall.EAGAIN || err == syscall.EWOULDBLOCK
}

func (p *Peer) drainFD(fd int) {
	var b [256]byte
	for {
		_, err := syscall.Read(fd, b[:])
		if err != nil {
			return
		}
	}
}

// Stop asks Run to return without concurrently closing any descriptor it may
// still be polling. Close owns the actual resource teardown and is called only
// after Run has returned.
func (p *Peer) Stop() {
	p.stopping.Store(true)
	p.wake()
}

func (p *Peer) Close() {
	p.Stop()
	if p.client != nil {
		if p.controlStarted {
			p.client.Interrupt()
			<-p.controlDone
		}
		p.client.Close()
	}
	for _, fd := range []int{p.edge, p.kqueue, p.wakeR, p.wakeW} {
		if fd >= 0 {
			syscall.Close(fd)
		}
	}
	p.edge, p.kqueue, p.wakeR, p.wakeW = -1, -1, -1, -1
	if p.cfg.EdgeLocal != "" {
		_ = os.Remove(p.cfg.EdgeLocal)
	}
}

func (s Stats) JSON() ([]byte, error) { return json.Marshal(s) }
