// Package dataplane connects a datagram Ethernet edge to the shared-memory bus.
package dataplane

import (
	"context"
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
	// Context bounds the join handshake. Nil keeps the historical behaviour of
	// blocking until the coordinator answers or the connection fails.
	Context    context.Context
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
	IngressSyscalls     uint64 `json:"ingress_syscalls"`
	IngressBatchFrames  uint64 `json:"ingress_batch_frames"`
	IngressTruncated    uint64 `json:"ingress_truncated"`
	EgressSyscalls      uint64 `json:"egress_syscalls"`
	EgressBatchFrames   uint64 `json:"egress_batch_frames"`
	EgressPartial       uint64 `json:"egress_partial"`
	EgressENOBUFS       uint64 `json:"egress_enobufs"`
	BatchFallbacks      uint64 `json:"batch_fallbacks"`
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
	frameScratch   [][]byte
	batch          edgeBatchState
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
	c, err := busctl.JoinWithContext(cfg.Context, cfg.Control, cfg.MAC, cfg.PortFlags)
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
		control:      make(chan controlResult, ring.MaxPorts*2),
		controlDone:  make(chan struct{}),
		frameScratch: make([][]byte, 0, edgeBatchCap)}
	p.batch.init(cfg.MaxFrame)
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
		var retry syscall.Timespec
		if len(p.batch.pending) != 0 && p.batch.blocked == edgeNoBuffers {
			d := time.Until(p.batch.retryAt)
			if d < 0 {
				d = 0
			}
			retry = syscall.NsecToTimespec(d.Nanoseconds())
			if timeout == nil || d < time.Duration(remaining.Nano()) {
				timeout = &retry
			}
		}
		n, err := syscall.Kevent(p.kqueue, nil, events, timeout)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return p.stats, err
		}
		if n == 0 {
			if len(p.batch.pending) != 0 && p.batch.blocked == edgeNoBuffers {
				p.batch.blocked = edgeNotBlocked
				continue
			}
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				break
			}
		}
		for i := 0; i < n; i++ {
			switch int(events[i].Ident) {
			case p.edge:
				switch events[i].Filter {
				case syscall.EVFILT_READ:
					edgeReady = true
				case syscall.EVFILT_WRITE:
					p.batch.blocked = edgeNotBlocked
				}
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

	// Receive before reserving ring space. Unlike the old zero-copy Read path,
	// private batch storage lets an empty nonblocking call return without
	// publishing an empty slot. One event handles at most edgeBatchCap frames;
	// kqueue remains level-triggered if more are queued, which is the fairness
	// bound the old drain-to-EAGAIN proposal lacked.
	count, err := p.recvEdge(edgeBatchCap)
	if isWouldBlock(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}

	var reservation ring.Reservation
	reserved := false
	slotRecords := uint64(0)
	slotFrames := uint64(0)
	slotBytes := uint64(0)
	for i := 0; i < count; i++ {
		if p.batch.recvBad[i] {
			continue
		}
		length := p.batch.recvLen[i]
		for {
			if !reserved {
				ok, reserveErr := p.ownRing.TryReserveInto(&reservation, states[:], p.pinMask)
				if reserveErr != nil {
					return reserveErr
				}
				if !ok {
					for j := i; j < count; j++ {
						if !p.batch.recvBad[j] && p.batch.recvLen[j] > 0 {
							p.stats.ReserveDrops++
						}
					}
					// TryReserve advanced head past an unpublished sequence. A
					// sleeping consumer needs a hint to classify that hole.
					if err := p.ringTargets(targets); err != nil {
						return err
					}
					return p.discardIngress()
				}
				reserved = true
				slotRecords = 0
				slotFrames = 0
				slotBytes = 0
			}

			buf, bufferErr := reservation.FrameBuffer()
			if bufferErr == nil && length <= len(buf) {
				copy(buf[:length], p.batch.recvBuf[i][:length])
				mask := targets
				if length < 14 {
					mask = 0
				} else {
					slotFrames++
					slotBytes += uint64(length)
				}
				if err := reservation.CommitFrame(uint32(length), 0, mask); err != nil {
					return err
				}
				slotRecords++
				break
			}
			if slotRecords == 0 {
				if bufferErr != nil {
					return bufferErr
				}
				return fmt.Errorf("frame length %d exceeds an empty ring slot", length)
			}
			if err := p.publishIngress(&reservation, snapshot.Epoch, targets,
				states[:], slotFrames, slotBytes); err != nil {
				return err
			}
			reserved = false
		}
	}
	if reserved {
		return p.publishIngress(&reservation, snapshot.Epoch, targets,
			states[:], slotFrames, slotBytes)
	}
	return nil
}

func (p *Peer) publishIngress(reservation *ring.Reservation, epoch, targets uint64,
	states []*ring.ConsumerState, frames, bytes uint64) error {
	if err := reservation.PublishDirect(epoch); err != nil {
		return err
	}
	p.stats.PublishedSlots++
	if frames == 0 {
		p.stats.PublishedEmpty++
	}
	p.stats.IngressFrames += frames
	p.stats.IngressBytes += bytes

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
	// Keep the exceptional drop path bounded too. The edge is level-triggered,
	// so another event follows when more data remains; one peer cannot hold the
	// event loop forever merely because it currently has no targets or ring
	// capacity.
	for batch := 0; batch < edgeDrainBatchLimit; batch++ {
		n, err := p.recvEdge(edgeBatchCap)
		if isWouldBlock(err) {
			return nil
		}
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if !p.batch.recvBad[i] && p.batch.recvLen[i] > 0 {
				p.stats.ReserveDrops++
			}
		}
	}
	return nil
}

func (p *Peer) consume() error {
	if err := p.flushHeldEgress(); err != nil {
		return err
	}
	if len(p.batch.pending) != 0 {
		return nil
	}
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
			if err := p.deliverClaim(&claim); err != nil {
				return err
			}
			if len(p.batch.pending) != 0 {
				return nil
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

func (p *Peer) deliverClaim(claim *ring.Claim) error {
	p.frameScratch = p.frameScratch[:0]
	if err := claim.Records(func(frame ring.Frame) error {
		if !accepts(frame.Bytes, p.cfg.MAC) {
			p.stats.EgressFiltered++
			return nil
		}
		p.frameScratch = append(p.frameScratch, frame.Bytes)
		return nil
	}); err != nil {
		_ = claim.Release(true)
		return err
	}

	sent, blocked, sendErr := p.sendFrames(p.frameScratch)
	var holdErr error
	if sendErr == nil && blocked != edgeNotBlocked {
		holdErr = p.holdEgress(p.frameScratch[sent:], blocked)
	}
	releaseErr := claim.Release(true)
	if sendErr != nil {
		return sendErr
	}
	if holdErr != nil {
		return holdErr
	}
	return releaseErr
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
