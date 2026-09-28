package dataplane

import (
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

const (
	edgeBatchCap        = 32
	edgeDrainBatchLimit = 8
	sysRecvmsgX         = 480
	sysSendmsgX         = 481
	messageTruncated    = 0x10
	edgeENOBUFSBackoff  = 50 * time.Microsecond
)

// messageX is Darwin's private struct msghdr_x. Keep the public msghdr first:
// the kernel consumes an array of this exact layout, not an array of pointers.
type messageX struct {
	header syscall.Msghdr
	length uintptr
}

type edgeBlock uint8

const (
	edgeNotBlocked edgeBlock = iota
	edgeWouldBlock
	edgeNoBuffers
)

type edgeBatchState struct {
	disabled bool
	iov      [edgeBatchCap]syscall.Iovec
	msg      [edgeBatchCap]messageX
	recvData []byte
	recvBuf  [edgeBatchCap][]byte
	recvLen  [edgeBatchCap]int
	recvBad  [edgeBatchCap]bool

	// pending owns copies only when a nonblocking send could not finish a
	// claim. The ordinary path sends directly from the shared ring and allocates
	// nothing. Owning the exceptional remainder lets us release the ring claim
	// immediately instead of pinning a producer behind one congested VZ edge.
	pending     [][]byte
	pendingData []byte
	blocked     edgeBlock
	retryAt     time.Time
	writeArmed  bool
}

func (b *edgeBatchState) init(maxFrame int) {
	b.recvData = make([]byte, edgeBatchCap*maxFrame)
	for i := range b.recvBuf {
		start := i * maxFrame
		b.recvBuf[i] = b.recvData[start : start+maxFrame]
	}
}

func batchUnsupported(err error) bool {
	return errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.ENOTSUP)
}

func (p *Peer) disableEdgeBatch() {
	if !p.batch.disabled {
		p.batch.disabled = true
		p.stats.BatchFallbacks++
	}
}

// recvEdge receives up to limit complete datagrams into reusable private
// storage. It never waits to fill a batch: the edge is nonblocking and the
// syscall returns whatever is already queued.
func (p *Peer) recvEdge(limit int) (int, error) {
	if limit < 1 || limit > edgeBatchCap {
		return 0, fmt.Errorf("invalid edge receive batch %d", limit)
	}
	if p.batch.disabled {
		n, err := syscall.Read(p.edge, p.batch.recvBuf[0])
		p.stats.IngressSyscalls++
		if err == nil {
			p.batch.recvLen[0] = n
			p.batch.recvBad[0] = false
			p.stats.IngressBatchFrames++
			return 1, nil
		}
		return 0, err
	}

	for i := 0; i < limit; i++ {
		buf := p.batch.recvBuf[i]
		p.batch.iov[i] = syscall.Iovec{Base: &buf[0]}
		p.batch.iov[i].SetLen(len(buf))
		p.batch.msg[i] = messageX{}
		p.batch.msg[i].header.Iov = &p.batch.iov[i]
		p.batch.msg[i].header.Iovlen = 1
		p.batch.msg[i].length = uintptr(len(buf))
	}
	n, err := messageXCall(sysRecvmsgX, p.edge, p.batch.msg[:limit])
	p.stats.IngressSyscalls++
	if batchUnsupported(err) {
		p.disableEdgeBatch()
		return p.recvEdge(1)
	}
	if err != nil {
		return 0, err
	}
	if n < 0 || n > limit {
		return 0, fmt.Errorf("recvmsg_x returned %d datagrams for %d buffers", n, limit)
	}
	p.stats.IngressBatchFrames += uint64(n)
	for i := 0; i < n; i++ {
		length := p.batch.msg[i].length
		bad := p.batch.msg[i].header.Flags&messageTruncated != 0 ||
			length > uintptr(len(p.batch.recvBuf[i]))
		p.batch.recvBad[i] = bad
		if bad {
			p.batch.recvLen[i] = 0
			p.stats.IngressTruncated++
		} else {
			p.batch.recvLen[i] = int(length)
		}
	}
	return n, nil
}

// sendEdge sends a prefix of frames. A short successful send is progress, not
// loss: the caller immediately retries the remainder. EAGAIN and ENOBUFS are
// returned separately so the event loop can choose write readiness or a short
// timer without sleeping in the dataplane.
func (p *Peer) sendEdge(frames [][]byte) (int, edgeBlock, error) {
	if len(frames) == 0 {
		return 0, edgeNotBlocked, nil
	}
	count := len(frames)
	if count > edgeBatchCap {
		count = edgeBatchCap
	}
	if p.batch.disabled {
		for i := 0; i < count; i++ {
			n, err := syscall.Write(p.edge, frames[i])
			p.stats.EgressSyscalls++
			if isWouldBlock(err) {
				p.stats.EgressWouldBlock++
				return i, edgeWouldBlock, nil
			}
			if err == syscall.ENOBUFS {
				p.stats.EgressENOBUFS++
				return i, edgeNoBuffers, nil
			}
			if err != nil {
				return i, edgeNotBlocked, err
			}
			if n != len(frames[i]) {
				return i, edgeNotBlocked,
					fmt.Errorf("short datagram send: %d of %d", n, len(frames[i]))
			}
			p.stats.EgressBatchFrames++
		}
		return count, edgeNotBlocked, nil
	}

	for i := 0; i < count; i++ {
		if len(frames[i]) == 0 {
			return 0, edgeNotBlocked, fmt.Errorf("cannot send an empty ethernet frame")
		}
		p.batch.iov[i] = syscall.Iovec{Base: &frames[i][0]}
		p.batch.iov[i].SetLen(len(frames[i]))
		p.batch.msg[i] = messageX{}
		p.batch.msg[i].header.Iov = &p.batch.iov[i]
		p.batch.msg[i].header.Iovlen = 1
		p.batch.msg[i].length = uintptr(len(frames[i]))
	}
	n, err := messageXCall(sysSendmsgX, p.edge, p.batch.msg[:count])
	p.stats.EgressSyscalls++
	if batchUnsupported(err) {
		p.disableEdgeBatch()
		return p.sendEdge(frames[:count])
	}
	if isWouldBlock(err) {
		p.stats.EgressWouldBlock++
		return 0, edgeWouldBlock, nil
	}
	if err == syscall.ENOBUFS {
		p.stats.EgressENOBUFS++
		return 0, edgeNoBuffers, nil
	}
	if err != nil {
		return 0, edgeNotBlocked, err
	}
	if n < 0 || n > count {
		return 0, edgeNotBlocked,
			fmt.Errorf("sendmsg_x returned %d datagrams for %d frames", n, count)
	}
	p.stats.EgressBatchFrames += uint64(n)
	if n < count {
		p.stats.EgressPartial++
		if n == 0 {
			return 0, edgeWouldBlock, nil
		}
	}
	return n, edgeNotBlocked, nil
}

func messageXCall(trap uintptr, fd int, messages []messageX) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}
	for {
		n, _, errno := syscall.Syscall6(trap, uintptr(fd),
			uintptr(unsafe.Pointer(&messages[0])), uintptr(len(messages)), 0, 0, 0)
		runtime.KeepAlive(messages)
		if errno == 0 {
			return int(n), nil
		}
		if errno != syscall.EINTR {
			return 0, errno
		}
	}
}

func (p *Peer) sendFrames(frames [][]byte) (int, edgeBlock, error) {
	sent := 0
	for sent < len(frames) {
		n, blocked, err := p.sendEdge(frames[sent:])
		for _, frame := range frames[sent : sent+n] {
			p.stats.EgressFrames++
			p.stats.EgressBytes += uint64(len(frame))
		}
		sent += n
		if err != nil || blocked != edgeNotBlocked {
			return sent, blocked, err
		}
	}
	return sent, edgeNotBlocked, nil
}

func (p *Peer) holdEgress(frames [][]byte, blocked edgeBlock) error {
	total := 0
	for _, frame := range frames {
		total += len(frame)
	}
	p.batch.pendingData = make([]byte, total)
	p.batch.pending = make([][]byte, len(frames))
	off := 0
	for i, frame := range frames {
		copy(p.batch.pendingData[off:], frame)
		p.batch.pending[i] = p.batch.pendingData[off : off+len(frame)]
		off += len(frame)
	}
	return p.setEgressBlocked(blocked)
}

func (p *Peer) setEgressBlocked(blocked edgeBlock) error {
	p.batch.blocked = blocked
	switch blocked {
	case edgeWouldBlock:
		p.batch.retryAt = time.Time{}
		return p.armEdgeWrite(true)
	case edgeNoBuffers:
		p.batch.retryAt = time.Now().Add(edgeENOBUFSBackoff)
		return p.armEdgeWrite(false)
	default:
		p.batch.retryAt = time.Time{}
		return p.armEdgeWrite(false)
	}
}

func (p *Peer) flushHeldEgress() error {
	if len(p.batch.pending) == 0 {
		return nil
	}
	if p.batch.blocked == edgeWouldBlock {
		return nil
	}
	if p.batch.blocked == edgeNoBuffers && time.Now().Before(p.batch.retryAt) {
		return nil
	}
	p.batch.blocked = edgeNotBlocked
	sent, blocked, err := p.sendFrames(p.batch.pending)
	if err != nil {
		return err
	}
	p.batch.pending = p.batch.pending[sent:]
	if len(p.batch.pending) == 0 {
		p.batch.pendingData = nil
		return p.setEgressBlocked(edgeNotBlocked)
	}
	return p.setEgressBlocked(blocked)
}

func (p *Peer) armEdgeWrite(enable bool) error {
	if p.kqueue < 0 || p.batch.writeArmed == enable {
		return nil
	}
	var event syscall.Kevent_t
	flags := syscall.EV_ADD | syscall.EV_ENABLE
	if !enable {
		flags = syscall.EV_DELETE
	}
	syscall.SetKevent(&event, p.edge, syscall.EVFILT_WRITE, flags)
	if _, err := syscall.Kevent(p.kqueue, []syscall.Kevent_t{event}, nil, nil); err != nil {
		if !enable && err == syscall.ENOENT {
			p.batch.writeArmed = false
			return nil
		}
		return err
	}
	p.batch.writeArmed = enable
	return nil
}
