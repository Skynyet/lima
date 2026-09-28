package dataplane

import (
	"bytes"
	"syscall"
	"testing"
)

func datagramSocketpair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if err := syscall.SetNonblock(fd, true); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = syscall.Close(fds[0])
		_ = syscall.Close(fds[1])
	})
	return fds[0], fds[1]
}

func batchTestPeer(fd, maxFrame int) *Peer {
	p := &Peer{edge: fd, kqueue: -1, cfg: Config{MaxFrame: maxFrame}}
	p.batch.init(maxFrame)
	return p
}

func TestRecvmsgXReadsQueuedDatagramsWithoutWaitingForFullBatch(t *testing.T) {
	rx, tx := datagramSocketpair(t)
	p := batchTestPeer(rx, 2048)
	want := [][]byte{[]byte("one"), []byte("two-two"), bytes.Repeat([]byte{3}, 1500)}
	for _, frame := range want {
		if n, err := syscall.Write(tx, frame); err != nil || n != len(frame) {
			t.Fatalf("write = %d, %v", n, err)
		}
	}

	n, err := p.recvEdge(edgeBatchCap)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Fatalf("recvmsg_x returned %d datagrams, want %d", n, len(want))
	}
	for i := range want {
		if p.batch.recvBad[i] || p.batch.recvLen[i] != len(want[i]) ||
			!bytes.Equal(p.batch.recvBuf[i][:p.batch.recvLen[i]], want[i]) {
			t.Fatalf("datagram %d: length=%d bad=%v", i, p.batch.recvLen[i], p.batch.recvBad[i])
		}
	}
	if p.stats.IngressSyscalls != 1 || p.stats.IngressBatchFrames != uint64(len(want)) {
		t.Fatalf("stats: calls=%d frames=%d", p.stats.IngressSyscalls,
			p.stats.IngressBatchFrames)
	}
	if _, err := p.recvEdge(edgeBatchCap); !isWouldBlock(err) {
		t.Fatalf("empty nonblocking batch returned %v, want EAGAIN", err)
	}
}

func TestSendmsgXWritesDatagramsInOneCall(t *testing.T) {
	tx, rx := datagramSocketpair(t)
	p := batchTestPeer(tx, 2048)
	frames := make([][]byte, edgeBatchCap)
	for i := range frames {
		frames[i] = bytes.Repeat([]byte{byte(i + 1)}, 64+i)
	}

	sent, blocked, err := p.sendFrames(frames)
	if err != nil || blocked != edgeNotBlocked || sent != len(frames) {
		t.Fatalf("sendFrames = %d, %d, %v", sent, blocked, err)
	}
	if p.stats.EgressSyscalls != 1 || p.stats.EgressBatchFrames != uint64(len(frames)) {
		t.Fatalf("stats: calls=%d frames=%d", p.stats.EgressSyscalls,
			p.stats.EgressBatchFrames)
	}
	for i, want := range frames {
		buf := make([]byte, 2048)
		n, err := syscall.Read(rx, buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:n], want) {
			t.Fatalf("datagram %d differs", i)
		}
	}
}

func TestHeldEgressOwnsTheCongestedRemainder(t *testing.T) {
	p := &Peer{kqueue: -1}
	frames := [][]byte{[]byte("first"), []byte("second")}
	if err := p.holdEgress(frames, edgeNoBuffers); err != nil {
		t.Fatal(err)
	}
	frames[0][0] = 'X'
	frames[1][0] = 'Y'
	if got := string(p.batch.pending[0]); got != "first" {
		t.Fatalf("first held frame = %q", got)
	}
	if got := string(p.batch.pending[1]); got != "second" {
		t.Fatalf("second held frame = %q", got)
	}
}
