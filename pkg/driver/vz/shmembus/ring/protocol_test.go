package ring

import (
	"bytes"
	"testing"
)

func testRing(t *testing.T, capacity uint32) (*Ring, []*ConsumerState) {
	t.Helper()
	slotBytes := uint32(512)
	rmem := make([]byte, RingRegionBytes(capacity, slotBytes))
	r, err := InitRing(rmem, 0, 7, 1500, capacity, slotBytes)
	if err != nil {
		t.Fatal(err)
	}
	states := make([]*ConsumerState, 2)
	for i := range states {
		mem := make([]byte, ConsumerRegionBytes())
		states[i], err = InitConsumerState(mem, uint32(i), uint32(10+i))
		if err != nil {
			t.Fatal(err)
		}
		if err := states[i].InstallSource(0, 7, 1); err != nil {
			t.Fatal(err)
		}
	}
	return r, states
}

func publishOne(t *testing.T, r *Ring, states []*ConsumerState, targets uint64, payload string) uint64 {
	t.Helper()
	res, ok, err := r.TryReserve(states, PinAll)
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	seq := res.sequence
	if err := res.Publish(2, []Frame{{Bytes: []byte(payload), DestinationMask: targets}}); err != nil {
		t.Fatal(err)
	}
	return seq
}

func TestClaimProtectsOnlyActiveReader(t *testing.T) {
	r, states := testRing(t, 2)
	seq1 := publishOne(t, r, states, 1<<1, "one")
	_ = publishOne(t, r, states, 1<<1, "two")

	claim, status, err := r.TryClaim(states[1], 1, seq1, func(uint64) bool { return true })
	if err != nil || status != ClaimReady {
		t.Fatalf("claim: status=%v err=%v", status, err)
	}
	if err := claim.Records(func(frame Frame) error {
		if !bytes.Equal(frame.Bytes, []byte("one")) {
			t.Fatalf("payload %q", frame.Bytes)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	blockedHead := r.Head()
	if _, ok, err := r.TryReserve(states, PinAll); err != nil || ok {
		t.Fatalf("active reader did not close reuse: ok=%v err=%v", ok, err)
	}
	if got := r.Head(); got != blockedHead+1 {
		t.Fatalf("blocked physical slot did not skip: head=%d want=%d", got, blockedHead+1)
	}
	if got := r.slotGuard(seq1); got != GuardPublished(seq1) {
		t.Fatalf("aborted reclaim restored guard %#x, want %#x", got, GuardPublished(seq1))
	}
	if err := claim.Release(true); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.TryReserve(states, PinAll); err != nil || !ok {
		t.Fatalf("released reader still blocks reuse: ok=%v err=%v", ok, err)
	}
}

func TestLaggingButInactiveConsumerDoesNotPinRing(t *testing.T) {
	r, states := testRing(t, 2)
	_ = publishOne(t, r, states, 1<<1, "one")
	_ = publishOne(t, r, states, 1<<1, "two")
	// Consumer 1's cursor is still 1, but it holds no active claim. Reusing the
	// first physical slot must succeed: backlog is not ownership.
	res, ok, err := r.TryReserve(states, PinAll)
	if err != nil || !ok {
		t.Fatalf("lagging inactive consumer pinned ring: ok=%v err=%v", ok, err)
	}
	if err := res.Publish(2, []Frame{{Bytes: []byte("three"), DestinationMask: 1 << 1}}); err != nil {
		t.Fatal(err)
	}
	if _, status, err := r.TryClaim(states[1], 1, 1, func(uint64) bool { return true }); err != nil || status != ClaimLapped {
		t.Fatalf("old sequence: status=%v err=%v", status, err)
	}
	if cursor, err := states[1].Cursor(0); err != nil || cursor != 2 {
		t.Fatalf("lapped cursor=%d err=%v, want oldest resident 2", cursor, err)
	}
}

func TestUninterestedConsumerDoesNotClaim(t *testing.T) {
	r, states := testRing(t, 2)
	seq := publishOne(t, r, states, 1<<1, "target one")
	if claim, status, err := r.TryClaim(states[0], 0, seq, func(uint64) bool { return true }); err != nil || status != ClaimNotTargeted || claim != nil {
		t.Fatalf("unexpected claim: claim=%v status=%v err=%v", claim, status, err)
	}
	off, _ := states[0].sourceOffset(0)
	if active, _ := LoadSeqCst(states[0].mem, off+ConsumerActiveSeqOffset); active != SequenceNone {
		t.Fatalf("uninterested consumer installed active claim %d", active)
	}
	if cursor, err := states[0].Cursor(0); err != nil || cursor != seq+1 {
		t.Fatalf("uninterested cursor=%d err=%v, want %d", cursor, err, seq+1)
	}
}

func TestClaimDistinguishesNotPublishedAndSourceStates(t *testing.T) {
	r, states := testRing(t, 2)
	seq1 := publishOne(t, r, states, 1<<1, "one")
	_ = publishOne(t, r, states, 1<<1, "two")

	claim, status, err := r.TryClaim(states[1], 1, seq1, func(uint64) bool { return true })
	if err != nil || status != ClaimReady {
		t.Fatalf("claim: status=%v err=%v", status, err)
	}
	missing := r.Head()
	if _, ok, err := r.TryReserve(states, PinAll); err != nil || ok {
		t.Fatalf("active claim did not force skip: ok=%v err=%v", ok, err)
	}
	if err := claim.Release(false); err != nil {
		t.Fatal(err)
	}
	if _, status, err := r.TryClaim(states[1], 1, missing, func(uint64) bool { return true }); err != nil || status != ClaimNotPublished {
		t.Fatalf("missing sequence: status=%v err=%v", status, err)
	}
	if cursor, err := states[1].Cursor(0); err != nil || cursor != missing+1 {
		t.Fatalf("not-published cursor=%d err=%v, want %d", cursor, err, missing+1)
	}

	if err := states[1].InstallSource(0, 0, r.Head()); err != nil {
		t.Fatal(err)
	}
	if _, status, err := r.TryClaim(states[1], 1, r.Head(), func(uint64) bool { return true }); err != nil || status != ClaimNotInstalled {
		t.Fatalf("source not installed: status=%v err=%v", status, err)
	}

	if err := states[1].InstallSource(0, 8, r.Head()); err != nil {
		t.Fatal(err)
	}
	before, _ := states[1].Cursor(0)
	if _, status, err := r.TryClaim(states[1], 1, r.Head(), func(uint64) bool { return true }); err != nil || status != ClaimSourceChanged {
		t.Fatalf("source change: status=%v err=%v", status, err)
	}
	if after, _ := states[1].Cursor(0); after != before {
		t.Fatalf("source change advanced cursor from %d to %d", before, after)
	}
}

func TestSkipClaimRetryAdvancesOnlyMatchingCursor(t *testing.T) {
	_, states := testRing(t, 2)
	state := states[1]
	if err := state.SkipClaimRetry(0, 1); err != nil {
		t.Fatal(err)
	}
	if got, err := state.Cursor(0); err != nil || got != 2 {
		t.Fatalf("cursor=%d err=%v, want 2", got, err)
	}
	off, _ := state.sourceOffset(0)
	if got, err := LoadSeqCst(state.mem, off+ConsumerDropRetryOffset); err != nil || got != 1 {
		t.Fatalf("retry drops=%d err=%v, want 1", got, err)
	}
	if err := state.SkipClaimRetry(0, 1); err != nil {
		t.Fatal(err)
	}
	if got, _ := LoadSeqCst(state.mem, off+ConsumerDropRetryOffset); got != 1 {
		t.Fatalf("stale retry incremented drops to %d", got)
	}
}

func TestClaimDistinguishesRoutingMissFromStaleIdentity(t *testing.T) {
	r, states := testRing(t, 2)
	seq := publishOne(t, r, states, 1<<1, "one")
	if _, status, err := r.TryClaim(states[0], 0, seq, func(uint64) bool { return false }); err != nil || status != ClaimNotTargeted {
		t.Fatalf("routing miss: status=%v err=%v", status, err)
	}
	if err := states[1].InstallSource(0, 7, seq); err != nil {
		t.Fatal(err)
	}
	if _, status, err := r.TryClaim(states[1], 1, seq, func(uint64) bool { return false }); err != nil || status != ClaimStaleTarget {
		t.Fatalf("stale identity: status=%v err=%v", status, err)
	}
}

func TestAbortRefusesAfterFrameBufferExposure(t *testing.T) {
	r, states := testRing(t, 1)
	_ = publishOne(t, r, states, 1<<1, "original")
	res, ok, err := r.TryReserve(states, PinAll)
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	buf, err := res.FrameBuffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(buf, "corrupt old payload")
	if err := res.Abort(); err == nil {
		t.Fatal("abort restored an old guard after exposing mutable payload")
	}
	// EAGAIN after exposure burns an empty, untargeted sequence instead.
	if err := res.PublishDirect(2); err != nil {
		t.Fatal(err)
	}
	if _, status, err := r.TryClaim(states[1], 1, res.sequence, func(uint64) bool { return true }); err != nil || status != ClaimNotTargeted {
		t.Fatalf("empty publication: status=%v err=%v", status, err)
	}
}

func TestClaimVisitsOnlyTargetedRecords(t *testing.T) {
	r, states := testRing(t, 2)
	res, ok, err := r.TryReserve(states, PinAll)
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	seq := res.sequence
	if err := res.Publish(2, []Frame{
		{Bytes: []byte("zero"), DestinationMask: 1 << 0},
		{Bytes: []byte("one"), DestinationMask: 1 << 1},
		{Bytes: []byte("both"), DestinationMask: 0b11},
	}); err != nil {
		t.Fatal(err)
	}
	claim, status, err := r.TryClaim(states[1], 1, seq, func(uint64) bool { return true })
	if err != nil || status != ClaimReady {
		t.Fatalf("claim: status=%v err=%v", status, err)
	}
	var got []string
	if err := claim.Records(func(frame Frame) error {
		got = append(got, string(frame.Bytes))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "both" {
		t.Fatalf("targeted records = %v", got)
	}
	if err := claim.Release(true); err != nil {
		t.Fatal(err)
	}
}

func TestRejectedPublishLeavesOldSlotIntact(t *testing.T) {
	r, states := testRing(t, 1)
	seq := publishOne(t, r, states, 1<<1, "old payload")

	res, ok, err := r.TryReserve(states, PinAll)
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	tooLarge := make([]byte, int(r.slotBytes))
	if err := res.Publish(2, []Frame{
		{Bytes: []byte("would overwrite old payload"), DestinationMask: 1 << 1},
		{Bytes: tooLarge, DestinationMask: 1 << 1},
	}); err == nil {
		t.Fatal("oversized batch unexpectedly published")
	}
	if err := res.Abort(); err != nil {
		t.Fatal(err)
	}

	claim, status, err := r.TryClaim(states[1], 1, seq, func(uint64) bool { return true })
	if err != nil || status != ClaimReady {
		t.Fatalf("claim old slot: status=%v err=%v", status, err)
	}
	var got string
	if err := claim.Records(func(frame Frame) error {
		got = string(frame.Bytes)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != "old payload" {
		t.Fatalf("old payload changed to %q", got)
	}
	if err := claim.Release(false); err != nil {
		t.Fatal(err)
	}
}

func TestDirectBuilderAndEpochScopedPinFilter(t *testing.T) {
	r, states := testRing(t, 1)
	_ = publishOne(t, r, states, 1<<1, "old")

	var gotEpoch, gotTargets uint64
	res, ok, err := r.TryReserve(states, func(epoch, targets uint64) (uint64, error) {
		gotEpoch, gotTargets = epoch, targets
		return 0, nil // model an old target whose process is proven retired
	})
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	if gotEpoch != 2 || gotTargets != 1<<1 {
		t.Fatalf("filter saw epoch=%d targets=%#x", gotEpoch, gotTargets)
	}
	buf, err := res.FrameBuffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(buf, "direct")
	if err := res.CommitFrame(6, 9, 1<<0); err != nil {
		t.Fatal(err)
	}
	if err := res.PublishDirect(4); err != nil {
		t.Fatal(err)
	}

	claim, status, err := r.TryClaim(states[0], 0, res.sequence, func(epoch uint64) bool { return epoch == 4 })
	if err != nil || status != ClaimReady {
		t.Fatalf("claim: status=%v err=%v", status, err)
	}
	if err := claim.Records(func(frame Frame) error {
		if string(frame.Bytes) != "direct" || frame.Flags != 9 {
			t.Fatalf("frame=%q flags=%d", frame.Bytes, frame.Flags)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := claim.Release(true); err != nil {
		t.Fatal(err)
	}
	if published, _ := LoadSeqCst(r.mem, RingPublishedSlotsOffset); published != 2 {
		t.Fatalf("published counter=%d", published)
	}
}

func TestCallerOwnedHotPathDoesNotAllocate(t *testing.T) {
	r, states := testRing(t, 2)
	var reservation Reservation
	if got := testing.AllocsPerRun(100, func() {
		ok, err := r.TryReserveInto(&reservation, states, PinAll)
		if err != nil || !ok {
			t.Fatalf("reserve: ok=%v err=%v", ok, err)
		}
		if err := reservation.Abort(); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("TryReserveInto/Abort allocated %.2f objects per run", got)
	}

	seq := publishOne(t, r, states, 1<<1, "payload")
	var claim Claim
	if got := testing.AllocsPerRun(100, func() {
		status, err := r.TryClaimInto(&claim, states[1], 1, seq, func(uint64) bool { return true })
		if err != nil || status != ClaimReady {
			t.Fatalf("claim: status=%v err=%v", status, err)
		}
		if err := claim.Release(false); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("TryClaimInto/Release allocated %.2f objects per run", got)
	}
}
