package ring

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

type ClaimStatus uint8

const (
	ClaimReady ClaimStatus = iota
	ClaimNotTargeted
	ClaimStaleTarget
	ClaimReclaiming
	ClaimNotPublished
	ClaimLapped
	ClaimNotInstalled
	ClaimSourceChanged
	ClaimError
)

type Claim struct {
	ring       *Ring
	consumer   *ConsumerState
	sourcePort uint32
	targetBit  uint64
	sequence   uint64
	slotOffset uint64
	released   bool
}

type Reservation struct {
	ring                   *Ring
	sequence               uint64
	slotOffset             uint64
	oldGuard               uint64
	nextOffset             uint64
	recordCount            uint32
	targetUnion            uint64
	bufferExposed          bool
	frameBufferOutstanding bool
	done                   bool
}

// Sequence is the publication sequence claimed by this reservation. It lets
// a producer compare consumer cursors after publish and avoid redundant rings.
func (r *Reservation) Sequence() uint64 { return r.sequence }

type Frame struct {
	Bytes           []byte
	Flags           uint32
	DestinationMask uint64
}

type PinMaskFunc func(publishEpoch, oldTargets uint64) (uint64, error)

func PinAll(_ uint64, oldTargets uint64) (uint64, error) { return oldTargets, nil }

func (c *ConsumerState) recordClaimConflict(sourcePort uint32) {
	off, err := c.sourceOffset(sourcePort)
	if err == nil {
		_, _ = AddSeqCst(c.mem, off+ConsumerClaimConflictsOffset, 1)
	}
}

func (c *ConsumerState) RecordClaimRetryExhausted(sourcePort uint32) error {
	off, err := c.sourceOffset(sourcePort)
	if err != nil {
		return err
	}
	_, err = AddSeqCst(c.mem, off+ConsumerDropRetryOffset, 1)
	return err
}

// SkipClaimRetry advances past a sequence whose reclaim transition remained
// contested after the caller's bounded retry budget. The comparison matters:
// an event-loop peer may have advanced the cursor while the caller yielded,
// and a stale retry must never move it backwards.
func (c *ConsumerState) SkipClaimRetry(sourcePort uint32, sequence uint64) error {
	off, err := c.sourceOffset(sourcePort)
	if err != nil {
		return err
	}
	cursor, err := LoadSeqCst(c.mem, off+ConsumerCursorOffset)
	if err != nil {
		return err
	}
	if cursor != sequence {
		return nil
	}
	if _, err := AddSeqCst(c.mem, off+ConsumerDropRetryOffset, 1); err != nil {
		return err
	}
	return StoreSeqCst(c.mem, off+ConsumerCursorOffset, sequence+1)
}

func (r *Ring) RecordDoorbells(count uint64) error {
	_, err := AddSeqCst(r.mem, RingDoorbellsOffset, count)
	return err
}

// TryClaim is the convenient allocating form. Packet loops should use
// TryClaimInto and keep one caller-owned Claim instead of feeding the GC once
// per consumed slot.
func (r *Ring) TryClaim(c *ConsumerState, consumerPort uint32, sequence uint64,
	identityValid func(publishEpoch uint64) bool) (*Claim, ClaimStatus, error) {
	claim := new(Claim)
	status, err := r.TryClaimInto(claim, c, consumerPort, sequence, identityValid)
	if err != nil || status != ClaimReady {
		return nil, status, err
	}
	return claim, status, nil
}

// TryClaimInto snapshots routing metadata before taking an active payload
// claim. identityValid decides whether this consumer's bit at publishEpoch
// still denotes the current participant generation.
func (r *Ring) TryClaimInto(claim *Claim, c *ConsumerState, consumerPort uint32,
	sequence uint64, identityValid func(publishEpoch uint64) bool) (ClaimStatus, error) {
	if claim == nil {
		return ClaimError, fmt.Errorf("claim destination is nil")
	}
	if consumerPort >= MaxPorts {
		return ClaimError, fmt.Errorf("consumer port %d exceeds max %d", consumerPort, MaxPorts)
	}
	if identityValid == nil {
		return ClaimError, fmt.Errorf("identity validator is nil")
	}
	stateOff, err := c.sourceOffset(r.ownerPort)
	if err != nil {
		return ClaimError, err
	}
	generation, err := LoadSeqCst(c.mem, stateOff+ConsumerSourceGenerationOffset)
	if err != nil {
		return ClaimError, err
	}
	if generation == 0 {
		return ClaimNotInstalled, nil
	}
	if generation != uint64(r.ownerGeneration) {
		return ClaimSourceChanged, nil
	}
	slot := r.slotOffset(sequence)
	want := GuardPublished(sequence)
	guard1, err := LoadSeqCst(r.mem, slot+SlotGuardOffset)
	if err != nil {
		return ClaimError, err
	}
	if guard1 != want {
		return r.classifyMiss(c, stateOff, sequence, guard1)
	}
	mask, err := LoadSeqCst(r.mem, slot+SlotTargetUnionOffset)
	if err != nil {
		return ClaimError, err
	}
	epoch, err := LoadSeqCst(r.mem, slot+SlotPublishEpochOffset)
	if err != nil {
		return ClaimError, err
	}
	guard2, err := LoadSeqCst(r.mem, slot+SlotGuardOffset)
	if err != nil {
		return ClaimError, err
	}
	if guard2 != guard1 {
		return r.classifyMiss(c, stateOff, sequence, guard2)
	}
	if mask&(uint64(1)<<consumerPort) == 0 {
		if err := StoreSeqCst(c.mem, stateOff+ConsumerCursorOffset, sequence+1); err != nil {
			return ClaimError, err
		}
		return ClaimNotTargeted, nil
	}
	if !identityValid(epoch) {
		if err := StoreSeqCst(c.mem, stateOff+ConsumerCursorOffset, sequence+1); err != nil {
			return ClaimError, err
		}
		return ClaimStaleTarget, nil
	}

	if err := StoreSeqCst(c.mem, stateOff+ConsumerActiveSeqOffset, sequence); err != nil {
		return ClaimError, err
	}
	guard3, err := LoadSeqCst(r.mem, slot+SlotGuardOffset)
	if err != nil {
		_ = StoreSeqCst(c.mem, stateOff+ConsumerActiveSeqOffset, SequenceNone)
		return ClaimError, err
	}
	if guard3 != want {
		_ = StoreSeqCst(c.mem, stateOff+ConsumerActiveSeqOffset, SequenceNone)
		return r.classifyMiss(c, stateOff, sequence, guard3)
	}
	*claim = Claim{
		ring: r, consumer: c, sourcePort: r.ownerPort, targetBit: uint64(1) << consumerPort,
		sequence: sequence, slotOffset: slot,
	}
	return ClaimReady, nil
}

// classifyMiss also performs the cursor transition implied by a terminal
// miss. A same-sequence reclaim is transient and source changes are handled
// before reading a guard, so neither advances the cursor.
func (r *Ring) classifyMiss(c *ConsumerState, stateOff, sequence, guard uint64) (ClaimStatus, error) {
	observed := GuardSequence(guard)
	if observed == sequence && GuardIsReclaiming(guard) {
		c.recordClaimConflict(r.ownerPort)
		return ClaimReclaiming, nil
	}
	if observed < sequence {
		if err := StoreSeqCst(c.mem, stateOff+ConsumerCursorOffset, sequence+1); err != nil {
			return ClaimError, err
		}
		return ClaimNotPublished, nil
	}
	head := r.Head()
	oldest := uint64(1)
	if head > uint64(r.capacity) {
		oldest = head - uint64(r.capacity)
	}
	if oldest <= sequence {
		oldest = sequence + 1
	}
	if err := StoreSeqCst(c.mem, stateOff+ConsumerCursorOffset, oldest); err != nil {
		return ClaimError, err
	}
	return ClaimLapped, nil
}

func (c *Claim) Records(visit func(Frame) error) error {
	if c.released {
		return fmt.Errorf("claim already released")
	}
	payloadBytes := binary.LittleEndian.Uint32(c.ring.mem[c.slotOffset+SlotPayloadBytesOffset:])
	recordCount := binary.LittleEndian.Uint32(c.ring.mem[c.slotOffset+SlotRecordCountOffset:])
	if uint64(payloadBytes) > uint64(c.ring.slotBytes-SlotHeaderBytes) {
		return fmt.Errorf("slot payload length %d exceeds capacity", payloadBytes)
	}
	off := c.slotOffset + SlotHeaderBytes
	end := off + uint64(payloadBytes)
	for i := uint32(0); i < recordCount; i++ {
		if end-off < FrameHeaderBytes {
			return fmt.Errorf("record %d header exceeds slot payload", i)
		}
		length := binary.LittleEndian.Uint32(c.ring.mem[off+FrameLengthOffset:])
		flags := binary.LittleEndian.Uint32(c.ring.mem[off+FrameFlagsOffset:])
		mask := binary.LittleEndian.Uint64(c.ring.mem[off+FrameDestinationMaskOffset:])
		frameStart := off + FrameHeaderBytes
		frameEnd := frameStart + uint64(length)
		if frameEnd > end {
			return fmt.Errorf("record %d payload exceeds slot", i)
		}
		if mask&c.targetBit != 0 {
			if err := visit(Frame{Bytes: c.ring.mem[frameStart:frameEnd], Flags: flags, DestinationMask: mask}); err != nil {
				return err
			}
		}
		off += Align8(FrameHeaderBytes + uint64(length))
	}
	if off != end {
		return fmt.Errorf("records end at %d, slot payload ends at %d", off, end)
	}
	return nil
}

func (c *Claim) Release(advance bool) error {
	if c.released {
		return fmt.Errorf("claim already released")
	}
	off, err := c.consumer.sourceOffset(c.sourcePort)
	if err != nil {
		return err
	}
	if err := StoreSeqCst(c.consumer.mem, off+ConsumerActiveSeqOffset, SequenceNone); err != nil {
		return err
	}
	if advance {
		if err := StoreSeqCst(c.consumer.mem, off+ConsumerCursorOffset, c.sequence+1); err != nil {
			return err
		}
	}
	c.released = true
	return nil
}

// TryReserve is the convenient allocating form. Packet loops should use
// TryReserveInto and retain caller-owned storage.
func (r *Ring) TryReserve(states []*ConsumerState, pinMask PinMaskFunc) (*Reservation, bool, error) {
	reservation := new(Reservation)
	ok, err := r.TryReserveInto(reservation, states, pinMask)
	if err != nil || !ok {
		return nil, ok, err
	}
	return reservation, true, nil
}

// TryReserveInto closes a slot to new readers, filters old targets against
// that exact slot's publication epoch, and checks their active claims. A
// blocked physical slot is skipped by advancing head; the caller then
// drains/drops the input batch and rings live consumers so sleepers cross the
// missing sequence.
func (r *Ring) TryReserveInto(reservation *Reservation, states []*ConsumerState,
	pinMask PinMaskFunc) (bool, error) {
	if reservation == nil {
		return false, fmt.Errorf("reservation destination is nil")
	}
	if pinMask == nil {
		return false, fmt.Errorf("pin-mask callback is nil")
	}
	seq := r.Head()
	if seq == 0 || seq > GuardSequenceMaximum {
		return false, fmt.Errorf("invalid ring head %d", seq)
	}
	slot := r.slotOffset(seq)
	oldGuard, err := LoadSeqCst(r.mem, slot+SlotGuardOffset)
	if err != nil {
		return false, err
	}
	if GuardIsReclaiming(oldGuard) {
		return false, fmt.Errorf("single producer found slot already reclaiming")
	}
	oldSeq := GuardSequence(oldGuard)
	if err := StoreSeqCst(r.mem, slot+SlotGuardOffset, GuardReclaiming(oldSeq)); err != nil {
		return false, err
	}
	oldTargets, err := LoadSeqCst(r.mem, slot+SlotTargetUnionOffset)
	if err != nil {
		_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
		return false, err
	}
	oldEpoch, err := LoadSeqCst(r.mem, slot+SlotPublishEpochOffset)
	if err != nil {
		_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
		return false, err
	}
	pins, err := pinMask(oldEpoch, oldTargets)
	if err != nil {
		_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
		return false, err
	}
	for targets := oldTargets & pins; targets != 0; targets &= targets - 1 {
		port := uint32(bits.TrailingZeros64(targets))
		if int(port) >= len(states) || states[port] == nil {
			_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
			return false, fmt.Errorf("live target %d has no consumer state", port)
		}
		stateOff, err := states[port].sourceOffset(r.ownerPort)
		if err != nil {
			_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
			return false, err
		}
		generation, err := LoadSeqCst(states[port].mem, stateOff+ConsumerSourceGenerationOffset)
		if err != nil {
			_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
			return false, err
		}
		active, err := LoadSeqCst(states[port].mem, stateOff+ConsumerActiveSeqOffset)
		if err != nil {
			_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
			return false, err
		}
		if generation == uint64(r.ownerGeneration) && active == oldSeq && oldSeq != SequenceNone {
			_ = StoreSeqCst(r.mem, slot+SlotGuardOffset, oldGuard)
			_ = StoreSeqCst(r.mem, RingHeadOffset, seq+1)
			_, _ = AddSeqCst(r.mem, RingDropClaimedOffset, 1)
			return false, nil
		}
	}
	*reservation = Reservation{ring: r, sequence: seq, slotOffset: slot, oldGuard: oldGuard,
		nextOffset: SlotHeaderBytes}
	return true, nil
}

func (r *Reservation) Abort() error {
	if r.done {
		return fmt.Errorf("reservation already completed")
	}
	if r.bufferExposed {
		return fmt.Errorf("cannot restore old guard after exposing mutable slot bytes")
	}
	r.done = true
	return StoreSeqCst(r.ring.mem, r.slotOffset+SlotGuardOffset, r.oldGuard)
}

func (r *Reservation) Publish(epoch uint64, frames []Frame) error {
	if r.done {
		return fmt.Errorf("reservation already completed")
	}
	if epoch == 0 || epoch&1 != 0 {
		return fmt.Errorf("publish epoch %d is not a stable nonzero epoch", epoch)
	}
	if len(frames) > math.MaxUint32 {
		return fmt.Errorf("record count %d exceeds uint32", len(frames))
	}

	// Preflight the complete batch before touching payload.  If validation
	// failed after a partial write, Abort would restore the old guard over an
	// already-corrupted old slot.
	var payloadBytes uint64
	for i, frame := range frames {
		if uint64(len(frame.Bytes)) > math.MaxUint32 {
			return fmt.Errorf("frame %d length %d exceeds uint32", i, len(frame.Bytes))
		}
		recordBytes := Align8(FrameHeaderBytes + uint64(len(frame.Bytes)))
		if recordBytes > uint64(r.ring.slotBytes-SlotHeaderBytes)-payloadBytes {
			return fmt.Errorf("frame %d does not fit slot", i)
		}
		payloadBytes += recordBytes
	}

	for _, frame := range frames {
		buf, err := r.FrameBuffer()
		if err != nil || len(frame.Bytes) > len(buf) {
			return fmt.Errorf("preflight mismatch: %v", err)
		}
		copy(buf, frame.Bytes)
		if err := r.CommitFrame(uint32(len(frame.Bytes)), frame.Flags, frame.DestinationMask); err != nil {
			return fmt.Errorf("preflight mismatch: %w", err)
		}
	}
	return r.PublishDirect(epoch)
}

// FrameBuffer returns shared-memory storage suitable as a nonblocking recv
// destination. The slice is valid only until CommitFrame, Abort, or Publish.
func (r *Reservation) FrameBuffer() ([]byte, error) {
	if r.done {
		return nil, fmt.Errorf("reservation already completed")
	}
	available := uint64(r.ring.slotBytes) - r.nextOffset
	if available < FrameHeaderBytes {
		return nil, fmt.Errorf("slot has no room for another frame")
	}
	// A recv can fill the advertised buffer before CommitFrame validates it;
	// round down now so its mandatory 8-byte padding always fits too.
	recordCapacity := available &^ 7
	start := r.slotOffset + r.nextOffset + FrameHeaderBytes
	r.bufferExposed = true
	r.frameBufferOutstanding = true
	return r.ring.mem[start : start+recordCapacity-FrameHeaderBytes], nil
}

// CommitFrame commits bytes already received into the leading part of the last
// FrameBuffer. Once FrameBuffer has exposed mutable bytes, publishing is the
// only legal exit, even if the receive subsequently reports EAGAIN.
func (r *Reservation) CommitFrame(length, flags uint32, destinationMask uint64) error {
	if r.done {
		return fmt.Errorf("reservation already completed")
	}
	if !r.frameBufferOutstanding {
		return fmt.Errorf("no frame buffer is outstanding")
	}
	available := uint64(r.ring.slotBytes) - r.nextOffset
	recordCapacity := available &^ 7
	capacity := recordCapacity - FrameHeaderBytes
	if uint64(length) > capacity {
		return fmt.Errorf("frame length %d exceeds remaining capacity %d", length, capacity)
	}
	off := r.slotOffset + r.nextOffset
	recordBytes := Align8(FrameHeaderBytes + uint64(length))
	binary.LittleEndian.PutUint32(r.ring.mem[off+FrameLengthOffset:], length)
	binary.LittleEndian.PutUint32(r.ring.mem[off+FrameFlagsOffset:], flags)
	binary.LittleEndian.PutUint64(r.ring.mem[off+FrameDestinationMaskOffset:], destinationMask)
	clear(r.ring.mem[off+FrameHeaderBytes+uint64(length) : off+recordBytes])
	r.nextOffset += recordBytes
	r.recordCount++
	r.targetUnion |= destinationMask
	r.frameBufferOutstanding = false
	return nil
}

func (r *Reservation) PublishDirect(epoch uint64) error {
	if r.done {
		return fmt.Errorf("reservation already completed")
	}
	if epoch == 0 || epoch&1 != 0 {
		return fmt.Errorf("publish epoch %d is not a stable nonzero epoch", epoch)
	}
	payloadBytes := r.nextOffset - SlotHeaderBytes
	binary.LittleEndian.PutUint32(r.ring.mem[r.slotOffset+SlotPayloadBytesOffset:], uint32(payloadBytes))
	binary.LittleEndian.PutUint32(r.ring.mem[r.slotOffset+SlotRecordCountOffset:], r.recordCount)
	if err := StoreSeqCst(r.ring.mem, r.slotOffset+SlotPublishEpochOffset, epoch); err != nil {
		return err
	}
	if err := StoreSeqCst(r.ring.mem, r.slotOffset+SlotTargetUnionOffset, r.targetUnion); err != nil {
		return err
	}
	if err := StoreSeqCst(r.ring.mem, r.slotOffset+SlotGuardOffset, GuardPublished(r.sequence)); err != nil {
		return err
	}
	if err := StoreSeqCst(r.ring.mem, RingHeadOffset, r.sequence+1); err != nil {
		return err
	}
	if _, err := AddSeqCst(r.ring.mem, RingPublishedSlotsOffset, 1); err != nil {
		return err
	}
	r.done = true
	return nil
}
