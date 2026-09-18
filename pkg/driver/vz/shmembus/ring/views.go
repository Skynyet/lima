package ring

import (
	"encoding/binary"
	"fmt"
)

type Ring struct {
	mem             []byte
	capacity        uint32
	slotBytes       uint32
	ownerPort       uint32
	ownerGeneration uint32
}

type ConsumerState struct {
	mem []byte
}

func InitRing(mem []byte, ownerPort, ownerGeneration, mtu, capacity, slotBytes uint32) (*Ring, error) {
	need := RingRegionBytes(capacity, slotBytes)
	if capacity == 0 || slotBytes < SlotHeaderBytes+FrameHeaderBytes || slotBytes%CacheLineBytes != 0 {
		return nil, fmt.Errorf("invalid ring geometry capacity=%d slotBytes=%d", capacity, slotBytes)
	}
	if uint64(len(mem)) != need {
		return nil, fmt.Errorf("ring has %d bytes, geometry requires %d", len(mem), need)
	}
	clear(mem)
	putPrefix(mem, RegionProducer, uint64(len(mem)), ownerPort, ownerGeneration, mtu, capacity, slotBytes)
	if err := StoreSeqCst(mem, RingHeadOffset, 1); err != nil {
		return nil, err
	}
	return OpenRing(mem)
}

func OpenRing(mem []byte) (*Ring, error) {
	if err := validatePrefix(mem, RegionProducer); err != nil {
		return nil, err
	}
	capacity := binary.LittleEndian.Uint32(mem[PrefixCapacityOffset:])
	slotBytes := binary.LittleEndian.Uint32(mem[PrefixSlotBytesOffset:])
	if capacity == 0 || slotBytes < SlotHeaderBytes+FrameHeaderBytes || slotBytes%CacheLineBytes != 0 {
		return nil, fmt.Errorf("invalid ring geometry capacity=%d slotBytes=%d", capacity, slotBytes)
	}
	if want := RingRegionBytes(capacity, slotBytes); want != uint64(len(mem)) {
		return nil, fmt.Errorf("ring bytes=%d, header geometry=%d", len(mem), want)
	}
	head, err := LoadSeqCst(mem, RingHeadOffset)
	if err != nil || head == 0 || head > GuardSequenceMaximum {
		return nil, fmt.Errorf("invalid ring head %d", head)
	}
	ownerGeneration64 := binary.LittleEndian.Uint64(mem[PrefixOwnerGenerationOffset:])
	if ownerGeneration64 > uint64(^uint32(0)) {
		return nil, fmt.Errorf("owner generation %d exceeds uint32", ownerGeneration64)
	}
	return &Ring{
		mem:             mem,
		capacity:        capacity,
		slotBytes:       slotBytes,
		ownerPort:       binary.LittleEndian.Uint32(mem[PrefixOwnerPortOffset:]),
		ownerGeneration: uint32(ownerGeneration64),
	}, nil
}

func InitConsumerState(mem []byte, ownerPort, ownerGeneration uint32) (*ConsumerState, error) {
	if uint64(len(mem)) != ConsumerRegionBytes() {
		return nil, fmt.Errorf("consumer state has %d bytes, want %d", len(mem), ConsumerRegionBytes())
	}
	clear(mem)
	putPrefix(mem, RegionConsumer, uint64(len(mem)), ownerPort, ownerGeneration, 0, MaxPorts, ConsumerSourceBytes)
	return OpenConsumerState(mem)
}

func OpenConsumerState(mem []byte) (*ConsumerState, error) {
	if err := validatePrefix(mem, RegionConsumer); err != nil {
		return nil, err
	}
	if uint64(len(mem)) != ConsumerRegionBytes() {
		return nil, fmt.Errorf("consumer state has %d bytes, want %d", len(mem), ConsumerRegionBytes())
	}
	return &ConsumerState{mem: mem}, nil
}

func (r *Ring) Head() uint64 {
	v, err := LoadSeqCst(r.mem, RingHeadOffset)
	if err != nil {
		panic(err)
	}
	return v
}

func (r *Ring) slotOffset(seq uint64) uint64 {
	return RegionHeaderBytes + ((seq - 1) % uint64(r.capacity) * uint64(r.slotBytes))
}

func (r *Ring) slotGuard(seq uint64) uint64 {
	v, err := LoadSeqCst(r.mem, r.slotOffset(seq)+SlotGuardOffset)
	if err != nil {
		panic(err)
	}
	return v
}

func (c *ConsumerState) sourceOffset(sourcePort uint32) (uint64, error) {
	if sourcePort >= MaxPorts {
		return 0, fmt.Errorf("source port %d exceeds max %d", sourcePort, MaxPorts)
	}
	return RegionHeaderBytes + uint64(sourcePort)*ConsumerSourceBytes, nil
}

// InstallSource starts a new source identity at head. Reinstalling a generation
// already recorded as installed is an idempotent no-op and preserves cursor.
func (c *ConsumerState) InstallSource(sourcePort, generation uint32, head uint64) error {
	off, err := c.sourceOffset(sourcePort)
	if err != nil {
		return err
	}
	installed, err := LoadSeqCst(c.mem, off+ConsumerInstalledGenerationOffset)
	if err != nil {
		return err
	}
	if installed == uint64(generation) && generation != 0 {
		current, err := LoadSeqCst(c.mem, off+ConsumerSourceGenerationOffset)
		if err != nil {
			return err
		}
		if current != installed {
			return fmt.Errorf("source %d has installed generation %d but source generation %d",
				sourcePort, installed, current)
		}
		return nil
	}
	if err := StoreSeqCst(c.mem, off+ConsumerActiveSeqOffset, SequenceNone); err != nil {
		return err
	}
	if err := StoreSeqCst(c.mem, off+ConsumerCursorOffset, head); err != nil {
		return err
	}
	return StoreSeqCst(c.mem, off+ConsumerSourceGenerationOffset, uint64(generation))
}

// MarkSourceInstalled records that this consumer holds the complete capability
// bundle for one concrete source identity. The directory generation must still
// be compared by every reader; this word is a fact, not permission by itself.
func (c *ConsumerState) MarkSourceInstalled(sourcePort, generation uint32) error {
	if generation == 0 {
		return fmt.Errorf("installed source generation is zero")
	}
	off, err := c.sourceOffset(sourcePort)
	if err != nil {
		return err
	}
	sourceGeneration, err := LoadSeqCst(c.mem, off+ConsumerSourceGenerationOffset)
	if err != nil {
		return err
	}
	if sourceGeneration != uint64(generation) {
		return fmt.Errorf("source %d generation %d, cannot mark %d installed",
			sourcePort, sourceGeneration, generation)
	}
	return StoreSeqCst(c.mem, off+ConsumerInstalledGenerationOffset, uint64(generation))
}

func (c *ConsumerState) InstalledGeneration(sourcePort uint32) (uint64, error) {
	off, err := c.sourceOffset(sourcePort)
	if err != nil {
		return 0, err
	}
	return LoadSeqCst(c.mem, off+ConsumerInstalledGenerationOffset)
}

// MarkPrepared records that all capability bundles required by this consumer
// generation are installed. The coordinator still evaluates the complete
// readiness matrix before publishing LIVE.
func (c *ConsumerState) MarkPrepared(generation uint32) error {
	if generation == 0 {
		return fmt.Errorf("prepared generation is zero")
	}
	ownerGeneration := binary.LittleEndian.Uint64(c.mem[PrefixOwnerGenerationOffset:])
	if ownerGeneration != uint64(generation) {
		return fmt.Errorf("consumer generation %d, cannot prepare %d", ownerGeneration, generation)
	}
	return StoreSeqCst(c.mem, ConsumerPreparedGenerationOffset, uint64(generation))
}

func (c *ConsumerState) PreparedGeneration() (uint64, error) {
	return LoadSeqCst(c.mem, ConsumerPreparedGenerationOffset)
}

func (c *ConsumerState) Cursor(sourcePort uint32) (uint64, error) {
	off, err := c.sourceOffset(sourcePort)
	if err != nil {
		return 0, err
	}
	return LoadSeqCst(c.mem, off+ConsumerCursorOffset)
}

func putPrefix(mem []byte, kind uint32, regionBytes uint64, ownerPort, ownerGeneration uint32,
	mtu, capacity, slotBytes uint32) {
	binary.LittleEndian.PutUint64(mem[PrefixMagicOffset:], ABIMagic)
	binary.LittleEndian.PutUint32(mem[PrefixVersionOffset:], ABIVersion)
	binary.LittleEndian.PutUint32(mem[PrefixKindOffset:], kind)
	binary.LittleEndian.PutUint32(mem[PrefixHeaderBytesOffset:], RegionHeaderBytes)
	binary.LittleEndian.PutUint32(mem[PrefixMaxPortsOffset:], MaxPorts)
	binary.LittleEndian.PutUint64(mem[PrefixRegionBytesOffset:], regionBytes)
	binary.LittleEndian.PutUint64(mem[PrefixOwnerGenerationOffset:], uint64(ownerGeneration))
	binary.LittleEndian.PutUint32(mem[PrefixOwnerPortOffset:], ownerPort)
	binary.LittleEndian.PutUint32(mem[PrefixMTUOffset:], mtu)
	binary.LittleEndian.PutUint32(mem[PrefixCapacityOffset:], capacity)
	binary.LittleEndian.PutUint32(mem[PrefixSlotBytesOffset:], slotBytes)
}

func validatePrefix(mem []byte, kind uint32) error {
	if len(mem) < RegionHeaderBytes {
		return fmt.Errorf("region has %d bytes, shorter than header", len(mem))
	}
	if binary.LittleEndian.Uint64(mem[PrefixMagicOffset:]) != ABIMagic {
		return fmt.Errorf("bad ABI magic")
	}
	if binary.LittleEndian.Uint32(mem[PrefixVersionOffset:]) != ABIVersion {
		return fmt.Errorf("unsupported ABI version")
	}
	if got := binary.LittleEndian.Uint32(mem[PrefixKindOffset:]); got != kind {
		return fmt.Errorf("region kind %d, want %d", got, kind)
	}
	if binary.LittleEndian.Uint64(mem[PrefixRegionBytesOffset:]) != uint64(len(mem)) {
		return fmt.Errorf("region length disagrees with header")
	}
	if binary.LittleEndian.Uint32(mem[PrefixHeaderBytesOffset:]) != RegionHeaderBytes ||
		binary.LittleEndian.Uint32(mem[PrefixMaxPortsOffset:]) != MaxPorts {
		return fmt.Errorf("incompatible region geometry")
	}
	if binary.LittleEndian.Uint32(mem[PrefixOwnerPortOffset:]) >= MaxPorts ||
		binary.LittleEndian.Uint64(mem[PrefixOwnerGenerationOffset:]) > uint64(^uint32(0)) {
		return fmt.Errorf("invalid owner identity")
	}
	return nil
}
