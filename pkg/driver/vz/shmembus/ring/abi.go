package ring

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// These constants mirror include/bus_abi.h, which is the normative layout.
// TestLayoutManifest compares the complete Go and C manifests without cgo.
const (
	ABIVersion = 2
	ABIMagic   = uint64(0x315355424d48534c)

	CacheLineBytes       = 128
	MaxPorts             = 64
	PortNone             = ^uint32(0)
	RegionPrefixBytes    = 128
	RegionHeaderBytes    = 256
	SlotHeaderBytes      = 128
	FrameHeaderBytes     = 16
	ConsumerSourceBytes  = 128
	DirectoryEntryBytes  = 128
	DefaultCapacity      = 256
	DefaultPayloadBytes  = 128 * 1024
	DefaultSlotBytes     = SlotHeaderBytes + DefaultPayloadBytes
	DefaultMTU           = 1500
	PortFlagUplink       = uint32(1)
	PortFlagsMask        = PortFlagUplink
	SequenceNone         = uint64(0)
	GuardReclaimingBit   = uint64(1)
	GuardSequenceMaximum = ^uint64(0) >> 1
)

const (
	RegionProducer  = 1
	RegionConsumer  = 2
	RegionDirectory = 3
)

const (
	PortFree      = 0
	PortPreparing = 1
	PortLive      = 2
	PortDraining  = 3
	PortRetired   = 4
)

// Explicit ABI offsets. Never cast an mmap to a Go struct: that would make Go
// field layout part of a cross-language protocol and obscure alignment checks.
const (
	PrefixMagicOffset           = 0
	PrefixVersionOffset         = 8
	PrefixKindOffset            = 12
	PrefixHeaderBytesOffset     = 16
	PrefixMaxPortsOffset        = 20
	PrefixRegionBytesOffset     = 24
	PrefixOwnerGenerationOffset = 32
	PrefixOwnerPortOffset       = 40
	PrefixMTUOffset             = 44
	PrefixCapacityOffset        = 48
	PrefixSlotBytesOffset       = 52
	PrefixFlagsOffset           = 56

	RingHeadOffset           = 128
	RingPublishedSlotsOffset = 136
	RingDropClaimedOffset    = 144
	RingDoorbellsOffset      = 152

	SlotGuardOffset        = 0
	SlotPublishEpochOffset = 8
	SlotTargetUnionOffset  = 16
	SlotPayloadBytesOffset = 24
	SlotRecordCountOffset  = 28

	FrameLengthOffset          = 0
	FrameFlagsOffset           = 4
	FrameDestinationMaskOffset = 8

	ConsumerSourceGenerationOffset    = 0
	ConsumerCursorOffset              = 8
	ConsumerActiveSeqOffset           = 16
	ConsumerClaimConflictsOffset      = 32
	ConsumerDropRetryOffset           = 40
	ConsumerInstalledGenerationOffset = 48
	ConsumerPreparedGenerationOffset  = 136

	DirectoryIdentityStateOffset = 0
	DirectoryChangedEpochOffset  = 8
	DirectoryMACOffset           = 16
	DirectoryDataBytesOffset     = 24
	DirectoryStateBytesOffset    = 32
	DirectoryMTUFlagsOffset      = 40
	DirectoryMembershipOffset    = 128
)

func GuardPublished(seq uint64) uint64  { return seq << 1 }
func GuardReclaiming(seq uint64) uint64 { return seq<<1 | GuardReclaimingBit }
func GuardSequence(guard uint64) uint64 { return guard >> 1 }
func GuardIsReclaiming(guard uint64) bool {
	return guard&GuardReclaimingBit != 0
}
func Align8(n uint64) uint64 { return (n + 7) &^ 7 }
func IdentityState(state, generation uint32) uint64 {
	return uint64(state) | uint64(generation)<<32
}
func IdentityPortState(identity uint64) uint32  { return uint32(identity) }
func IdentityGeneration(identity uint64) uint32 { return uint32(identity >> 32) }
func PackMTUFlags(mtu, flags uint32) uint64 {
	return uint64(mtu) | uint64(flags)<<32
}
func PortMTU(mtuFlags uint64) uint32   { return uint32(mtuFlags) }
func PortFlags(mtuFlags uint64) uint32 { return uint32(mtuFlags >> 32) }
func RingRegionBytes(capacity, slotBytes uint32) uint64 {
	return RegionHeaderBytes + uint64(capacity)*uint64(slotBytes)
}
func ConsumerRegionBytes() uint64 {
	return RegionHeaderBytes + MaxPorts*ConsumerSourceBytes
}
func DirectoryRegionBytes() uint64 {
	return RegionHeaderBytes + MaxPorts*DirectoryEntryBytes
}

func atomicWord(region []byte, off uint64) (*uint64, error) {
	if off > uint64(len(region)) || uint64(len(region))-off < 8 {
		return nil, fmt.Errorf("atomic word at offset %d exceeds %d-byte region", off, len(region))
	}
	p := unsafe.Pointer(&region[off])
	if uintptr(p)%8 != 0 {
		return nil, fmt.Errorf("atomic word at offset %d is not 8-byte aligned", off)
	}
	return (*uint64)(p), nil
}

func LoadSeqCst(region []byte, off uint64) (uint64, error) {
	p, err := atomicWord(region, off)
	if err != nil {
		return 0, err
	}
	return atomic.LoadUint64(p), nil
}

func StoreSeqCst(region []byte, off, value uint64) error {
	p, err := atomicWord(region, off)
	if err != nil {
		return err
	}
	atomic.StoreUint64(p, value)
	return nil
}

func AddSeqCst(region []byte, off, delta uint64) (uint64, error) {
	p, err := atomicWord(region, off)
	if err != nil {
		return 0, err
	}
	return atomic.AddUint64(p, delta), nil
}

func PutUint32(region []byte, off uint64, value uint32) error {
	if off > uint64(len(region)) || uint64(len(region))-off < 4 {
		return fmt.Errorf("uint32 at offset %d exceeds %d-byte region", off, len(region))
	}
	binary.LittleEndian.PutUint32(region[off:off+4], value)
	return nil
}

func PutUint64(region []byte, off, value uint64) error {
	if off > uint64(len(region)) || uint64(len(region))-off < 8 {
		return fmt.Errorf("uint64 at offset %d exceeds %d-byte region", off, len(region))
	}
	binary.LittleEndian.PutUint64(region[off:off+8], value)
	return nil
}
