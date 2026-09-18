package ring

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// Directory is a read-only logical view. Whether the underlying mapping is
// actually read-only remains a capability property enforced by mmap.
type Directory struct {
	mem []byte
}

// PortSnapshot and MembershipSnapshot are process-local, validated copies.
// They do not alias shared memory and remain stable after Snapshot returns.
type PortSnapshot struct {
	State          uint32
	Generation     uint32
	ChangedAtEpoch uint64
	MAC            uint64
	DataBytes      uint64
	StateBytes     uint64
	MTUFlags       uint64
}

func (p PortSnapshot) MTU() uint32   { return PortMTU(p.MTUFlags) }
func (p PortSnapshot) Flags() uint32 { return PortFlags(p.MTUFlags) }
func (p PortSnapshot) IsUplink() bool {
	return p.Flags()&PortFlagUplink != 0
}

type MembershipSnapshot struct {
	Epoch      uint64
	LiveMask   uint64
	UplinkMask uint64
	Ports      [MaxPorts]PortSnapshot
}

// TargetsFor applies the v1 role rules to one exact membership snapshot.
// Guests flood every other live port, including every uplink. Uplinks deliver
// only to live guests, never back to themselves or across to another uplink.
func (s MembershipSnapshot) TargetsFor(source uint32) (uint64, error) {
	if source >= MaxPorts {
		return 0, fmt.Errorf("source port %d exceeds max %d", source, MaxPorts)
	}
	bit := uint64(1) << source
	if s.LiveMask&bit == 0 {
		return 0, fmt.Errorf("source port %d is not live", source)
	}
	if s.Ports[source].IsUplink() {
		return s.LiveMask &^ s.UplinkMask, nil
	}
	return s.LiveMask &^ bit, nil
}

func OpenDirectory(mem []byte) (*Directory, error) {
	if uint64(len(mem)) != DirectoryRegionBytes() {
		return nil, fmt.Errorf("directory has %d bytes, want %d", len(mem), DirectoryRegionBytes())
	}
	if binary.LittleEndian.Uint64(mem[PrefixMagicOffset:]) != ABIMagic ||
		binary.LittleEndian.Uint32(mem[PrefixVersionOffset:]) != ABIVersion ||
		binary.LittleEndian.Uint32(mem[PrefixKindOffset:]) != RegionDirectory {
		return nil, fmt.Errorf("incompatible directory prefix")
	}
	if binary.LittleEndian.Uint32(mem[PrefixHeaderBytesOffset:]) != RegionHeaderBytes ||
		binary.LittleEndian.Uint32(mem[PrefixMaxPortsOffset:]) != MaxPorts ||
		binary.LittleEndian.Uint64(mem[PrefixRegionBytesOffset:]) != uint64(len(mem)) ||
		binary.LittleEndian.Uint32(mem[PrefixOwnerPortOffset:]) != PortNone ||
		binary.LittleEndian.Uint64(mem[PrefixOwnerGenerationOffset:]) != 0 ||
		binary.LittleEndian.Uint32(mem[PrefixCapacityOffset:]) != MaxPorts ||
		binary.LittleEndian.Uint32(mem[PrefixSlotBytesOffset:]) != DirectoryEntryBytes {
		return nil, fmt.Errorf("incompatible directory geometry")
	}
	return &Directory{mem: mem}, nil
}

// Epoch is the cheap cache-validation word for membership snapshots. An odd
// value means a coordinator update is in progress; callers needing entries
// must still use Snapshot.
func (d *Directory) Epoch() (uint64, error) {
	return LoadSeqCst(d.mem, DirectoryMembershipOffset)
}

// Snapshot retries a seqlock read at most retries+1 times, matching the C API.
// An even epoch that is unchanged after copying all entries names one exact
// membership rather than a mixture of before and after a mutation.
func (d *Directory) Snapshot(retries uint) (MembershipSnapshot, error) {
	for attempt := uint(0); ; attempt++ {
		before, err := LoadSeqCst(d.mem, DirectoryMembershipOffset)
		if err != nil {
			return MembershipSnapshot{}, err
		}
		if before&1 == 0 && before != 0 {
			var snap MembershipSnapshot
			snap.Epoch = before
			valid := true
			for port := uint32(0); port < MaxPorts; port++ {
				off := RegionHeaderBytes + uint64(port)*DirectoryEntryBytes
				identity, loadErr := LoadSeqCst(d.mem, off+DirectoryIdentityStateOffset)
				if loadErr != nil {
					return MembershipSnapshot{}, loadErr
				}
				p := &snap.Ports[port]
				p.State = IdentityPortState(identity)
				p.Generation = IdentityGeneration(identity)
				if p.State > PortRetired {
					valid = false
					break
				}
				loads := []struct {
					offset uint64
					dst    *uint64
				}{
					{DirectoryChangedEpochOffset, &p.ChangedAtEpoch},
					{DirectoryMACOffset, &p.MAC},
					{DirectoryDataBytesOffset, &p.DataBytes},
					{DirectoryStateBytesOffset, &p.StateBytes},
					{DirectoryMTUFlagsOffset, &p.MTUFlags},
				}
				for _, field := range loads {
					*field.dst, loadErr = LoadSeqCst(d.mem, off+field.offset)
					if loadErr != nil {
						return MembershipSnapshot{}, loadErr
					}
				}
				if p.Flags()&^PortFlagsMask != 0 {
					valid = false
					break
				}
				if p.State == PortLive {
					snap.LiveMask |= uint64(1) << port
					if p.IsUplink() {
						snap.UplinkMask |= uint64(1) << port
					}
				}
			}
			after, err := LoadSeqCst(d.mem, DirectoryMembershipOffset)
			if err != nil {
				return MembershipSnapshot{}, err
			}
			if valid && after == before {
				return snap, nil
			}
		}
		if attempt == retries {
			break
		}
	}
	return MembershipSnapshot{}, fmt.Errorf("directory changed during %d snapshot attempt(s)", retries+1)
}

// Pins returns the old targets whose process may still read the slot. DRAINING
// remains pinned because stopping new traffic says nothing about old claims.
// A currently LIVE identity pins only if it was already that identity at the
// slot's publication epoch; PREPARING, RETIRED and FREE cannot hold the old
// target identity.
func (s MembershipSnapshot) Pins(publishEpoch, oldTargets uint64) uint64 {
	pins := oldTargets
	for targets := oldTargets; targets != 0; targets &= targets - 1 {
		port := uint32(bits.TrailingZeros64(targets))
		p := s.Ports[port]
		if p.State == PortDraining ||
			(p.State == PortLive && p.ChangedAtEpoch != 0 && p.ChangedAtEpoch <= publishEpoch) {
			continue
		}
		pins &^= uint64(1) << port
	}
	return pins
}

// PinMask returns the callback shape consumed by Ring.TryReserve.
func (d *Directory) PinMask(retries uint) PinMaskFunc {
	return func(publishEpoch, oldTargets uint64) (uint64, error) {
		snap, err := d.Snapshot(retries)
		if err != nil {
			return 0, err
		}
		return snap.Pins(publishEpoch, oldTargets), nil
	}
}
