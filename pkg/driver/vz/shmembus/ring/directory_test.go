package ring

import (
	"encoding/binary"
	"testing"
)

func testDirectory(t *testing.T) ([]byte, *Directory) {
	t.Helper()
	mem := make([]byte, DirectoryRegionBytes())
	putPrefix(mem, RegionDirectory, uint64(len(mem)), PortNone, 0, 0, MaxPorts, DirectoryEntryBytes)
	if err := StoreSeqCst(mem, DirectoryMembershipOffset, 2); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDirectory(mem)
	if err != nil {
		t.Fatal(err)
	}
	return mem, d
}

func putDirectoryPort(t *testing.T, mem []byte, port, state, generation uint32,
	changed, mac uint64) {
	t.Helper()
	off := RegionHeaderBytes + uint64(port)*DirectoryEntryBytes
	values := []struct {
		offset uint64
		value  uint64
	}{
		{DirectoryChangedEpochOffset, changed},
		{DirectoryMACOffset, mac},
		{DirectoryDataBytesOffset, 4096 + uint64(port)},
		{DirectoryStateBytesOffset, 8192 + uint64(port)},
		{DirectoryMTUFlagsOffset, 1500},
		{DirectoryIdentityStateOffset, IdentityState(state, generation)},
	}
	for _, v := range values {
		if err := StoreSeqCst(mem, off+v.offset, v.value); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDirectorySnapshotAndPins(t *testing.T) {
	mem, d := testDirectory(t)
	if epoch, err := d.Epoch(); err != nil || epoch != 2 {
		t.Fatalf("epoch=%d err=%v", epoch, err)
	}
	putDirectoryPort(t, mem, 0, PortLive, 1, 2, 0x020000000001)
	putDirectoryPort(t, mem, 1, PortDraining, 2, 4, 0x020000000002)
	putDirectoryPort(t, mem, 2, PortRetired, 3, 4, 0)
	putDirectoryPort(t, mem, 3, PortPreparing, 4, 4, 0x020000000004)
	putDirectoryPort(t, mem, 4, PortLive, 5, 6, 0x020000000005)

	snap, err := d.Snapshot(0)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Epoch != 2 || snap.LiveMask != (1<<0)|(1<<4) {
		t.Fatalf("epoch=%d live=%#x", snap.Epoch, snap.LiveMask)
	}
	if p := snap.Ports[0]; p.Generation != 1 || p.MAC != 0x020000000001 ||
		p.DataBytes != 4096 || p.StateBytes != 8192 || p.MTUFlags != 1500 {
		t.Fatalf("port zero snapshot=%+v", p)
	}
	all := uint64(0x1f)
	if got, want := snap.Pins(2, all), uint64((1<<0)|(1<<1)); got != want {
		t.Fatalf("pins=%#x want=%#x", got, want)
	}
	pin := d.PinMask(0)
	if got, err := pin(2, all); err != nil || got != (1<<0)|(1<<1) {
		t.Fatalf("callback pins=%#x err=%v", got, err)
	}
}

func TestDirectorySnapshotRejectsMutationAndBadGeometry(t *testing.T) {
	mem, d := testDirectory(t)
	if err := StoreSeqCst(mem, DirectoryMembershipOffset, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Snapshot(2); err == nil {
		t.Fatal("snapshot accepted an in-flight odd epoch")
	}

	bad := append([]byte(nil), mem...)
	binary.LittleEndian.PutUint32(bad[PrefixSlotBytesOffset:], DirectoryEntryBytes+8)
	if _, err := OpenDirectory(bad); err == nil {
		t.Fatal("directory accepted incompatible entry stride")
	}
}
