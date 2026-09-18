package ring

import "testing"

func TestGuardEncoding(t *testing.T) {
	for _, seq := range []uint64{1, 2, 17, 1 << 32, GuardSequenceMaximum} {
		p := GuardPublished(seq)
		r := GuardReclaiming(seq)
		if GuardSequence(p) != seq || GuardSequence(r) != seq {
			t.Fatalf("sequence round trip failed for %d", seq)
		}
		if GuardIsReclaiming(p) || !GuardIsReclaiming(r) {
			t.Fatalf("state bit failed for %d", seq)
		}
	}
}

func TestIdentityEncoding(t *testing.T) {
	identity := IdentityState(PortDraining, 0xfedcba98)
	if got := IdentityPortState(identity); got != PortDraining {
		t.Fatalf("state=%d", got)
	}
	if got := IdentityGeneration(identity); got != 0xfedcba98 {
		t.Fatalf("generation=%#x", got)
	}
}

func TestGeometry(t *testing.T) {
	if DefaultSlotBytes%CacheLineBytes != 0 {
		t.Fatalf("default slot stride %d is not cache-line integral", DefaultSlotBytes)
	}
	if got, want := ConsumerRegionBytes(), uint64(8448); got != want {
		t.Fatalf("consumer region: got %d want %d", got, want)
	}
	if got, want := DirectoryRegionBytes(), uint64(8448); got != want {
		t.Fatalf("directory region: got %d want %d", got, want)
	}
}

func TestConsumerInstallationLedger(t *testing.T) {
	mem := make([]byte, ConsumerRegionBytes())
	c, err := InitConsumerState(mem, 3, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.InstallSource(7, 19, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkSourceInstalled(7, 18); err == nil {
		t.Fatal("marked a generation other than the installed source")
	}
	if err := c.MarkSourceInstalled(7, 19); err != nil {
		t.Fatal(err)
	}
	if got, err := c.InstalledGeneration(7); err != nil || got != 19 {
		t.Fatalf("installed generation=%d err=%v", got, err)
	}
	off, _ := c.sourceOffset(7)
	if err := StoreSeqCst(c.mem, off+ConsumerCursorOffset, 77); err != nil {
		t.Fatal(err)
	}
	if err := c.InstallSource(7, 19, 99); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Cursor(7); err != nil || got != 77 {
		t.Fatalf("duplicate install reset cursor to %d, err=%v", got, err)
	}
	if err := c.MarkPrepared(10); err == nil {
		t.Fatal("prepared a generation other than the region owner")
	}
	if err := c.MarkPrepared(11); err != nil {
		t.Fatal(err)
	}
	if got, err := c.PreparedGeneration(); err != nil || got != 11 {
		t.Fatalf("prepared generation=%d err=%v", got, err)
	}
}

func TestAtomicBoundsAndAlignment(t *testing.T) {
	b := make([]byte, RegionHeaderBytes+8)
	if err := StoreSeqCst(b, RingHeadOffset, 42); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadSeqCst(b, RingHeadOffset); err != nil || got != 42 {
		t.Fatalf("load got %d, %v", got, err)
	}
	if _, err := LoadSeqCst(b, 1); err == nil {
		t.Fatal("misaligned atomic unexpectedly accepted")
	}
	if _, err := LoadSeqCst(b, uint64(len(b)-7)); err == nil {
		t.Fatal("out-of-bounds atomic unexpectedly accepted")
	}
}
