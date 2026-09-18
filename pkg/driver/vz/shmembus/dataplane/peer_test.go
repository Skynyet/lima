package dataplane

import "testing"

func TestAcceptsEthernetDestination(t *testing.T) {
	frame := make([]byte, 14)
	put := func(mac uint64) {
		for i := 0; i < 6; i++ {
			frame[i] = byte(mac >> uint(8*(5-i)))
		}
	}
	put(0x020000000002)
	if !accepts(frame, 0x020000000002) || accepts(frame, 0x020000000001) {
		t.Fatal("unicast filter did not match only the registered MAC")
	}
	put(0xffffffffffff)
	if !accepts(frame, 0x020000000001) {
		t.Fatal("broadcast was filtered")
	}
	put(0x01005e000001)
	if !accepts(frame, 0x020000000001) {
		t.Fatal("multicast was filtered")
	}
}
