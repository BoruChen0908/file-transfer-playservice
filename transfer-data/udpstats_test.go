package transferdata

import (
	"runtime"
	"testing"
)

func TestUDPCountersOnLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc/net/snmp only exists on Linux")
	}
	counters, err := UDPCounters()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"InDatagrams", "RcvbufErrors", "SndbufErrors"} {
		if _, found := counters[name]; !found {
			t.Fatalf("no %v in %v", name, counters)
		}
	}
}
