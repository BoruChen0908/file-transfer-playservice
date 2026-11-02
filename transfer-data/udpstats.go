package transferdata

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// UDPCounters reads the kernel's UDP counters for this network namespace (one per Fargate
// task) from /proc/net/snmp, for example InDatagrams and RcvbufErrors (datagrams dropped
// because the socket's receive buffer was full).
//
// Returns: the counters by name.
// Errors: an error when the file is missing (it only exists on Linux) or cannot be parsed.
// Side effects: reads one file.
func UDPCounters() (map[string]int64, error) {
	data, err := os.ReadFile("/proc/net/snmp")
	if err != nil {
		return nil, err
	}
	var header []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "Udp:" {
			continue
		}
		if header == nil {
			header = fields[1:]
			continue
		}
		counters := make(map[string]int64, len(header))
		for i, name := range header {
			if i+1 < len(fields) {
				counters[name], _ = strconv.ParseInt(fields[i+1], 10, 64)
			}
		}
		return counters, nil
	}
	return nil, fmt.Errorf("no Udp lines in /proc/net/snmp")
}

// LogUDPCounters prints the UDP counters as one "UDPSTATS" line every interval, skipping
// intervals in which they did not change. It returns at once when the counters cannot be
// read (not on Linux), so it is safe to start on any machine.
//
// Side effects: runs forever; meant to be started with go.
func LogUDPCounters(interval time.Duration) {
	last := ""
	for {
		counters, err := UDPCounters()
		if err != nil {
			return
		}
		line := fmt.Sprintf("UDPSTATS in_datagrams=%v out_datagrams=%v rcvbuf_errors=%v sndbuf_errors=%v in_errors=%v no_ports=%v",
			counters["InDatagrams"], counters["OutDatagrams"], counters["RcvbufErrors"], counters["SndbufErrors"],
			counters["InErrors"], counters["NoPorts"])
		if line != last {
			fmt.Fprintln(os.Stderr, line)
			last = line
		}
		time.Sleep(interval)
	}
}
