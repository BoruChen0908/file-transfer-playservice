package udp

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	transferdata "example.com/go-file-transfer/transfer-data"
)

// How long the server may stay quiet before the client decides it is done sending,
// how many rounds of RETY requests the client makes before giving up, and the largest
// NUM_CHUNKS a datagram may announce (65536 chunks of 64 bytes is 4 MiB). The cap stops a
// buggy or hostile datagram from making the client request and store millions of chunks.
// maxOpenAttempts is how many times OPEN is sent, on a fresh socket each time, when nothing
// comes back. With 2s timeouts, 5 OPEN attempts plus 5 retry rounds wait at most about 20s,
// well inside a load balancer's 60s idle timeout.
const (
	maxRetryRounds  = 5
	maxOpenAttempts = 5
	maxChunks       = 65536
)

// readTimeout is how long the server may stay quiet. It is a variable only so tests can
// shorten it.
var readTimeout = 2 * time.Second

// ServerError is returned when the server answers a request with "ERR <message>".
// Message holds the text after "ERR " (for example "file-not-found").
type ServerError struct {
	Message string
}

func (serverError *ServerError) Error() string {
	return "server error: " + serverError.Message
}

var (
	// ErrNoData means no valid file datagram arrived before the read timeout. This is also
	// what an empty file looks like, since the protocol sends nothing for it.
	ErrNoData = errors.New("no file data received from the server")
	// ErrIncomplete means chunks were still missing after maxRetryRounds rounds of RETY.
	ErrIncomplete = errors.New("gave up with chunks still missing")
)

// RunClient connects to an FTP_6650 server over UDP and downloads one file.
//
// Inputs: args holds the optional "-host=XXX" (default "localhost") and "-port=YYY"
// (default "8080") options, followed by the required file name as the LAST argument.
// Returns: nothing; on success the file is saved in the current directory.
// Errors: any failure to fetch or to save the file ends the program through log.Fatal.
// Empty args panics, since the file name is required.
// Side effects: everything Fetch does (network traffic, stderr logging), then creates or
// overwrites the file named by the last argument.
func RunClient(args []string) {

	filename := args[len(args)-1]
	//host := "ec2-44-247-74-97.us-west-2.compute.amazonaws.com"
	host := "localhost"
	port := "8080"

	if len(args) > 1 {
		args = args[:len(args)-1]
		for _, option := range args {
			if strings.Contains(option, "-host=") {
				host = strings.Split(option, "=")[1]
			} else if strings.Contains(option, "-port=") {
				port = strings.Split(option, "=")[1]
			}
		}
	}

	file_data, err := Fetch(net.JoinHostPort(host, port), filename)
	if err != nil {
		log.Fatal(err)
	}

	err = storeDataIntoFile(filename, file_data)
	if err != nil {
		log.Fatalf("Error in storing file: %v", err)
	}
}

// Fetch runs one FTP_6650 download and returns the file's bytes. It sends OPEN, collects
// the file datagrams until the server goes quiet, asks for any missing chunks with RETY,
// and recombines the chunks. It never exits the program and never touches the disk, so it
// is safe to call from a long-running program such as a web server, and from many
// goroutines at once (every call uses its own UDP socket).
//
// Inputs: address is the server as "host:port"; filename is the file to request. The name
// must not contain spaces, since the protocol separates fields with spaces.
// Returns: the complete contents of the file.
// The address may be a load balancer. Each socket is dialed to one address picked at random
// from the name's IPv4 addresses (a load balancer has one per zone), and a new socket has a
// new source port, which a load balancer may send to a different server. Every server can
// answer every request, so the client re-dials when the server it is talking to goes quiet.
//
// Errors: a *ServerError when the server replies "ERR <message>"; ErrNoData when no file
// data arrives after maxOpenAttempts attempts (an empty file looks the same, so it takes
// that long to report one); an error wrapping ErrIncomplete (listing the missing IDs) when chunks are
// still missing after maxRetryRounds rounds; and wrapped network errors when the address
// cannot be resolved, the socket cannot be opened, or a write or read fails for a reason
// other than a timeout.
// Side effects: opens and closes a UDP socket, writes OPEN and RETY datagrams, sets read
// deadlines on that socket, and logs progress to stderr.
func Fetch(address string, filename string) ([]byte, error) {
	stats := fetchStats{start: time.Now(), rcvbufErrors: rcvbufErrors()}
	file_data, err := fetch(address, filename, &stats)
	stats.rcvbufErrors = rcvbufErrors() - stats.rcvbufErrors
	stats.log(filename, err)
	return file_data, err
}

// fetchStats summarizes one download for the experiments: how many OPENs it took, how many
// chunks were missing after the first pass and before each RETY round, and how it ended.
type fetchStats struct {
	start            time.Time
	opens            int
	numChunks        int
	missingAfterOpen int
	missingByRound   []int
	retysSent        int
	finalMissing     int
	// rcvbufErrors is how many datagrams the kernel dropped during this download because a
	// socket's receive buffer was full (for the whole task, so it includes any download
	// running at the same time). -1 when the counter cannot be read.
	rcvbufErrors int64
}

// rcvbufErrors returns the task's UDP RcvbufErrors counter, or -1 when it cannot be read.
func rcvbufErrors() int64 {
	counters, err := transferdata.UDPCounters()
	if err != nil {
		return -1
	}
	return counters["RcvbufErrors"]
}

// log writes the summary as one "STATS key=value ..." line to stderr (the gateway's log).
func (stats *fetchStats) log(filename string, err error) {
	var serverError *ServerError
	result := "ok"
	switch {
	case errors.Is(err, ErrIncomplete):
		result = "incomplete"
	case errors.Is(err, ErrNoData):
		result = "nodata"
	case errors.As(err, &serverError):
		result = "server-error"
	case err != nil:
		result = "error"
	}
	fmt.Fprintf(os.Stderr, "STATS file=%v chunks=%v opens=%v missing_after_open=%v rounds=%v retys=%v missing_by_round=%v final_missing=%v rcvbuf_errors=%v dur_ms=%v result=%v\n",
		filename, stats.numChunks, stats.opens, stats.missingAfterOpen, len(stats.missingByRound), stats.retysSent,
		strings.Join(strings.Fields(fmt.Sprint(stats.missingByRound)), ","), stats.finalMissing, stats.rcvbufErrors,
		time.Since(stats.start).Milliseconds(), result)
}

// fetch is the body of Fetch; it fills in stats as it goes.
func fetch(address string, filename string, stats *fetchStats) ([]byte, error) {
	var conn *net.UDPConn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	chunks := make(map[int]transferdata.TCP_Pack)
	num_chunks := 0
	for attempt := 0; attempt < maxOpenAttempts && num_chunks == 0; attempt++ {
		var err error
		conn, err = redial(conn, address)
		if err != nil {
			return nil, err
		}
		stats.opens++
		_, err = fmt.Fprintf(conn, "OPEN %v", filename)
		if err != nil {
			return nil, fmt.Errorf("sending OPEN: %w", err)
		}
		num_chunks, err = readChunks(conn, chunks)
		if err != nil {
			return nil, err
		}
	}
	if num_chunks == 0 {
		return nil, ErrNoData
	}
	stats.numChunks = num_chunks

	silent := false
	for round := 0; round < maxRetryRounds; round++ {
		missing := missingChunkIds(chunks, num_chunks)
		if round == 0 {
			stats.missingAfterOpen = len(missing)
		}
		if len(missing) == 0 {
			break
		}
		stats.missingByRound = append(stats.missingByRound, len(missing))
		stats.retysSent += len(missing)
		if silent {
			// The last round brought nothing back: that server may be gone or the flow dead.
			var err error
			conn, err = redial(conn, address)
			if err != nil {
				return nil, err
			}
		}
		fmt.Fprintf(os.Stderr, "Missing chunks %v, retrying\n", missing)
		for _, id := range missing {
			_, err := fmt.Fprintf(conn, "RETY %v %v", filename, id)
			if err != nil {
				return nil, fmt.Errorf("sending RETY: %w", err)
			}
		}
		before := len(chunks)
		_, err := readChunks(conn, chunks)
		if err != nil {
			return nil, err
		}
		silent = len(chunks) == before
	}

	missing := missingChunkIds(chunks, num_chunks)
	stats.finalMissing = len(missing)
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %v", ErrIncomplete, missing)
	}

	packs := make([]transferdata.TCP_Pack, num_chunks)
	for id := 1; id <= num_chunks; id++ {
		packs[id-1] = chunks[id]
	}
	return transferdata.RecombineData(packs), nil
}

// redial closes old (if not nil) and opens a new UDP socket to one of address's IPv4
// addresses, picked at random.
//
// Inputs: old is the socket to replace, or nil; address is "host:port".
// Returns: the new connected socket.
// Errors: wrapped errors when the address cannot be resolved or the socket cannot be opened.
// Side effects: closes old, performs a DNS lookup and opens a socket.
func redial(old *net.UDPConn, address string) (*net.UDPConn, error) {
	if old != nil {
		old.Close()
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("resolving %v: %w", address, err)
	}
	port_number, err := net.LookupPort("udp", port)
	if err != nil {
		return nil, fmt.Errorf("resolving %v: %w", address, err)
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("resolving %v: %w", address, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolving %v: no addresses", address)
	}
	var ipv4s []net.IP
	for _, ip := range ips {
		if ip.To4() != nil {
			ipv4s = append(ipv4s, ip)
		}
	}
	if len(ipv4s) == 0 {
		ipv4s = ips
	}
	target := &net.UDPAddr{IP: ipv4s[rand.IntN(len(ipv4s))], Port: port_number}
	conn, err := net.DialUDP("udp", nil, target)
	if err != nil {
		return nil, fmt.Errorf("connecting to %v: %w", address, err)
	}
	return conn, nil
}

// readChunks collects file datagrams from the server until it has been quiet for readTimeout.
//
// Inputs: conn is the connected UDP socket; chunks is filled in place, keyed by chunk ID
// (starting from 1). Chunks already in the map are overwritten if received again.
// Returns: the total number of chunks the server announced in the last valid datagram, or 0
// if no valid datagram arrived.
// Errors: an "ERR <message>" reply is returned as a *ServerError carrying that message, and
// so is any read error that is not a timeout. A timeout is the normal end of a round and
// returns a nil error. Malformed datagrams are logged and skipped.
// Side effects: sets a read deadline on conn before every read, mutates chunks, and logs
// skipped datagrams to stderr.
func readChunks(conn *net.UDPConn, chunks map[int]transferdata.TCP_Pack) (int, error) {
	num_chunks := 0
	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		buf := [1024]byte{}
		n_bytes, _, err := conn.ReadFromUDP(buf[:])
		if err != nil {
			if isTimeout(err) {
				return num_chunks, nil
			}
			return num_chunks, err
		}
		message := buf[:n_bytes]

		if bytes.HasPrefix(message, []byte("ERR ")) {
			return num_chunks, &ServerError{Message: string(message[len("ERR "):])}
		}

		id, total, pack, err := parseDatagram(message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Ignoring bad datagram: %v\n", err)
			continue
		}
		chunks[id] = pack
		num_chunks = total
	}
}

// parseDatagram decodes one file datagram: "<ID> <NUM_CHUNKS> <SIZE_OF_CHUNK> <64-bytes-of-data>".
// The data bytes may themselves contain spaces, so only the first three spaces separate fields.
//
// Inputs: message is the raw bytes of one received datagram.
// Returns: the chunk ID, the total number of chunks in the file, and a TCP_Pack holding the
// chunk's size and its 64 data bytes.
// Errors: returns an error, and zero values, when the message does not have four fields,
// the data is not exactly 64 bytes, a number does not parse, or the values are out of range
// (ID outside 1..NUM_CHUNKS, NUM_CHUNKS above maxChunks, or SIZE outside 0..64).
// Side effects: none.
func parseDatagram(message []byte) (id int, num_chunks int, pack transferdata.TCP_Pack, err error) {
	parts := bytes.SplitN(message, []byte(" "), 4)
	if len(parts) != 4 || len(parts[3]) != transferdata.CHUNK_SIZE {
		return 0, 0, pack, errors.New("not a file datagram")
	}

	id, err = strconv.Atoi(string(parts[0]))
	if err != nil {
		return 0, 0, pack, err
	}
	num_chunks, err = strconv.Atoi(string(parts[1]))
	if err != nil {
		return 0, 0, pack, err
	}
	size, err := strconv.Atoi(string(parts[2]))
	if err != nil {
		return 0, 0, pack, err
	}
	if id < 1 || id > num_chunks || num_chunks > maxChunks || size < 0 || size > transferdata.CHUNK_SIZE {
		return 0, 0, pack, fmt.Errorf("out of range values %v %v %v", id, num_chunks, size)
	}

	pack.Size = int64(size)
	copy(pack.Data[:], parts[3])
	return id, num_chunks, pack, nil
}

// missingChunkIds lists the chunks that have not been received yet.
//
// Inputs: chunks holds the received chunks keyed by ID; num_chunks is the total number of
// chunks in the file.
// Returns: the IDs in 1..num_chunks that are not in chunks, in increasing order. The slice is
// empty when the file is complete.
// Errors: none.
// Side effects: none.
func missingChunkIds(chunks map[int]transferdata.TCP_Pack, num_chunks int) []int {
	missing := make([]int, 0)
	for id := 1; id <= num_chunks; id++ {
		_, found := chunks[id]
		if !found {
			missing = append(missing, id)
		}
	}
	return missing
}

// isTimeout reports whether err came from a network operation hitting its deadline.
//
// Inputs: err is an error returned by a read on a connection; it may be nil.
// Returns: true only when err wraps a net.Error whose Timeout method is true.
// Errors: none.
// Side effects: none.
func isTimeout(err error) bool {
	var net_err net.Error
	return errors.As(err, &net_err) && net_err.Timeout()
}

// storeDataIntoFile writes the downloaded bytes to disk.
//
// Inputs: filename is the path to create; file_data is the complete file contents.
// Returns: nil on success.
// Errors: the error from writing the file is returned. The error from os.Create is not
// checked directly; when it fails, Write on the resulting nil file reports os.ErrInvalid.
// Side effects: creates the file, or truncates and overwrites it if it already exists.
func storeDataIntoFile(filename string, file_data []byte) error {
	file, _ := os.Create(filename)
	_, err := file.Write(file_data)
	file.Close()
	return err
}
