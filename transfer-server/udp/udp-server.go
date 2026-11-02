package udp

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	transferdata "example.com/go-file-transfer/transfer-data"
	"example.com/go-file-transfer/transfer-server/health"
	"example.com/go-file-transfer/transfer-server/loss"
	"example.com/go-file-transfer/transfer-server/opts"
	"example.com/go-file-transfer/transfer-server/protocol"
)

/*
 * optional args (each also has an environment variable, which the option overrides)
 *  -send-loss=N percent chance (0-100) that a datagram the server sends is dropped. Defaults to 0. Env SEND_LOSS.
 *  -recv-loss=N percent chance (0-100) that a datagram the server receives is dropped. Defaults to 0. Env RECV_LOSS.
 *  -workers=N number of requests handled at the same time (1 or more). Defaults to 8. Env WORKERS.
 *  -bucket=NAME S3 bucket files are fetched from for each request. Env BUCKET.
 *  -prefix=KEY/PREFIX/ key prefix prepended to every requested file name. Defaults to none. Env PREFIX.
 *  -dir=PATH serve files from a local folder instead of S3 (for development and tests). Env FILES_DIR.
 *  Exactly one of -bucket and -dir is required.
 *  -port=N UDP port the file server listens on. Defaults to 8080. Env PORT.
 *  -health-port=N TCP port of the HTTP health check (GET /health) for a load balancer. Defaults to 8081. Env HEALTH_PORT.
 *  -drain=SECONDS time between the health check turning unhealthy and the server stopping, so the load balancer stops sending traffic first. Defaults to 5.
 */

// RunServer starts the FTP_6650 server on a UDP port and serves until SIGTERM or SIGINT.
//
// Inputs: args may contain the options listed above; other arguments are ignored. Each of
// -send-loss, -recv-loss, -workers, -bucket, -prefix, -dir, -port and -health-port falls back to its environment variable
// when the option is absent.
// Returns: after a SIGTERM or SIGINT, once the health check has reported unhealthy for the
// drain time and the requests already being handled have finished.
// Errors: ends the program through log.Fatal when a loss option is not an integer from
// 0 to 100, when -workers is not an integer of 1 or more, when a port or -drain is not a
// valid number, when not exactly one of -bucket and -dir is given or S3 cannot be configured,
// or when the server cannot listen on its ports.
// Side effects: opens a UDP socket and a TCP health port, prints the ports, worker count (and
// any simulated loss rates) to stdout, and handles requests through protocol.Serve.
func RunServer(args []string) {

	port := opts.EnvOr("PORT", "8080")
	healthPort := opts.EnvOr("HEALTH_PORT", "8081")
	sendLossPercent := 0
	recvLossPercent := 0
	workers := 8
	bucket := os.Getenv("BUCKET")
	prefix := os.Getenv("PREFIX")
	dir := os.Getenv("FILES_DIR")
	drain := 5 * time.Second
	var err error
	if os.Getenv("WORKERS") != "" {
		workers, err = parseWorkers("WORKERS=" + os.Getenv("WORKERS"))
		if err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("SEND_LOSS") != "" {
		sendLossPercent, err = loss.ParsePercent("SEND_LOSS=" + os.Getenv("SEND_LOSS"))
		if err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("RECV_LOSS") != "" {
		recvLossPercent, err = loss.ParsePercent("RECV_LOSS=" + os.Getenv("RECV_LOSS"))
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, option := range args {
		if strings.HasPrefix(option, "-send-loss=") {
			sendLossPercent, err = loss.ParsePercent(option)
		} else if strings.HasPrefix(option, "-recv-loss=") {
			recvLossPercent, err = loss.ParsePercent(option)
		} else if strings.HasPrefix(option, "-workers=") {
			workers, err = parseWorkers(option)
		} else if strings.HasPrefix(option, "-bucket=") {
			bucket = strings.TrimPrefix(option, "-bucket=")
		} else if strings.HasPrefix(option, "-prefix=") {
			prefix = strings.TrimPrefix(option, "-prefix=")
		} else if strings.HasPrefix(option, "-dir=") {
			dir = strings.TrimPrefix(option, "-dir=")
		} else if strings.HasPrefix(option, "-port=") {
			port = strings.TrimPrefix(option, "-port=")
		} else if strings.HasPrefix(option, "-health-port=") {
			healthPort = strings.TrimPrefix(option, "-health-port=")
		} else if strings.HasPrefix(option, "-drain=") {
			drain, err = opts.ParseSeconds(option)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, value := range []string{port, healthPort} {
		err := opts.ValidPort(value)
		if err != nil {
			log.Fatal(err)
		}
	}

	source, err := opts.OpenSource(dir, bucket, prefix)
	if err != nil {
		log.Fatal(err)
	}

	address, err := net.ResolveUDPAddr("udp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatal("Failed to listen")
	}
	defer ln.Close()

	var draining atomic.Bool
	go health.Serve(":"+healthPort, &draining)
	go transferdata.LogUDPCounters(10 * time.Second)

	fmt.Printf("Listening on UDP port %v (health check on TCP port %v) with %v workers\n", port, healthPort, workers)
	if sendLossPercent > 0 || recvLossPercent > 0 {
		fmt.Printf("Simulating loss: send %v%%, receive %v%%\n", sendLossPercent, recvLossPercent)
	}

	transport := &UDPTransport{
		conn:            ln,
		sendLossPercent: sendLossPercent,
		recvLossPercent: recvLossPercent,
	}

	// On SIGTERM (a scale-in or deploy) report unhealthy first so the load balancer stops
	// sending new requests, wait for it to notice, and only then stop reading. Serve returns
	// once the requests already accepted are done.
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-signals.Done()
		fmt.Printf("Shutting down: unhealthy now, stopping in %v\n", drain)
		draining.Store(true)
		time.Sleep(drain)
		transport.StopReceiving()
	}()

	protocol.Serve(transport, source, workers)
}

// parseWorkers reads the number out of a "-workers=N" option.
//
// Inputs: option is a command line argument of the form "-workers=N".
// Returns: N as an integer of 1 or more.
// Errors: returns an error naming the option when N is missing, is not an integer, or is
// less than 1.
// Side effects: none.
func parseWorkers(option string) (int, error) {
	value := strings.SplitN(option, "=", 2)[1]
	workers, err := strconv.Atoi(value)
	if err != nil || workers < 1 {
		return 0, fmt.Errorf("invalid %v: expected an integer of 1 or more", option)
	}
	return workers, nil
}
