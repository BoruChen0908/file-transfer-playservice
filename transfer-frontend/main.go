package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/go-file-transfer/transfer-client/tcp"
	"example.com/go-file-transfer/transfer-client/udp"
	transferdata "example.com/go-file-transfer/transfer-data"
	"example.com/go-file-transfer/transfer-frontend/gateway"
)

/*
 * HTTP frontend for the FTP_6650 file server (UDP or TCP version): GET /files?name=<filename>, and
 * GET /health for a load balancer.
 * optional args (each also has an environment variable, which the option overrides)
 *  -listen=ADDR   address the HTTP server listens on. Defaults to ":8000". Env LISTEN.
 *  -transport=udp|tcp which FTP_6650 server version to fetch files with. Defaults to "udp". Env TRANSPORT.
 *  -upstream=ADDR the FTP_6650 server (UDP or TCP, to match -transport) as host:port, normally
 *                  the backend load balancer. Defaults to "localhost:8080". Env UPSTREAM.
 *  -max-inflight=N number of file transfers run against the UDP server at once (1 or more).
 *                  Defaults to 4. Keep it at or below the UDP server's -workers. Env MAX_INFLIGHT.
 *  -queue-wait=SECONDS how long a request waits for a free transfer slot before getting 503.
 *                  Defaults to 5. Env QUEUE_WAIT.
 *  -drain=SECONDS time between /health turning 503 and the process exiting after SIGTERM, so
 *                  the load balancer stops sending requests first. Defaults to 5.
 */
func main() {
	listen := envOr("LISTEN", ":8000")
	upstream := envOr("UPSTREAM", "localhost:8080")
	transport := envOr("TRANSPORT", "udp")
	maxInFlight := 4
	queueWait := gateway.DefaultQueueWait
	drain := 5 * time.Second

	var err error
	if os.Getenv("MAX_INFLIGHT") != "" {
		maxInFlight, err = parseMaxInFlight("MAX_INFLIGHT=" + os.Getenv("MAX_INFLIGHT"))
		if err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("QUEUE_WAIT") != "" {
		queueWait, err = parseSeconds("QUEUE_WAIT=" + os.Getenv("QUEUE_WAIT"))
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, option := range os.Args[1:] {
		if strings.HasPrefix(option, "-listen=") {
			listen = strings.SplitN(option, "=", 2)[1]
		} else if strings.HasPrefix(option, "-upstream=") {
			upstream = strings.SplitN(option, "=", 2)[1]
		} else if strings.HasPrefix(option, "-transport=") {
			transport = strings.SplitN(option, "=", 2)[1]
		} else if strings.HasPrefix(option, "-max-inflight=") {
			maxInFlight, err = parseMaxInFlight(option)
		} else if strings.HasPrefix(option, "-queue-wait=") {
			queueWait, err = parseSeconds(option)
		} else if strings.HasPrefix(option, "-drain=") {
			drain, err = parseSeconds(option)
		}
		if err != nil {
			log.Fatal(err)
		}
	}

	fetch, err := fetcherFor(transport)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Serving http://%v/files, fetching over %v from %v (up to %v at once)\n", listen, transport, upstream, maxInFlight)
	frontend := gateway.New(upstream, fetch, maxInFlight, queueWait)
	go transferdata.LogUDPCounters(10 * time.Second)

	// On SIGTERM (a scale-in or deploy) report unhealthy first so the load balancer stops
	// sending new requests, wait for it to notice, let running downloads finish, then exit.
	// Gin's Run has no shutdown hook, so exiting the process is what stops the server.
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-signals.Done()
		fmt.Printf("Shutting down: unhealthy now, stopping in %v\n", drain)
		frontend.StartDraining()
		time.Sleep(drain)
		if !frontend.WaitIdle(30 * time.Second) {
			fmt.Println("Downloads still running after 30s, exiting anyway")
		}
		os.Exit(0)
	}()

	err = frontend.Router.Run(listen)
	if err != nil {
		log.Fatal(err)
	}
}

// fetcherFor picks the client that downloads files from the upstream server.
//
// Inputs: transport is "udp" or "tcp".
// Returns: udp.Fetch for "udp" and tcp.Fetch for "tcp".
// Errors: returns an error naming the value when it is neither.
// Side effects: none.
func fetcherFor(transport string) (gateway.Fetcher, error) {
	switch transport {
	case "udp":
		return udp.Fetch, nil
	case "tcp":
		return tcp.Fetch, nil
	}
	return nil, fmt.Errorf("invalid transport %q: expected udp or tcp", transport)
}

// envOr returns the environment variable name, or fallback when it is unset or empty.
func envOr(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

// parseSeconds reads the number of seconds out of an option such as "-queue-wait=5".
//
// Inputs: option is a command line argument of the form "-name=N".
// Returns: N seconds as a duration.
// Errors: returns an error naming the option when N is missing, is not an integer, or is
// negative.
// Side effects: none.
func parseSeconds(option string) (time.Duration, error) {
	value := strings.SplitN(option, "=", 2)[1]
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 0 {
		return 0, fmt.Errorf("invalid %v: expected an integer of 0 or more", option)
	}
	return time.Duration(seconds) * time.Second, nil
}

// parseMaxInFlight reads the number out of a "-max-inflight=N" option.
//
// Inputs: option is a command line argument of the form "-max-inflight=N".
// Returns: N as an integer of 1 or more.
// Errors: returns an error naming the option when N is missing, is not an integer, or is
// less than 1.
// Side effects: none.
func parseMaxInFlight(option string) (int, error) {
	value := strings.SplitN(option, "=", 2)[1]
	maxInFlight, err := strconv.Atoi(value)
	if err != nil || maxInFlight < 1 {
		return 0, fmt.Errorf("invalid %v: expected an integer of 1 or more", option)
	}
	return maxInFlight, nil
}
