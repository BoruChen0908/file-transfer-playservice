package udp

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"example.com/go-file-transfer/transfer-server/loss"
	"example.com/go-file-transfer/transfer-server/protocol"
)

// UDPTransport implements protocol.Transport over a single UDP socket,
// one message per datagram. The Peer is the *net.UDPAddr of the sender.
// The loss percentages (0-100) simulate an unreliable network: each datagram sent or
// received is dropped with that probability. Zero means the direction never loses.
type UDPTransport struct {
	conn            *net.UDPConn
	sendLossPercent int
	recvLossPercent int
	stopped         atomic.Bool
}

// StopReceiving makes Receive return net.ErrClosed, including a Receive that is already
// blocked, while Send keeps working so that running transfers can finish. The caller closes
// the socket afterwards.
//
// Side effects: sets a read deadline in the past on the socket.
func (transport *UDPTransport) StopReceiving() {
	transport.stopped.Store(true)
	transport.conn.SetReadDeadline(time.Now())
}

// Receive waits for the next datagram that the simulated network lets through.
//
// Inputs: none; reads from the transport's socket.
// Returns: the trimmed datagram text and the address of its sender.
// Errors: net.ErrClosed once StopReceiving has been called. A read error on the socket is
// returned immediately. Datagrams dropped by the
// simulated loss are not errors; they are skipped and the next datagram is read.
// Side effects: blocks until a datagram survives, and logs each dropped datagram to stderr.
func (transport *UDPTransport) Receive() (string, protocol.Peer, error) {
	buf := make([]byte, 1024)
	for {
		n, from, err := transport.conn.ReadFromUDP(buf)
		if transport.stopped.Load() {
			return "", nil, net.ErrClosed
		}
		if err != nil {
			return "", nil, err
		}
		if loss.ShouldDrop(transport.recvLossPercent) {
			protocol.SimulatedRecvDrops.Add(1)
			fmt.Fprintf(os.Stderr, "DEBUG: Simulated loss: dropped incoming datagram (%v bytes)\n", n)
			continue
		}
		return strings.TrimSpace(string(buf[:n])), from, nil
	}
}

// Send writes one message as one datagram, unless the simulated network loses it.
//
// Inputs: to is the destination address; message is the datagram payload.
// Returns: nil on success, and also nil when the datagram is dropped on purpose, since a
// real sender cannot tell that a datagram was lost.
// Errors: a write error on the socket is returned.
// Side effects: sends a datagram to to, or logs the drop to stderr instead.
func (transport *UDPTransport) Send(to protocol.Peer, message []byte) error {
	if loss.ShouldDrop(transport.sendLossPercent) {
		protocol.SimulatedSendDrops.Add(1)
		fmt.Fprintf(os.Stderr, "DEBUG: Simulated loss: dropped outgoing datagram (%v bytes)\n", len(message))
		return nil
	}
	_, err := transport.conn.WriteTo(message, to)
	return err
}
