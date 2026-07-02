package transport

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// Tuning for TCP. Variables, not constants, so tests can shorten them.
var (
	// peerQueueSize is how many messages may wait for one peer. When the
	// queue is full, Send drops the message: a peer that cannot keep up
	// must not stall the Raft loop, and Raft recovers from lost messages.
	peerQueueSize = 256
	dialTimeout   = 2 * time.Second
	writeTimeout  = 2 * time.Second
	maxBackoff    = time.Second
)

// hello is the first value on every connection: the sender's node ID.
type hello struct {
	From uint64
}

// TCP carries Raft messages between nodes over TCP. Each node keeps one
// outgoing connection to every peer, redialing when it breaks, and accepts
// incoming connections from peers. Messages are gob-encoded.
//
// Peers are trusted: there is no authentication or encryption.
type TCP struct {
	id     uint64
	ln     net.Listener
	peers  map[uint64]chan raft.Message
	recv   chan raft.Message
	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]bool // accepted connections, closed by Close
}

// NewTCP starts a transport for node id, accepting connections on ln and
// sending to the peers in addrs (node ID to host:port; an entry for id
// itself is ignored).
func NewTCP(id uint64, ln net.Listener, addrs map[uint64]string, log *slog.Logger) *TCP {
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCP{
		id:     id,
		ln:     ln,
		peers:  map[uint64]chan raft.Message{},
		recv:   make(chan raft.Message, peerQueueSize),
		log:    log,
		ctx:    ctx,
		cancel: cancel,
		conns:  map[net.Conn]bool{},
	}
	for p, addr := range addrs {
		if p == id {
			continue
		}
		q := make(chan raft.Message, peerQueueSize)
		t.peers[p] = q
		t.wg.Add(1)
		go t.sendLoop(p, addr, q)
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t
}

// Send queues m for delivery to m.To. It never blocks: if the peer's queue
// is full, or m.To is unknown, the message is dropped.
func (t *TCP) Send(m raft.Message) {
	q, ok := t.peers[m.To]
	if !ok {
		return
	}
	select {
	case q <- m:
	default:
	}
}

// Recv returns the channel on which messages from peers arrive.
func (t *TCP) Recv() <-chan raft.Message { return t.recv }

// Close stops the transport and waits for its goroutines to exit.
func (t *TCP) Close() error {
	t.cancel()
	err := t.ln.Close()
	t.mu.Lock()
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
	return err
}

// sendLoop keeps a connection to one peer open and writes its queued
// messages to it.
func (t *TCP) sendLoop(peer uint64, addr string, q chan raft.Message) {
	defer t.wg.Done()
	backoff := 50 * time.Millisecond
	for t.ctx.Err() == nil {
		d := net.Dialer{Timeout: dialTimeout}
		conn, err := d.DialContext(t.ctx, "tcp", addr)
		if err == nil {
			backoff = 50 * time.Millisecond
			err = t.stream(conn, q)
			conn.Close()
			if t.ctx.Err() != nil {
				return
			}
			t.log.Debug("connection to peer lost", "peer", peer, "addr", addr, "err", err)
		}
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (t *TCP) stream(conn net.Conn, q chan raft.Message) error {
	w := bufio.NewWriter(conn)
	enc := gob.NewEncoder(w)
	if err := enc.Encode(hello{From: t.id}); err != nil {
		return err
	}
	for {
		select {
		case <-t.ctx.Done():
			return nil
		case m := <-q:
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := enc.Encode(&m); err != nil {
				return err
			}
			// Batch writes: flush only once the queue is empty.
			if len(q) == 0 {
				if err := w.Flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (t *TCP) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			if t.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			t.log.Warn("accept failed", "err", err)
			continue
		}
		t.mu.Lock()
		t.conns[conn] = true
		t.mu.Unlock()
		t.wg.Add(1)
		go t.receive(conn)
	}
}

// receive reads messages from one peer's connection until it breaks or
// carries something unexpected.
func (t *TCP) receive(conn net.Conn) {
	defer t.wg.Done()
	defer func() {
		t.mu.Lock()
		delete(t.conns, conn)
		t.mu.Unlock()
		conn.Close()
	}()
	dec := gob.NewDecoder(bufio.NewReader(conn))
	var h hello
	if err := dec.Decode(&h); err != nil {
		return
	}
	if _, ok := t.peers[h.From]; !ok {
		t.log.Warn("connection from unknown node", "node", h.From, "remote", conn.RemoteAddr())
		return
	}
	for {
		var m raft.Message
		if err := dec.Decode(&m); err != nil {
			return
		}
		if m.From != h.From || m.To != t.id {
			t.log.Warn("misaddressed message; closing connection", "peer", h.From, "from", m.From, "to", m.To)
			return
		}
		select {
		case t.recv <- m:
		case <-t.ctx.Done():
			return
		}
	}
}
