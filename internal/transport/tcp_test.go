package transport

import (
	"encoding/gob"
	"io"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// These tests use real sockets on the loopback interface. They wait for
// messages with a generous deadline but never sleep for a fixed time.

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// pair starts transports for nodes 1 and 2, connected to each other.
func pair(t *testing.T) (*TCP, *TCP) {
	t.Helper()
	ln1, ln2 := listen(t), listen(t)
	addrs := map[uint64]string{1: ln1.Addr().String(), 2: ln2.Addr().String()}
	a, b := NewTCP(1, ln1, addrs, quiet), NewTCP(2, ln2, addrs, quiet)
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func receive(t *testing.T, tr *TCP) raft.Message {
	t.Helper()
	select {
	case m := <-tr.Recv():
		return m
	case <-time.After(10 * time.Second):
		t.Fatal("no message within 10s")
		return raft.Message{}
	}
}

func TestTCPDeliversMessages(t *testing.T) {
	a, b := pair(t)
	want := raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 3, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4, Round: 9,
		Entries: []raft.Entry{{Index: 5, Term: 3, Data: []byte("x")}, {Index: 6, Term: 3, Data: []byte("yz")}},
	}
	a.Send(want)
	if got := receive(t, b); !reflect.DeepEqual(got, want) {
		t.Fatalf("received %+v, want %+v", got, want)
	}
	reply := raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 3, Success: true, MatchIndex: 6, Round: 9}
	b.Send(reply)
	if got := receive(t, a); !reflect.DeepEqual(got, reply) {
		t.Fatalf("received %+v, want %+v", got, reply)
	}
}

func TestTCPPreservesOrderOnOneConnection(t *testing.T) {
	a, b := pair(t)
	for i := range uint64(100) {
		a.Send(raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: i + 1})
	}
	for i := range uint64(100) {
		if m := receive(t, b); m.Term != i+1 {
			t.Fatalf("message %d has term %d", i+1, m.Term)
		}
	}
}

func TestTCPReconnectsAfterPeerRestart(t *testing.T) {
	ln1, ln2 := listen(t), listen(t)
	addr2 := ln2.Addr().String()
	addrs := map[uint64]string{1: ln1.Addr().String(), 2: addr2}
	a := NewTCP(1, ln1, addrs, quiet)
	defer a.Close()
	b := NewTCP(2, ln2, addrs, quiet)
	a.Send(raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 1})
	receive(t, b)
	b.Close()

	ln2, err := net.Listen("tcp", addr2)
	if err != nil {
		t.Fatal(err)
	}
	b = NewTCP(2, ln2, addrs, quiet)
	defer b.Close()
	// Messages sent while the old connection was breaking may be lost, as
	// Raft allows. Keep sending until one gets through.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a.Send(raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 2})
		select {
		case m := <-b.Recv():
			if m.Term != 2 {
				t.Fatalf("received %+v", m)
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("no message reached the restarted peer")
}

func TestTCPSendNeverBlocks(t *testing.T) {
	// Node 2's address accepts nothing, so its queue fills. Send must drop
	// rather than wait.
	ln1, ln2 := listen(t), listen(t)
	dead := ln2.Addr().String()
	ln2.Close()
	a := NewTCP(1, ln1, map[uint64]string{1: ln1.Addr().String(), 2: dead}, quiet)
	defer a.Close()
	for range 10 * peerQueueSize {
		a.Send(raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 1})
	}
	a.Send(raft.Message{Type: raft.MsgVote, From: 1, To: 99}) // unknown peer
}

func TestTCPRejectsUnknownNode(t *testing.T) {
	_, b := pair(t)
	conn, err := net.Dial("tcp", b.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := gob.NewEncoder(conn).Encode(hello{From: 7}); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read from connection claiming to be node 7: %v, want EOF (closed by peer)", err)
	}
}

func TestTCPRejectsMisaddressedMessage(t *testing.T) {
	_, b := pair(t)
	conn, err := net.Dial("tcp", b.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	enc := gob.NewEncoder(conn)
	enc.Encode(hello{From: 1})
	enc.Encode(raft.Message{Type: raft.MsgVote, From: 3, To: 2, Term: 1}) // From does not match hello
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read after spoofed message: %v, want EOF (closed by peer)", err)
	}
	select {
	case m := <-b.Recv():
		t.Fatalf("delivered spoofed message %+v", m)
	default:
	}
}

func TestTCPCloseStopsEverything(t *testing.T) {
	a, b := pair(t)
	a.Send(raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 1})
	receive(t, b)
	// Close must return even with live connections in both directions.
	done := make(chan struct{})
	go func() { a.Close(); b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
}
