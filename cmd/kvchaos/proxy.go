package main

import (
	"io"
	"net"
	"sync"
)

// link forwards one direction of Raft traffic, from node from to node to,
// so it can be cut. Each node dials its own connection to every peer for
// the messages it sends, so cutting from->to loses exactly the messages
// from sends to to, which is a one-way partition. Cutting both directions
// partitions the pair.
type link struct {
	from, to uint64
	ln       net.Listener
	target   string // to's real Raft address

	mu    sync.Mutex
	cut   bool
	conns map[net.Conn]bool
}

func newLink(from, to uint64, target string) (*link, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	l := &link{from: from, to: to, ln: ln, target: target, conns: map[net.Conn]bool{}}
	go l.serve()
	return l, nil
}

// addr is the address from must dial to reach to.
func (l *link) addr() string { return l.ln.Addr().String() }

// setCut cuts or restores the link. Cutting closes the connections in use,
// so messages already in flight are lost too, and refuses new ones until
// the link is restored. The sender sees connection errors and keeps
// redialing, as it would across a real partition once TCP gives up.
func (l *link) setCut(cut bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cut = cut
	if cut {
		for c := range l.conns {
			c.Close()
		}
		clear(l.conns)
	}
}

func (l *link) close() {
	l.ln.Close()
	l.setCut(true)
}

func (l *link) serve() {
	for {
		in, err := l.ln.Accept()
		if err != nil {
			return // closed
		}
		go l.forward(in)
	}
}

func (l *link) forward(in net.Conn) {
	out, err := net.Dial("tcp", l.target)
	if err != nil {
		in.Close() // to is down; from will redial
		return
	}
	l.mu.Lock()
	if l.cut {
		l.mu.Unlock()
		in.Close()
		out.Close()
		return
	}
	l.conns[in], l.conns[out] = true, true
	l.mu.Unlock()

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(out, in)
	go pipe(in, out)
	<-done // either side closing ends both
	in.Close()
	out.Close()
	l.mu.Lock()
	delete(l.conns, in)
	delete(l.conns, out)
	l.mu.Unlock()
}
