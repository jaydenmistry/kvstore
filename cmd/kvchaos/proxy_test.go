package main

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// echoServer accepts connections and echoes each line back.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					c.Write([]byte(line))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// roundTrip dials addr, sends a line, and reports whether it came back.
func roundTrip(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte("ping\n")); err != nil {
		return false
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	return err == nil && line == "ping\n"
}

func TestLinkForwardsCutsAndRestores(t *testing.T) {
	l, err := newLink(1, 2, echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	if !roundTrip(l.addr()) {
		t.Fatal("no round trip through an intact link")
	}

	// An open connection is closed by the cut.
	c, err := net.Dial("tcp", l.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	c.Write([]byte("a\n"))
	if line, err := r.ReadString('\n'); err != nil || line != "a\n" {
		t.Fatalf("read %q, %v", line, err)
	}
	l.setCut(true)
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write([]byte("b\n"))
	if line, err := r.ReadString('\n'); err == nil {
		t.Fatalf("read %q through a cut link", line)
	}
	if roundTrip(l.addr()) {
		t.Fatal("new connection got through a cut link")
	}

	l.setCut(false)
	if !roundTrip(l.addr()) {
		t.Fatal("no round trip after restoring the link")
	}
}
