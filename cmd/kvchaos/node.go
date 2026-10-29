package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/jaydenmistry/kvstore/internal/server"
)

type nodeState int

const (
	down nodeState = iota
	running
	paused
)

// node is one kvserver process, which kvchaos starts, kills, pauses, and
// restarts.
type node struct {
	id   uint64
	bin  string
	args []string
	api  string // base URL of its client API
	log  *os.File

	mu     sync.Mutex
	state  nodeState
	cmd    *exec.Cmd
	exited chan struct{} // closed when the current process has exited
	// killing is set while kvchaos is stopping the process on purpose. An
	// exit without it is a failure: the node crashed, or stopped itself
	// with exit status 3 after a storage error.
	killing bool
	status  error // the exit error of the last process; nil for status 0
}

func (n *node) start(failed func(error)) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != down {
		return nil
	}
	fmt.Fprintf(n.log, "--- kvchaos: starting node %d at %s\n", n.id, time.Now().Format(time.RFC3339Nano))
	cmd := exec.Command(n.bin, n.args...)
	cmd.Stdout, cmd.Stderr = n.log, n.log
	if err := cmd.Start(); err != nil {
		return err
	}
	n.cmd, n.state, n.killing = cmd, running, false
	exited := make(chan struct{})
	n.exited = exited
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		n.state, n.status = down, err
		unexpected := !n.killing
		n.mu.Unlock()
		close(exited)
		if unexpected {
			failed(fmt.Errorf("node %d exited on its own: %v (see %s)", n.id, err, n.log.Name()))
		}
	}()
	return nil
}

// signal sends sig to the process, if there is one, and returns a channel
// closed when it exits.
func (n *node) signal(sig syscall.Signal, killing bool) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == down {
		c := make(chan struct{})
		close(c)
		return c
	}
	fmt.Fprintf(n.log, "--- kvchaos: %v to node %d at %s\n", sig, n.id, time.Now().Format(time.RFC3339Nano))
	if killing {
		n.killing = true
	}
	n.cmd.Process.Signal(sig)
	switch sig {
	case syscall.SIGSTOP:
		n.state = paused
	case syscall.SIGCONT:
		n.state = running
	}
	return n.exited
}

// kill crashes the node, as a power cut would stop the process. Its data
// directory survives with whatever it had written: page cache contents
// are not lost, since the host keeps running.
func (n *node) kill() { <-n.signal(syscall.SIGKILL, true) }

func (n *node) pause()  { n.signal(syscall.SIGSTOP, false) }
func (n *node) resume() { n.signal(syscall.SIGCONT, false) }

// stop shuts the node down cleanly and returns an error unless it exited
// with status 0.
func (n *node) stop() error {
	n.resume() // a paused process cannot handle SIGTERM
	select {
	case <-n.signal(syscall.SIGTERM, true):
	case <-time.After(10 * time.Second):
		n.kill()
		return fmt.Errorf("node %d did not stop within 10s of SIGTERM", n.id)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.status != nil {
		return fmt.Errorf("node %d: clean stop exited with %v", n.id, n.status)
	}
	return nil
}

func (n *node) getState() nodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *node) fetchStatus(ctx context.Context) (server.Status, error) {
	var st server.Status
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.api+"/status", nil)
	if err != nil {
		return st, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, err
	}
	if st.ID != n.id {
		return st, errors.New("wrong node answered")
	}
	return st, nil
}
