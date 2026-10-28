// Package client is a Go client for the kvstore HTTP API.
//
// It finds the leader by following redirects, retries until it learns each
// request's outcome, and numbers its writes so that retries are applied at
// most once (see kv.Command).
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Header names, matching internal/server.
const (
	headerClientID = "Kv-Client-Id"
	headerSeq      = "Kv-Seq"
)

// Client talks to a cluster. It sends one request at a time: concurrent
// calls wait their turn. That is what duplicate detection assumes (the
// cluster remembers only each client's latest write), so use one Client per
// concurrent caller.
type Client struct {
	servers []string // base URLs, e.g. http://10.0.0.1:8001
	http    *http.Client
	id      uint64

	// AttemptTimeout bounds one HTTP attempt. MinBackoff and MaxBackoff
	// bound the wait between attempts that found no leader.
	AttemptTimeout         time.Duration
	MinBackoff, MaxBackoff time.Duration
	// Spread, if set, sends each request first to a random server rather
	// than the last known leader, as a client behind a load balancer
	// would. Followers redirect it, so it costs a round trip most of the
	// time. Tests use it to reach deposed leaders and followers with stale
	// views, which a client that sticks to the leader rarely does.
	Spread bool

	mu     sync.Mutex
	seq    uint64
	leader string // base URL of the last known leader, or ""
	next   int    // index into servers of the next node to try
}

// New returns a client for the cluster whose API addresses are servers,
// given as base URLs (http://host:port). Its client ID is random: 64 bits
// make a collision between clients vanishingly unlikely without a
// registration step.
func New(servers []string) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("client: no servers")
	}
	var b [8]byte
	var id uint64
	for id == 0 {
		rand.Read(b[:])
		id = binary.BigEndian.Uint64(b[:])
	}
	return &Client{
		servers: servers,
		id:      id,
		http: &http.Client{
			// Redirects are followed here, not by net/http, so the client
			// can remember where the leader is.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		AttemptTimeout: 5 * time.Second,
		MinBackoff:     20 * time.Millisecond,
		MaxBackoff:     time.Second,
	}, nil
}

// ID returns the client's ID.
func (c *Client) ID() uint64 { return c.id }

// Error is a definite failure reported by the cluster, such as an invalid
// request. Retrying the same request cannot succeed.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("client: %d: %s", e.Status, e.Message) }

// Get returns the value stored under key and whether it exists.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.do(ctx, http.MethodGet, key, nil)
}

// Put stores value under key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, _, err := c.do(ctx, http.MethodPut, key, value)
	return err
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	_, found, err := c.do(ctx, http.MethodDelete, key, nil)
	return found, err
}

// EscapeKey encodes key as one URL path segment. Dots are escaped too, so
// that keys "." and ".." are not treated as path steps.
func EscapeKey(key string) string {
	return strings.ReplaceAll(url.PathEscape(key), ".", "%2E")
}

// do runs one request to completion, retrying until the cluster reports an
// outcome or ctx ends. If ctx ends first, a write's outcome is unknown.
func (c *Client) do(ctx context.Context, method, key string, body []byte) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var seq uint64
	if method != http.MethodGet {
		// One sequence number per write, kept for every retry.
		c.seq++
		seq = c.seq
	}
	if c.Spread {
		c.leader, c.next = "", mrand.IntN(len(c.servers))
	}
	backoff := c.MinBackoff
	redirects := 0
	for {
		base := c.target()
		value, found, retry, err := c.attempt(ctx, base, method, key, body, seq)
		if !retry {
			return value, found, err
		}
		if ctx.Err() != nil {
			return nil, false, fmt.Errorf("client: %w (last error: %v)", ctx.Err(), err)
		}
		if c.leader != "" && c.leader != base && redirects < 3 {
			// Redirected: go straight to the leader. Nodes with stale views
			// can redirect in a circle, so only a few times in a row.
			redirects++
			continue
		}
		redirects = 0
		// No leader known, or the node we tried failed: try another node
		// after a pause, in case an election is under way.
		c.leader = ""
		c.next = (c.next + 1) % len(c.servers)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
		}
		backoff = min(2*backoff, c.MaxBackoff)
	}
}

func (c *Client) target() string {
	if c.leader != "" {
		return c.leader
	}
	return c.servers[c.next]
}

// attempt sends one HTTP request. retry reports whether the outcome is still
// unknown.
func (c *Client) attempt(ctx context.Context, base, method, key string, body []byte, seq uint64) (value []byte, found, retry bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, base+"/kv/"+EscapeKey(key), bytes.NewReader(body))
	if err != nil {
		return nil, false, false, err
	}
	if seq != 0 {
		req.Header.Set(headerClientID, strconv.FormatUint(c.id, 10))
		req.Header.Set(headerSeq, strconv.FormatUint(seq, 10))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, true, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, true, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return data, true, false, nil
	case http.StatusNoContent:
		return nil, true, false, nil
	case http.StatusNotFound:
		return nil, false, false, nil
	case http.StatusTemporaryRedirect:
		loc, err := resp.Location()
		if err != nil {
			return nil, false, true, fmt.Errorf("client: redirect without a usable Location: %w", err)
		}
		c.leader = loc.Scheme + "://" + loc.Host
		return nil, false, true, fmt.Errorf("client: redirected to %s", c.leader)
	case http.StatusServiceUnavailable:
		return nil, false, true, &Error{resp.StatusCode, strings.TrimSpace(string(data))}
	default:
		return nil, false, false, &Error{resp.StatusCode, strings.TrimSpace(string(data))}
	}
}
