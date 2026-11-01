package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaydenmistry/kvstore/internal/client"
	"github.com/jaydenmistry/kvstore/internal/linearize"
)

// recorder collects the history of every client's operations. Times are
// monotonic nanoseconds since the run began, from one clock shared by
// every client, so real-time order between operations is preserved.
type recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []linearize.Op
}

func (r *recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

func (r *recorder) add(o linearize.Op) {
	r.mu.Lock()
	r.ops = append(r.ops, o)
	r.mu.Unlock()
}

// runClient issues random operations one at a time until stop is closed.
// Each operation runs until it has an outcome: the client retries with the
// same sequence number across leader changes and node failures, so a
// write is not abandoned with its outcome unknown. The exception is a write
// refused because the client's session was evicted, which is recorded as
// pending: it may or may not have been applied. If ctx ends first, the
// operation is recorded as pending too, and the run fails for having left
// it unfinished.
func runClient(ctx context.Context, stop <-chan struct{}, rec *recorder, servers []string, keys []string, seed uint64, n int, expired *atomic.Int64) error {
	newClient := func() (*client.Client, error) {
		c, err := client.New(servers)
		if err != nil {
			return nil, err
		}
		// Short attempts, so a client stuck on a paused node moves on
		// quickly.
		c.AttemptTimeout = time.Second
		// Half the clients send each request to a random node. Clients
		// that stick to the leader leave a deposed leader as soon as a
		// write stalls there, and rarely come back while it is stale.
		c.Spread = n%2 == 1
		return c, nil
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(seed, uint64(n)))
	for i := 0; ; i++ {
		select {
		case <-stop:
			return nil
		default:
		}
		// Now and then the client is replaced by a new one, as clients come
		// and go, and registers a new session. With fewer sessions than
		// clients, the registrations keep evicting sessions that are
		// still in use, sometimes while a write in them is being retried.
		if rng.IntN(100) == 0 {
			if c, err = newClient(); err != nil {
				return err
			}
		}
		key := keys[rng.IntN(len(keys))]
		o := linearize.Op{Client: uint64(n), Key: key, Call: rec.now()}
		switch p := rng.IntN(100); {
		case p < 50:
			o.Kind = linearize.Get
			var v []byte
			v, o.Found, err = c.Get(ctx, key)
			o.Value = string(v)
		case p < 85:
			o.Kind, o.Value = linearize.Put, fmt.Sprintf("c%d-%d", n, i)
			err = c.Put(ctx, key, []byte(o.Value))
		default:
			o.Kind = linearize.Delete
			o.Found, err = c.Delete(ctx, key)
		}
		if errors.Is(err, client.ErrSessionExpired) {
			o.Return = linearize.Pending
			rec.add(o)
			expired.Add(1)
			continue
		}
		if err != nil {
			var definite *client.Error
			if errors.As(err, &definite) {
				return fmt.Errorf("client %d: %v %q failed: %w", n, o.Kind, key, err)
			}
			o.Return = linearize.Pending
			rec.add(o)
			return fmt.Errorf("client %d: %v %q unfinished when the run ended: %w", n, o.Kind, key, err)
		}
		o.Return = rec.now()
		rec.add(o)
		// A short random pause varies how operations interleave.
		time.Sleep(time.Duration(rng.IntN(3)) * time.Millisecond)
	}
}
