package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fake is a scripted server. handle is called for each request; it runs on
// the server's goroutines, so the log is guarded.
type fake struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
}

func newFake(t *testing.T, handle func(w http.ResponseWriter, r *http.Request, n int)) *fake {
	t.Helper()
	f := &fake{}
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs = append(f.reqs, r)
		n := len(f.reqs)
		f.mu.Unlock()
		handle(w, r, n)
	})
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fake) requests() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*http.Request(nil), f.reqs...)
}

func newClient(t *testing.T, servers ...string) *Client {
	t.Helper()
	c, err := New(servers)
	if err != nil {
		t.Fatal(err)
	}
	c.MinBackoff, c.MaxBackoff = time.Millisecond, 5*time.Millisecond
	return c
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRetryKeepsSequenceNumber(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n <= 2 {
			http.Error(w, "timed out", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, f.URL)
	if err := c.Put(ctx(t), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx(t), "k", []byte("w")); err != nil {
		t.Fatal(err)
	}
	var seqs []string
	for _, r := range f.requests() {
		if id := r.Header.Get(headerClientID); id != strconv.FormatUint(c.ID(), 10) {
			t.Fatalf("client ID header %q, want %d", id, c.ID())
		}
		seqs = append(seqs, r.Header.Get(headerSeq))
	}
	// Three attempts at write 1, then write 2.
	if want := []string{"1", "1", "1", "2"}; !slices.Equal(seqs, want) {
		t.Fatalf("sequence numbers %q, want %q", seqs, want)
	}
}

func TestFollowsRedirectAndRemembersLeader(t *testing.T) {
	leader := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		w.Write([]byte("v"))
	})
	follower := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		http.Redirect(w, r, leader.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	})
	c := newClient(t, follower.URL, leader.URL)
	for range 3 {
		v, ok, err := c.Get(ctx(t), "k")
		if err != nil || !ok || string(v) != "v" {
			t.Fatalf("Get = %q, %v, %v", v, ok, err)
		}
	}
	if n := len(follower.requests()); n != 1 {
		t.Fatalf("follower got %d requests, want 1 (later ones go to the leader)", n)
	}
	if n := len(leader.requests()); n != 3 {
		t.Fatalf("leader got %d requests, want 3", n)
	}
}

func TestRedirectKeepsWriteBody(t *testing.T) {
	var got string
	leader := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(http.StatusNoContent)
	})
	follower := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		http.Redirect(w, r, leader.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	})
	c := newClient(t, follower.URL)
	if err := c.Put(ctx(t), "k", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if got != "value" {
		t.Fatalf("leader received body %q", got)
	}
	if r := leader.requests()[0]; r.Method != http.MethodPut || r.Header.Get(headerSeq) != "1" {
		t.Fatalf("redirected request %s seq %q, want PUT seq 1", r.Method, r.Header.Get(headerSeq))
	}
}

func TestTriesNextServerWhenOneIsDown(t *testing.T) {
	down := newFake(t, nil)
	down.Close()
	up := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, down.URL, up.URL)
	if found, err := c.Delete(ctx(t), "k"); err != nil || !found {
		t.Fatalf("Delete = %v, %v", found, err)
	}
}

func TestNotFound(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		http.Error(w, "key not found", http.StatusNotFound)
	})
	c := newClient(t, f.URL)
	if _, ok, err := c.Get(ctx(t), "k"); err != nil || ok {
		t.Fatalf("Get = %v, %v; want not found", ok, err)
	}
	if found, err := c.Delete(ctx(t), "k"); err != nil || found {
		t.Fatalf("Delete = %v, %v; want not found", found, err)
	}
}

func TestDefiniteErrorIsNotRetried(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		http.Error(w, "empty key", http.StatusBadRequest)
	})
	c := newClient(t, f.URL)
	err := c.Put(ctx(t), "k", nil)
	var e *Error
	if !errors.As(err, &e) || e.Status != http.StatusBadRequest || e.Message != "empty key" {
		t.Fatalf("err = %v, want a 400 Error", err)
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("%d attempts, want 1", n)
	}
}

func TestGivesUpWhenContextEnds(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
	})
	c := newClient(t, f.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Put(ctx, "k", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

func TestKeysReachServerIntact(t *testing.T) {
	var got []string
	var mu sync.Mutex
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, n int) {
		mu.Lock()
		got = append(got, r.PathValue("key"))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	keys := []string{"a", "a/b", "a//b", ".", "..", "../x", "a/./b", "%", "%2F", " sp", "?q#f", "日本"}
	c := newClient(t, f.URL)
	for _, k := range keys {
		if err := c.Put(ctx(t), k, nil); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("server saw keys %q, want %q", got, keys)
	}
}
