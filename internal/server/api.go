package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/jaydenmistry/kvstore/internal/kv"
	"github.com/jaydenmistry/kvstore/internal/raft"
)

// The client API:
//
//	GET    /kv/{key}  200 with the value, or 404
//	PUT    /kv/{key}  body is the value; 204
//	DELETE /kv/{key}  204 if the key existed, 404 if not
//	POST   /session   200 with a new client ID, in decimal
//	GET    /status    this node's Raft state, as JSON
//
// The key is one path segment, percent-escaped (including "/" and ".").
// Writes must carry the headers Kv-Client-Id and Kv-Seq: a client ID from
// POST /session and that client's sequence number for this write, starting
// at 1 and reused on every retry. A write answered 410 Gone was refused
// because the client's session was evicted; its outcome is unknown, and the
// client must register again.
//
// A node that is not the leader answers 307 Temporary Redirect to the
// leader's API, or 503 if it knows no leader. 503 also means the request's
// outcome is unknown (it timed out, or leadership changed): retry it, with
// the same sequence number for a write.
const (
	HeaderClientID = "Kv-Client-Id"
	HeaderSeq      = "Kv-Seq"
	// HeaderLeader names the leader's node ID on redirects.
	HeaderLeader = "Kv-Leader"
)

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key...}", s.handleKV)
	mux.HandleFunc("PUT /kv/{key...}", s.handleKV)
	mux.HandleFunc("DELETE /kv/{key...}", s.handleKV)
	mux.HandleFunc("POST /session", s.handleRegister)
	mux.HandleFunc("GET /status", s.handleStatus)
	return mux
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	res, err := s.submit(r.Context(), kv.Request{Op: kv.OpRegister})
	switch {
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(res.Err, kv.ErrNotLeader):
		s.redirect(w, r, res.Leader)
	case res.Err != nil: // ErrTimeout, ErrLeadershipLost
		w.Header().Set("Retry-After", "1")
		http.Error(w, res.Err.Error(), http.StatusServiceUnavailable)
	default:
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, res.ClientID)
	}
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	req := kv.Request{Key: r.PathValue("key")}
	switch r.Method {
	case http.MethodGet:
		req.Op = kv.OpGet
	case http.MethodPut:
		req.Op = kv.OpPut
	case http.MethodDelete:
		req.Op = kv.OpDelete
	}
	if req.Op != kv.OpGet {
		var err error
		if req.ClientID, req.Seq, err = clientHeaders(r); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if req.Op == kv.OpPut {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, kv.MaxValueSize))
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, fmt.Sprintf("value is over the %d byte limit", kv.MaxValueSize), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
			return
		}
		req.Value = body
	}

	res, err := s.submit(r.Context(), req)
	switch {
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(res.Err, kv.ErrNotLeader):
		s.redirect(w, r, res.Leader)
	case errors.Is(res.Err, kv.ErrInvalid):
		http.Error(w, res.Err.Error(), http.StatusBadRequest)
	case errors.Is(res.Err, kv.ErrStaleSequence):
		http.Error(w, res.Err.Error(), http.StatusConflict)
	case errors.Is(res.Err, kv.ErrSessionExpired):
		http.Error(w, res.Err.Error(), http.StatusGone)
	case res.Err != nil: // ErrTimeout, ErrLeadershipLost
		w.Header().Set("Retry-After", "1")
		http.Error(w, res.Err.Error(), http.StatusServiceUnavailable)
	case req.Op == kv.OpGet && res.Found:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(res.Value)
	case req.Op == kv.OpPut || res.Found:
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "key not found", http.StatusNotFound)
	}
}

func clientHeaders(r *http.Request) (client, seq uint64, err error) {
	client, err = strconv.ParseUint(r.Header.Get(HeaderClientID), 10, 64)
	if err != nil || client == 0 {
		return 0, 0, fmt.Errorf("writes need a nonzero %s header", HeaderClientID)
	}
	seq, err = strconv.ParseUint(r.Header.Get(HeaderSeq), 10, 64)
	if err != nil || seq == 0 {
		return 0, 0, fmt.Errorf("writes need a nonzero %s header", HeaderSeq)
	}
	return client, seq, nil
}

// redirect sends the client to the leader. 307 keeps the method and body,
// unlike 302, so a PUT is repeated as a PUT.
func (s *Server) redirect(w http.ResponseWriter, r *http.Request, leader uint64) {
	p, ok := s.cfg.Peers[leader]
	if leader == raft.None || !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader known; retry", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set(HeaderLeader, strconv.FormatUint(leader, 10))
	http.Redirect(w, r, "http://"+p.API+r.URL.RequestURI(), http.StatusTemporaryRedirect)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.status(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}
