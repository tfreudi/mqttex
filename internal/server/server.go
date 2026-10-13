// Package server exposes the store and broker to the browser UI.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/silasmartin/mqttex/internal/broker"
	"github.com/silasmartin/mqttex/internal/profiles"
	"github.com/silasmartin/mqttex/internal/store"
)

type Server struct {
	store    *store.Store
	broker   *broker.Manager
	profiles *profiles.Book
	loopback bool // only answer requests addressed to a loopback host name
	mux      *http.ServeMux
}

// New wires the API. With loopbackOnly the Host header must name this machine,
// which blocks DNS rebinding from a web page against the local port.
func New(st *store.Store, br *broker.Manager, book *profiles.Book, ui fs.FS, loopbackOnly bool) *Server {
	s := &Server{store: st, broker: br, profiles: book, loopback: loopbackOnly, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/profiles", s.listProfiles)
	s.mux.HandleFunc("POST /api/profiles", s.saveProfile)
	s.mux.HandleFunc("DELETE /api/profiles/{id}", s.deleteProfile)
	s.mux.HandleFunc("POST /api/connect", s.connect)
	s.mux.HandleFunc("POST /api/disconnect", s.disconnect)
	s.mux.HandleFunc("POST /api/publish", s.publish)
	s.mux.HandleFunc("POST /api/focus", s.focus)
	s.mux.HandleFunc("POST /api/clear", s.clear)
	s.mux.HandleFunc("GET /ws", s.serveWS)
	s.mux.Handle("GET /", http.FileServerFS(ui))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := s.guard(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.mux.ServeHTTP(w, r)
}

// guard keeps other web pages in the same browser from driving this server:
// the API can publish to a production broker, so cross-site requests are refused.
func (s *Server) guard(r *http.Request) error {
	if s.loopback && !isLoopbackHost(r.Host) {
		return fmt.Errorf("host %q is not a loopback name", r.Host)
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u.Host != r.Host {
			return fmt.Errorf("cross-origin request from %q refused", origin)
		}
	}
	if r.Method == http.MethodPost && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		return errors.New("POST requests must use Content-Type application/json")
	}
	return nil
}

func isLoopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func (s *Server) listProfiles(w http.ResponseWriter, _ *http.Request) {
	list := s.profiles.List()
	out := make([]profiles.Public, 0, len(list))
	for _, p := range list {
		out = append(out, p.Public())
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) saveProfile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		profiles.Profile
		ClearPassword bool `json:"clearPassword"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	p, err := s.profiles.Save(req.Profile, req.ClearPassword)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, p.Public())
}

func (s *Server) deleteProfile(w http.ResponseWriter, r *http.Request) {
	if err := s.profiles.Delete(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) connect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	p, ok := s.profiles.Get(req.ID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("profile %q does not exist", req.ID))
		return
	}
	if err := s.broker.Connect(p); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.broker.Status())
}

func (s *Server) disconnect(w http.ResponseWriter, _ *http.Request) {
	s.broker.Disconnect()
	writeJSON(w, http.StatusOK, s.broker.Status())
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var req broker.PublishRequest
	if !readJSON(w, r, &req) {
		return
	}
	if err := s.broker.Publish(req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

const (
	maxFocusFilters   = 32
	maxFocusFilterLen = 1024
)

func (s *Server) focus(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filters []string `json:"filters"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if len(req.Filters) > maxFocusFilters {
		writeError(w, http.StatusBadRequest, fmt.Errorf("at most %d filters can be focused, got %d", maxFocusFilters, len(req.Filters)))
		return
	}
	for _, f := range req.Filters {
		if utf8.RuneCountInString(f) > maxFocusFilterLen {
			writeError(w, http.StatusBadRequest, fmt.Errorf("topic filter is longer than %d characters", maxFocusFilterLen))
			return
		}
		if err := profiles.ValidateFilter(f); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	if err := s.broker.Focus(req.Filters); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, broker.ErrNotConnected) {
			code = http.StatusConflict
		}
		writeError(w, code, err)
		return
	}
	writeJSON(w, http.StatusOK, s.broker.Status())
}

func (s *Server) clear(w http.ResponseWriter, _ *http.Request) {
	s.store.Reset()
	writeJSON(w, http.StatusOK, struct{}{})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("request body is not valid JSON: %w", err))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
