package observe

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Server struct {
	http    *http.Server
	store   Pinger
	metrics *Metrics
	log     *slog.Logger
}

func NewServer(addr string, store Pinger, metrics *Metrics, log *slog.Logger) *Server {
	s := &Server{store: store, metrics: metrics, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) Start() {
	go func() {
		s.log.Info("http server listening", "addr", s.http.Addr)
		if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server failed", "error", err)
		}
	}()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status := "ok"
	code := http.StatusOK
	pg := "ok"
	if err := s.store.Ping(ctx); err != nil {
		status = "degraded"
		code = http.StatusServiceUnavailable
		pg = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":   status,
		"postgres": pg,
	})
}
