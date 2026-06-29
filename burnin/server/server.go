// Package server exposes the burn-in control HTTP API: /health /ready /info
// /metrics /run/start /run/stop /run/status /run/config /run/report /run.
// Mirrors the kubemq-aws burnin server, recast for GCP Pub/Sub. The dashboard
// drives it exactly like the SDK burn-in apps (POST /run/start → GET /run/status
// → POST /run/stop).
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/api"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/config"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/metrics"
	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/report"
)

// RunController is the engine surface the server drives.
type RunController interface {
	State() string
	StartRun(cfg *api.RunConfig) error
	StopRun() error
	RunID() string
	RunConfig() *config.Config
	RunReport() (*report.Summary, *report.Verdict)
	RunStatus() api.RunStatus
	RunSnapshot() api.RunSnapshot
}

// Server is the control HTTP server.
type Server struct {
	httpSrv    *http.Server
	controller RunController
	startupCfg *config.Config
	logger     *slog.Logger
}

// New builds the control server.
func New(port int, controller RunController, startupCfg *config.Config, logger *slog.Logger) *Server {
	s := &Server{
		controller: controller,
		startupCfg: startupCfg,
		logger:     logger.With("component", "server"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/info", s.handleInfo)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/run/start", s.handleRunStart)
	mux.HandleFunc("/run/stop", s.handleRunStop)
	mux.HandleFunc("/run/status", s.handleRunStatus)
	mux.HandleFunc("/run/config", s.handleRunConfig)
	mux.HandleFunc("/run/report", s.handleRunReport)
	mux.HandleFunc("/run", s.handleRunSnapshot)

	s.httpSrv = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           s.corsMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// Start launches the HTTP server.
func (s *Server) Start() error {
	s.logger.Info("starting control API server", "addr", s.httpSrv.Addr)
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("API server error", "error", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping control API server")
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origins := s.startupCfg.CORS.Origins
		if origins == "" {
			origins = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origins)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "state": s.controller.State()})
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	emulator := s.startupCfg.GCP.EmulatorHost
	if emulator == "" {
		emulator = s.startupCfg.Broker.Address
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"connector":  "gcp-pub-sub",
		"sdk":        metrics.SDK(),
		"version":    s.startupCfg.Output.SDKVersion,
		"broker":     s.startupCfg.Broker.Address,
		"endpoint":   emulator,
		"project_id": s.startupCfg.GCP.ProjectID,
		"port":       strconv.Itoa(s.startupCfg.Metrics.Port),
	})
}

func (s *Server) handleRunStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	state := s.controller.State()
	if state != "idle" && state != "stopped" && state != "error" {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": fmt.Sprintf("cannot start run in state %q", state), "state": state,
		})
		return
	}
	var rc api.RunConfig
	if r.Body != nil {
		// Tolerate an empty body (start from base config).
		_ = json.NewDecoder(r.Body).Decode(&rc)
	}
	if err := s.controller.StartRun(&rc); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "starting", "run_id": s.controller.RunID()})
}

func (s *Server) handleRunStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if err := s.controller.StopRun(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"state": "stopping"})
}

func (s *Server) handleRunStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.controller.RunStatus())
}

func (s *Server) handleRunConfig(w http.ResponseWriter, _ *http.Request) {
	cfg := s.controller.RunConfig()
	if cfg == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no active or completed run"})
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) handleRunReport(w http.ResponseWriter, _ *http.Request) {
	summary, verdict := s.controller.RunReport()
	if summary == nil || verdict == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no report available (run not stopped)"})
		return
	}
	type fullReport struct {
		*report.Summary
		Verdict *report.Verdict `json:"verdict"`
	}
	writeJSON(w, http.StatusOK, fullReport{Summary: summary, Verdict: verdict})
}

func (s *Server) handleRunSnapshot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.controller.RunSnapshot())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
