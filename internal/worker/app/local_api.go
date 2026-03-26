package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

const DefaultLocalAPIListenAddress = "127.0.0.1:27841"

const localAPIShutdownTimeout = 2 * time.Second

type LocalAPIServerConfig struct {
	ListenAddress string
}

type LocalAPIServer struct {
	logger *slog.Logger
	state  *LocalState
	config LocalAPIServerConfig
}

func NewLocalAPIServer(logger *slog.Logger, state *LocalState, config LocalAPIServerConfig) *LocalAPIServer {
	if logger == nil {
		logger = slog.Default()
	}
	if state == nil {
		state = NewLocalState(LocalStateConfig{})
	}
	if strings.TrimSpace(config.ListenAddress) == "" {
		config.ListenAddress = DefaultLocalAPIListenAddress
	}
	state.SetAPIAddress(config.ListenAddress)
	return &LocalAPIServer{
		logger: logger,
		state:  state,
		config: config,
	}
}

func (s *LocalAPIServer) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}

	listener, err := net.Listen("tcp", s.config.ListenAddress)
	if err != nil {
		return err
	}

	server := &http.Server{
		Handler:           newLocalAPIHandler(s.state),
		ReadHeaderTimeout: 2 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), localAPIShutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.WarnContext(context.Background(), "local worker api exited", slog.String("error", err.Error()))
		}
	}()

	s.logger.InfoContext(context.Background(), "local worker api listening", slog.String("address", listener.Addr().String()))
	return nil
}

func newLocalAPIHandler(state *LocalState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	infoHandler := func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		snapshot := localSnapshot{}
		if state != nil {
			snapshot = state.Snapshot()
		}
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(snapshot)
	}
	mux.HandleFunc("/info", infoHandler)
	mux.HandleFunc("/v1/info", infoHandler)
	return mux
}
