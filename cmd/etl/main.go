package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/config"
	"github.com/chuda123/trust-wallet-etl/internal/extractor"
	"github.com/chuda123/trust-wallet-etl/internal/lake"
	"github.com/chuda123/trust-wallet-etl/internal/observe"
	"github.com/chuda123/trust-wallet-etl/internal/pipeline"
	"github.com/chuda123/trust-wallet-etl/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger, logFile, err := observe.NewLogger(cfg.LogPath)
	if err != nil {
		return err
	}
	defer logFile.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("connecting to postgres", "poll_interval", cfg.PollInterval.String())
	db, err := waitForStore(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Info("shutdown before postgres was ready")
			return nil
		}
		return err
	}
	defer db.Close()

	lk, err := lake.New(cfg.DataDir)
	if err != nil {
		return err
	}

	metrics := observe.NewMetrics()
	srv := observe.NewServer(cfg.HTTPAddr, db, metrics, logger)
	srv.Start(func(err error) {
		logger.Error("http listener failed; stopping pipeline", "error", err)
		stop()
	})

	ex := extractor.New(cfg.APIURL, cfg.FetchResults, cfg.APITimeout)
	pipe := pipeline.New(cfg.SourceName, cfg.SchemaVersion, ex, db, lk, metrics, logger)
	pipe.Run(ctx, cfg.PollInterval)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "error", err)
	}
	logger.Info("shutdown complete")
	return nil
}

func waitForStore(ctx context.Context, url string, logger *slog.Logger) (*store.Store, error) {
	var last error
	for i := 0; i < 20; i++ {
		db, err := store.Connect(ctx, url)
		if err == nil {
			return db, nil
		}
		last = err
		logger.Info("postgres not ready, retrying", "attempt", i+1, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, last
}
