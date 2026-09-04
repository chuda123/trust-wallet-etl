package config_test

import (
	"testing"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/config"
)

func TestLoadRejectsNonPositiveInterval(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "0s")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for POLL_INTERVAL=0s")
	}
	t.Setenv("POLL_INTERVAL", "30s")
	t.Setenv("API_TIMEOUT", "0s")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for API_TIMEOUT=0s")
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("API_TIMEOUT", "")
	t.Setenv("FETCH_RESULTS", "")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PollInterval != 30*time.Second {
		t.Fatalf("interval=%s", cfg.PollInterval)
	}
}
