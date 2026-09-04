package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is loaded from the environment so the same binary runs locally and in Docker.
type Config struct {
	HTTPAddr      string
	PollInterval  time.Duration
	FetchResults  int
	APIURL        string
	APITimeout    time.Duration
	DatabaseURL   string
	DataDir       string
	LogPath       string
	SourceName    string
	SchemaVersion string
	ShutdownGrace time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:      env("HTTP_ADDR", ":8080"),
		APIURL:        env("API_URL", "https://randomuser.me/api/"),
		DatabaseURL:   env("DATABASE_URL", "postgres://etl:etl@localhost:5432/etl?sslmode=disable"),
		DataDir:       env("DATA_DIR", "./data"),
		LogPath:       env("LOG_PATH", "./logs/etl.log"),
		SourceName:    env("SOURCE_NAME", "randomuser"),
		SchemaVersion: env("SCHEMA_VERSION", "1.0.0"),
	}

	var err error
	if cfg.PollInterval, err = duration("POLL_INTERVAL", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval <= 0 {
		return Config{}, fmt.Errorf("POLL_INTERVAL must be > 0")
	}
	if cfg.APITimeout, err = duration("API_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.APITimeout <= 0 {
		return Config{}, fmt.Errorf("API_TIMEOUT must be > 0")
	}
	if cfg.ShutdownGrace, err = duration("SHUTDOWN_GRACE", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownGrace <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_GRACE must be > 0")
	}
	if cfg.FetchResults, err = integer("FETCH_RESULTS", 10); err != nil {
		return Config{}, err
	}
	if cfg.FetchResults < 1 || cfg.FetchResults > 50 {
		return Config{}, fmt.Errorf("FETCH_RESULTS must be between 1 and 50")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func duration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func integer(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
