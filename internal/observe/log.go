package observe

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// NewLogger writes JSON logs to stdout and logs/etl.log (Go's slog).
func NewLogger(logPath string) (*slog.Logger, *os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	w := io.MultiWriter(os.Stdout, f)
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h), f, nil
}
