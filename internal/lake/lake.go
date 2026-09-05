package lake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Lake is a local filesystem stand-in for an object-store data lake.
// Files are NDJSON (one compact JSON object per line), partitioned by UTC date,
// and always appended.
type Lake struct {
	root string
	mu   sync.Mutex
}

type DeadLetter struct {
	Error   string          `json:"error"`
	Payload json.RawMessage `json:"payload"`
}

func New(root string) (*Lake, error) {
	l := &Lake{root: root}
	for _, dir := range []string{
		filepath.Join(root, "raw"),
		filepath.Join(root, "processed"),
		filepath.Join(root, "dlq"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// AppendRaw writes each source record to data/raw/dt=YYYY-MM-DD/events.ndjson.
func (l *Lake) AppendRaw(ingestedAt time.Time, records []json.RawMessage) error {
	return l.append("raw", "events.ndjson", ingestedAt, records)
}

// AppendProcessed writes normalized records to data/processed/dt=YYYY-MM-DD/users.ndjson.
func (l *Lake) AppendProcessed(ingestedAt time.Time, records []any) error {
	raw := make([]json.RawMessage, 0, len(records))
	for _, rec := range records {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		raw = append(raw, b)
	}
	return l.append("processed", "users.ndjson", ingestedAt, raw)
}

// AppendDLQ keeps poison payloads out of processed while still making them inspectable.
func (l *Lake) AppendDLQ(ingestedAt time.Time, items []DeadLetter) error {
	raw := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		b, err := json.Marshal(item)
		if err != nil {
			return err
		}
		raw = append(raw, b)
	}
	return l.append("dlq", "errors.ndjson", ingestedAt, raw)
}

func (l *Lake) append(kind, partName string, ingestedAt time.Time, records []json.RawMessage) error {
	if len(records) == 0 {
		return nil
	}
	compacted := make([]json.RawMessage, 0, len(records))
	for _, rec := range records {
		c, err := compactJSON(rec)
		if err != nil {
			return fmt.Errorf("compact json: %w", err)
		}
		compacted = append(compacted, c)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	day := ingestedAt.UTC().Format("2006-01-02")
	partDir := filepath.Join(l.root, kind, "dt="+day)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", partDir, err)
	}
	return appendNDJSON(filepath.Join(partDir, partName), compacted)
}

func compactJSON(raw json.RawMessage) (json.RawMessage, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func appendNDJSON(path string, records []json.RawMessage) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	for _, rec := range records {
		if _, err := f.Write(rec); err != nil {
			return err
		}
		if _, err := f.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return f.Sync()
}
