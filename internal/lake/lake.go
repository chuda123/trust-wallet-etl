package lake

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Lake is a local filesystem stand-in for an object-store data lake.
// Files are NDJSON (one JSON object per line) and always appended.
type Lake struct {
	root string
	mu   sync.Mutex
}

func New(root string) (*Lake, error) {
	l := &Lake{root: root}
	for _, dir := range []string{
		filepath.Join(root, "raw"),
		filepath.Join(root, "processed"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// AppendRaw writes each source record to:
//   - data/raw/dt=YYYY-MM-DD/events.ndjson  (partitioned lake path)
//   - data/raw_data.json                    (spec example path; NDJSON despite .json)
func (l *Lake) AppendRaw(ingestedAt time.Time, records []json.RawMessage) error {
	return l.append("raw", "events.ndjson", "raw_data.json", ingestedAt, records)
}

// AppendProcessed writes normalized records to:
//   - data/processed/dt=YYYY-MM-DD/users.ndjson
//   - data/processed_data.json
func (l *Lake) AppendProcessed(ingestedAt time.Time, records []any) error {
	raw := make([]json.RawMessage, 0, len(records))
	for _, rec := range records {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		raw = append(raw, b)
	}
	return l.append("processed", "users.ndjson", "processed_data.json", ingestedAt, raw)
}

func (l *Lake) append(kind, partName, flatName string, ingestedAt time.Time, records []json.RawMessage) error {
	if len(records) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	day := ingestedAt.UTC().Format("2006-01-02")
	partDir := filepath.Join(l.root, kind, "dt="+day)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", partDir, err)
	}

	partPath := filepath.Join(partDir, partName)
	flatPath := filepath.Join(l.root, flatName)

	if err := appendNDJSON(partPath, records); err != nil {
		return err
	}
	if err := appendNDJSON(flatPath, records); err != nil {
		return err
	}
	return nil
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
