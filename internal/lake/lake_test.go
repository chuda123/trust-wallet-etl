package lake_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/lake"
)

func TestAppendDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	lk, err := lake.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	a := []json.RawMessage{json.RawMessage(`{"id":1}`)}
	b := []json.RawMessage{json.RawMessage(`{"id":2}`)}
	if err := lk.AppendRaw(ts, a); err != nil {
		t.Fatal(err)
	}
	if err := lk.AppendRaw(ts, b); err != nil {
		t.Fatal(err)
	}

	part := filepath.Join(dir, "raw", "dt=2026-09-04", "events.ndjson")
	flat := filepath.Join(dir, "raw_data.json")
	for _, p := range []string{part, flat} {
		body, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		if len(lines) != 2 {
			t.Fatalf("%s: want 2 lines, got %d (%q)", p, len(lines), body)
		}
	}
}

func TestAppendCompactsEmbeddedNewlines(t *testing.T) {
	dir := t.TempDir()
	lk, err := lake.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	pretty := []json.RawMessage{json.RawMessage("{\n  \"id\": 1\n}")}
	if err := lk.AppendRaw(ts, pretty); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, "raw", "dt=2026-09-04", "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 1 {
		t.Fatalf("pretty json must compact to one NDJSON line, got %d (%q)", len(lines), body)
	}
}
