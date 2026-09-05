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
	body, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("%s: want 2 lines, got %d (%q)", part, len(lines), body)
	}
}

func TestAppendDoesNotWriteUnpartitionedCopies(t *testing.T) {
	dir := t.TempDir()
	lk, err := lake.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := lk.AppendRaw(ts, []json.RawMessage{json.RawMessage(`{"id":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err := lk.AppendProcessed(ts, []any{map[string]int{"id": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := lk.AppendDLQ(ts, []lake.DeadLetter{{Error: "boom", Payload: json.RawMessage(`{"id":1}`)}}); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"raw_data.json", "processed_data.json", "dlq.json"} {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("unpartitioned copy %s should not exist", p)
		}
	}
}

func TestAppendSplitsByUTCDate(t *testing.T) {
	dir := t.TempDir()
	lk, err := lake.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	day1 := time.Date(2026, 9, 4, 23, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
	if err := lk.AppendRaw(day1, []json.RawMessage{json.RawMessage(`{"id":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err := lk.AppendRaw(day2, []json.RawMessage{json.RawMessage(`{"id":2}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raw", "dt=2026-09-04", "events.ndjson")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raw", "dt=2026-09-05", "events.ndjson")); err != nil {
		t.Fatal(err)
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
