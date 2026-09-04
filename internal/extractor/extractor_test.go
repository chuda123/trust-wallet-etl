package extractor_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/extractor"
)

func TestFetchSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{"login": map[string]string{"uuid": "abc"}}},
			"info":    map[string]any{"version": "1.4", "results": 1, "page": 1, "seed": "x"},
		})
	}))
	defer srv.Close()

	ex := extractor.New(srv.URL, 1, 2*time.Second)
	env, err := ex.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(env.RawResults) != 1 {
		t.Fatalf("records=%d", len(env.RawResults))
	}
}

func TestFetchRetriesThenFails(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ex := extractor.New(srv.URL, 1, 2*time.Second)
	_, err := ex.Fetch(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if hits < 2 {
		t.Fatalf("expected retries, hits=%d", hits)
	}
}

func TestFetchDoesNotRetryClientErrors(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()

	ex := extractor.New(srv.URL, 1, 2*time.Second)
	_, err := ex.Fetch(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if hits != 1 {
		t.Fatalf("should not retry 400, hits=%d", hits)
	}
}
