package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Envelope is the Random User API response. RawResults keep the original JSON
// bytes for the immutable raw layer (Postgres + data lake).
type Envelope struct {
	Results    []json.RawMessage `json:"results"`
	Info       Info              `json:"info"`
	RawResults []json.RawMessage `json:"-"`
}

type Info struct {
	Seed    string `json:"seed"`
	Results int    `json:"results"`
	Page    int    `json:"page"`
	Version string `json:"version"`
}

type Extractor struct {
	client  *http.Client
	baseURL string
	results int
	retries int
}

func New(baseURL string, results int, timeout time.Duration) *Extractor {
	return &Extractor{
		client:  &http.Client{Timeout: timeout},
		baseURL: baseURL,
		results: results,
		retries: 3,
	}
}

// Fetch pulls a batch from the public API with bounded retries.
func (e *Extractor) Fetch(ctx context.Context) (Envelope, error) {
	var lastErr error
	backoff := 400 * time.Millisecond
	for attempt := 1; attempt <= e.retries; attempt++ {
		env, err := e.fetchOnce(ctx)
		if err == nil {
			return env, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	return Envelope{}, fmt.Errorf("extract failed after %d attempts: %w", e.retries, lastErr)
}

func (e *Extractor) fetchOnce(ctx context.Context) (Envelope, error) {
	u, err := url.Parse(e.baseURL)
	if err != nil {
		return Envelope{}, fmt.Errorf("api url: %w", err)
	}
	q := u.Query()
	q.Set("results", strconv.Itoa(e.results))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Envelope{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "trust-wallet-etl/1.0")

	resp, err := e.client.Do(req)
	if err != nil {
		return Envelope{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return Envelope{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Envelope{}, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, truncate(body, 200))
	}

	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Envelope{}, fmt.Errorf("decode: %w", err)
	}
	if len(env.Results) == 0 {
		return Envelope{}, fmt.Errorf("empty result set")
	}
	env.RawResults = env.Results
	return env, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
