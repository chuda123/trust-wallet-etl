package extractor

import (
	"context"
	"encoding/json"
	"errors"
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

type statusError struct {
	code int
	body string
}

func (e statusError) Error() string {
	return fmt.Sprintf("unexpected status %d: %s", e.code, e.body)
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

// Fetch pulls a batch from the public API with bounded retries on transient errors.
func (e *Extractor) Fetch(ctx context.Context) (Envelope, error) {
	var lastErr error
	backoff := 400 * time.Millisecond
	for attempt := 1; attempt <= e.retries; attempt++ {
		env, err := e.fetchOnce(ctx)
		if err == nil {
			return env, nil
		}
		lastErr = err
		if !retryable(err) || attempt == e.retries {
			break
		}
		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	return Envelope{}, fmt.Errorf("extract failed after retries: %w", lastErr)
}

func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var se statusError
	if errors.As(err, &se) {
		return se.code == http.StatusTooManyRequests || se.code >= 500
	}
	// Timeouts and network errors wrap here; decode/empty-result do not.
	var de decodeError
	if errors.As(err, &de) {
		return false
	}
	return true
}

type decodeError struct{ err error }

func (e decodeError) Error() string { return e.err.Error() }
func (e decodeError) Unwrap() error { return e.err }

func (e *Extractor) fetchOnce(ctx context.Context) (Envelope, error) {
	u, err := url.Parse(e.baseURL)
	if err != nil {
		return Envelope{}, decodeError{err: fmt.Errorf("api url: %w", err)}
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
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
		}
		return Envelope{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return Envelope{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Envelope{}, statusError{code: resp.StatusCode, body: truncate(body, 200)}
	}

	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Envelope{}, decodeError{err: fmt.Errorf("decode: %w", err)}
	}
	if len(env.Results) == 0 {
		return Envelope{}, decodeError{err: fmt.Errorf("empty result set")}
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
