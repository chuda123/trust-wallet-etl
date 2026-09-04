package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/transformer"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) ensureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS raw_events (
    id           BIGSERIAL PRIMARY KEY,
    ingested_at  TIMESTAMPTZ NOT NULL,
    source       TEXT NOT NULL,
    batch_id     UUID NOT NULL,
    payload      JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS raw_events_ingested_at_idx ON raw_events (ingested_at);
CREATE INDEX IF NOT EXISTS raw_events_batch_id_idx ON raw_events (batch_id);

CREATE TABLE IF NOT EXISTS processed_users (
    source_uuid   UUID PRIMARY KEY,
    ingested_at   TIMESTAMPTZ NOT NULL,
    batch_id      UUID NOT NULL,
    email         TEXT,
    country       TEXT,
    payload       JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS processed_users_ingested_at_idx ON processed_users (ingested_at);
CREATE INDEX IF NOT EXISTS processed_users_country_idx ON processed_users (country);
`
	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

// InsertRaw appends immutable source records. The lake and this table are the
// system of record for replay; we never update or delete here.
func (s *Store) InsertRaw(ctx context.Context, ingestedAt time.Time, source, batchID string, payloads []json.RawMessage) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	const q = `INSERT INTO raw_events (ingested_at, source, batch_id, payload) VALUES ($1, $2, $3, $4)`
	for _, p := range payloads {
		if _, err := tx.Exec(ctx, q, ingestedAt.UTC(), source, batchID, p); err != nil {
			return fmt.Errorf("insert raw: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// UpsertProcessed keeps the current snapshot by natural key (source_uuid).
// Historical versions still live in the processed lake as an append-only log.
func (s *Store) UpsertProcessed(ctx context.Context, records []transformer.Record) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	const q = `
INSERT INTO processed_users (source_uuid, ingested_at, batch_id, email, country, payload)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (source_uuid) DO UPDATE SET
    ingested_at = EXCLUDED.ingested_at,
    batch_id    = EXCLUDED.batch_id,
    email       = EXCLUDED.email,
    country     = EXCLUDED.country,
    payload     = EXCLUDED.payload
`
	for _, rec := range records {
		payload, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		ingestedAt, err := time.Parse(time.RFC3339Nano, rec.Meta.IngestedAt)
		if err != nil {
			return fmt.Errorf("ingested_at: %w", err)
		}
		if _, err := tx.Exec(ctx, q,
			rec.User.SourceUUID,
			ingestedAt,
			rec.Meta.BatchID,
			rec.User.Email,
			rec.User.Location.Country,
			payload,
		); err != nil {
			return fmt.Errorf("upsert processed: %w", err)
		}
	}
	return tx.Commit(ctx)
}
