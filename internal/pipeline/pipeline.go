package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/extractor"
	"github.com/chuda123/trust-wallet-etl/internal/lake"
	"github.com/chuda123/trust-wallet-etl/internal/observe"
	"github.com/chuda123/trust-wallet-etl/internal/store"
	"github.com/chuda123/trust-wallet-etl/internal/transformer"
	"github.com/google/uuid"
)

type Pipeline struct {
	source    string
	extractor *extractor.Extractor
	store     *store.Store
	lake      *lake.Lake
	metrics   *observe.Metrics
	log       *slog.Logger
}

func New(source string, ex *extractor.Extractor, st *store.Store, lk *lake.Lake, m *observe.Metrics, log *slog.Logger) *Pipeline {
	return &Pipeline{source: source, extractor: ex, store: st, lake: lk, metrics: m, log: log}
}

// Run ticks every interval until ctx is cancelled. One cycle also runs immediately.
func (p *Pipeline) Run(ctx context.Context, interval time.Duration) {
	p.log.Info("pipeline started", "interval", interval.String())
	p.runOnce(ctx)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.log.Info("pipeline stopping")
			return
		case <-t.C:
			p.runOnce(ctx)
		}
	}
}

func (p *Pipeline) runOnce(ctx context.Context) {
	start := time.Now()
	err := p.cycle(ctx)
	p.metrics.PipelineDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		p.metrics.PipelineRuns.WithLabelValues("failure").Inc()
		p.log.Error("pipeline cycle failed", "error", err, "duration_ms", time.Since(start).Milliseconds())
		return
	}
	p.metrics.PipelineRuns.WithLabelValues("success").Inc()
	p.metrics.LastSuccess.Set(float64(time.Now().Unix()))
	p.log.Info("pipeline cycle complete", "duration_ms", time.Since(start).Milliseconds())
}

func (p *Pipeline) cycle(ctx context.Context) error {
	ingestedAt := time.Now().UTC()
	batchID := uuid.NewString()

	extractStart := time.Now()
	env, err := p.extractor.Fetch(ctx)
	p.metrics.ExtractDuration.Observe(time.Since(extractStart).Seconds())
	if err != nil {
		p.metrics.ExtractTotal.WithLabelValues("failure").Inc()
		p.log.Error("api request failed", "error", err, "batch_id", batchID)
		return err
	}
	p.metrics.ExtractTotal.WithLabelValues("success").Inc()
	p.metrics.ExtractRecords.Add(float64(len(env.RawResults)))
	p.log.Info("api request succeeded",
		"batch_id", batchID,
		"records", len(env.RawResults),
		"api_version", env.Info.Version,
	)

	if err := p.store.InsertRaw(ctx, ingestedAt, p.source, batchID, env.RawResults); err != nil {
		p.log.Error("postgres raw save failed", "error", err, "batch_id", batchID)
		return err
	}
	p.metrics.LoadTotal.WithLabelValues("postgres_raw").Add(float64(len(env.RawResults)))
	p.log.Info("data saved successfully", "sink", "postgres_raw", "records", len(env.RawResults), "batch_id", batchID)

	if err := p.lake.AppendRaw(ingestedAt, env.RawResults); err != nil {
		p.log.Error("lake raw save failed", "error", err, "batch_id", batchID)
		return err
	}
	p.metrics.LoadTotal.WithLabelValues("lake_raw").Add(float64(len(env.RawResults)))
	p.log.Info("data saved successfully", "sink", "lake_raw", "records", len(env.RawResults), "batch_id", batchID)

	results := transformer.TransformAll(env.RawResults, p.source, env.Info.Version, batchID, ingestedAt)
	ok := make([]transformer.Record, 0, len(results))
	asAny := make([]any, 0, len(results))
	for _, r := range results {
		if r.Err != nil {
			p.metrics.TransformErrors.Inc()
			p.log.Error("transformation error", "error", r.Err, "batch_id", batchID)
			continue
		}
		ok = append(ok, r.Record)
		asAny = append(asAny, r.Record)
	}
	if len(ok) == 0 {
		return errNoProcessed
	}

	if err := p.store.UpsertProcessed(ctx, ok); err != nil {
		p.log.Error("postgres processed save failed", "error", err, "batch_id", batchID)
		return err
	}
	p.metrics.LoadTotal.WithLabelValues("postgres_processed").Add(float64(len(ok)))
	p.log.Info("data saved successfully", "sink", "postgres_processed", "records", len(ok), "batch_id", batchID)

	if err := p.lake.AppendProcessed(ingestedAt, asAny); err != nil {
		p.log.Error("lake processed save failed", "error", err, "batch_id", batchID)
		return err
	}
	p.metrics.LoadTotal.WithLabelValues("lake_processed").Add(float64(len(ok)))
	p.log.Info("data saved successfully", "sink", "lake_processed", "records", len(ok), "batch_id", batchID)
	return nil
}

type cycleError string

func (e cycleError) Error() string { return string(e) }

const errNoProcessed cycleError = "no records survived transformation"
