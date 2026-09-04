package observe

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Registry *prometheus.Registry

	ExtractTotal    *prometheus.CounterVec
	ExtractDuration prometheus.Histogram
	ExtractRecords  prometheus.Counter

	TransformErrors  prometheus.Counter
	LoadTotal        *prometheus.CounterVec
	PipelineRuns     *prometheus.CounterVec
	PipelineDuration prometheus.Histogram
	LastSuccess      prometheus.Gauge
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		ExtractTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_extract_total",
			Help: "Completed extract calls (retries collapsed into one call) by result (success|failure).",
		}, []string{"status"}),
		ExtractDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "etl_extract_duration_seconds",
			Help:    "Time spent calling the source API, including retries.",
			Buckets: prometheus.DefBuckets,
		}),
		ExtractRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etl_records_extracted_total",
			Help: "Raw records pulled from the source API.",
		}),
		TransformErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etl_transform_errors_total",
			Help: "Records that failed normalization.",
		}),
		LoadTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_records_loaded_total",
			Help: "Records successfully written to a sink.",
		}, []string{"sink"}),
		PipelineRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_pipeline_runs_total",
			Help: "End-to-end pipeline cycles by result.",
		}, []string{"status"}),
		PipelineDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "etl_pipeline_duration_seconds",
			Help:    "End-to-end duration of one extract-transform-load cycle.",
			Buckets: prometheus.DefBuckets,
		}),
		LastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "etl_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last fully successful pipeline cycle.",
		}),
	}
	reg.MustRegister(
		m.ExtractTotal,
		m.ExtractDuration,
		m.ExtractRecords,
		m.TransformErrors,
		m.LoadTotal,
		m.PipelineRuns,
		m.PipelineDuration,
		m.LastSuccess,
	)
	return m
}
