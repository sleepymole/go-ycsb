// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package measurement

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type prometheusMetrics struct {
	operations *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

var (
	prometheusRegistry *prometheus.Registry
	prometheusMu       sync.RWMutex
)

// Cover one microsecond through about eight seconds while keeping adjacent
// buckets close enough for useful histogram_quantile estimates.
var prometheusLatencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 24)

func initPrometheusMetrics() *prometheusMetrics {
	collectors := &prometheusMetrics{
		operations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "go_ycsb",
				Name:      "operations_total",
				Help:      "Total number of YCSB operations.",
			},
			[]string{"table", "operation", "result"},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "go_ycsb",
				Name:      "operation_duration_millis",
				Help:      "YCSB operation latency in milliseconds.",
				Buckets:   prometheusLatencyBuckets,
			},
			[]string{"table", "operation", "result"},
		),
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.operations, collectors.duration)

	prometheusMu.Lock()
	prometheusRegistry = registry
	prometheusMu.Unlock()

	return collectors
}

func (m *prometheusMetrics) observe(table, op string, latency time.Duration) {
	result := "ok"
	if strings.HasSuffix(op, "_ERROR") {
		result = "error"
		op = strings.TrimSuffix(op, "_ERROR")
	}
	m.operations.WithLabelValues(table, op, result).Inc()
	m.duration.WithLabelValues(table, op, result).Observe(float64(latency) / float64(time.Millisecond))
}

// MetricsHandler returns the Prometheus exposition handler for this process.
func MetricsHandler() http.Handler {
	prometheusMu.RLock()
	registry := prometheusRegistry
	prometheusMu.RUnlock()
	if registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

// StartMetricsServer starts a Prometheus metrics endpoint on addr. The caller
// owns the returned function and should call it when the benchmark exits.
func StartMetricsServer(addr string) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", MetricsHandler())
	server := &http.Server{Handler: mux}
	go func() {
		_ = server.Serve(listener)
	}()

	return func() {
		_ = server.Close()
	}, nil
}
