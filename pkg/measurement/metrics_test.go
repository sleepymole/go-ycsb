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
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsHandler(t *testing.T) {
	p := properties.NewProperties()
	p.Set(prop.MetricsAddr, "127.0.0.1:0")
	InitMeasure(p)
	MeasureWithTable("table-a", "READ", time.Now(), 100*time.Microsecond)
	MeasureWithTable("table-a", "READ_ERROR", time.Now(), 200*time.Microsecond)
	MeasureWithTable("table-b", "READ", time.Now(), 300*time.Microsecond)
	Measure("SCAN", time.Now(), 400*time.Microsecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	resp := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(resp, req)

	if resp.Code != 200 {
		t.Fatalf("unexpected status code %d", resp.Code)
	}
	body := resp.Body.String()
	for _, metric := range []string{
		`go_ycsb_operations_total{operation="READ",result="ok",table="table-a"} 1`,
		`go_ycsb_operations_total{operation="READ",result="error",table="table-a"} 1`,
		`go_ycsb_operation_duration_millis_count{operation="READ",result="ok",table="table-a"} 1`,
		`go_ycsb_operation_duration_millis_sum{operation="READ",result="ok",table="table-a"} 0.1`,
		`go_ycsb_operation_duration_millis_count{operation="READ",result="error",table="table-a"} 1`,
		`go_ycsb_operation_duration_millis_sum{operation="READ",result="error",table="table-a"} 0.2`,
		`go_ycsb_operations_total{operation="READ",result="ok",table="table-b"} 1`,
		`go_ycsb_operation_duration_millis_sum{operation="READ",result="ok",table="table-b"} 0.3`,
		`go_ycsb_operations_total{operation="SCAN",result="ok",table="usertable"} 1`,
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics output does not contain %q:\n%s", metric, body)
		}
	}
}

func TestMetricsDisabled(t *testing.T) {
	InitMeasure(properties.NewProperties())

	req := httptest.NewRequest("GET", "/metrics", nil)
	resp := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(resp, req)

	if resp.Code != 404 {
		t.Fatalf("unexpected status code %d", resp.Code)
	}
}

func TestMetricsHistogramQuantileResolution(t *testing.T) {
	p := properties.NewProperties()
	p.Set(prop.MetricsAddr, "127.0.0.1:0")
	InitMeasure(p)

	const latencyMillis = 0.185
	for i := 0; i < 100; i++ {
		Measure("READ", time.Now(), 185*time.Microsecond)
	}

	histogram := gatherHistogram(t, "go_ycsb_operation_duration_millis")
	quantile := estimateHistogramQuantile(0.99, histogram)
	if quantile < latencyMillis/2 || quantile > latencyMillis*2 {
		t.Fatalf("estimated p99 = %fms, want within a factor of two of %fms", quantile, latencyMillis)
	}
	if got := prometheusLatencyBuckets[len(prometheusLatencyBuckets)-1]; got < 8000 {
		t.Fatalf("largest finite bucket = %fms, want at least 8000ms", got)
	}
}

func gatherHistogram(t *testing.T, name string) *dto.Histogram {
	t.Helper()

	metricFamilies, err := prometheusRegistry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() == name && len(family.Metric) == 1 {
			return family.Metric[0].GetHistogram()
		}
	}
	t.Fatalf("histogram %q not found", name)
	return nil
}

// estimateHistogramQuantile implements the interpolation used by
// histogram_quantile for a classic histogram with finite positive buckets.
func estimateHistogramQuantile(q float64, histogram *dto.Histogram) float64 {
	rank := q * float64(histogram.GetSampleCount())
	var previousCount uint64
	previousUpperBound := float64(0)
	for _, bucket := range histogram.Bucket {
		count := bucket.GetCumulativeCount()
		upperBound := bucket.GetUpperBound()
		if float64(count) >= rank {
			return previousUpperBound + (upperBound-previousUpperBound)*
				(rank-float64(previousCount))/float64(count-previousCount)
		}
		previousCount = count
		previousUpperBound = upperBound
	}
	return previousUpperBound
}
