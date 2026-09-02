// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package measurement

import (
	"sort"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/pingcap/go-ycsb/pkg/util"
)

type histogram struct {
	boundCounts       util.ConcurrentMap
	startTime         time.Time
	hist              *hdrhistogram.Histogram
	intervalStartTime time.Time
	intervalHist      *hdrhistogram.Histogram
}

// Metric name.
const (
	ELAPSED   = "ELAPSED"
	COUNT     = "COUNT"
	QPS       = "QPS"
	AVG       = "AVG"
	MIN       = "MIN"
	MAX       = "MAX"
	PER50TH   = "PER50TH"
	PER90TH   = "PER90TH"
	PER95TH   = "PER95TH"
	PER99TH   = "PER99TH"
	PER999TH  = "PER999TH"
	PER9999TH = "PER9999TH"
)

func newHistogram() *histogram {
	return newHistogramAt(time.Now())
}

func newHistogramAt(intervalStartTime time.Time) *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	h.hist = newHdrHistogram()
	h.intervalStartTime = intervalStartTime
	h.intervalHist = newHdrHistogram()
	return h
}

func newHdrHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(1, 24*60*60*1000*1000, 3)
}

func (h *histogram) Measure(latency time.Duration) {
	value := latency.Microseconds()
	h.hist.RecordValue(value)
	h.intervalHist.RecordValue(value)
}

func (h *histogram) Summary() []string {
	res := h.getInfo(h.hist, h.startTime, time.Now())

	return formatSummary(res)
}

// IntervalSummary returns measurements since the previous interval summary,
// then starts a new interval. The cumulative Summary remains unchanged.
func (h *histogram) IntervalSummary() []string {
	return h.intervalSummaryAt(time.Now())
}

func (h *histogram) intervalSummaryAt(endTime time.Time) []string {
	res := h.getInfo(h.intervalHist, h.intervalStartTime, endTime)
	h.intervalHist.Reset()
	h.intervalStartTime = endTime

	return formatSummary(res)
}

func formatSummary(res map[string]interface{}) []string {

	return []string{
		util.FloatToOneString(res[ELAPSED]),
		util.IntToString(res[COUNT]),
		util.FloatToOneString(res[QPS]),
		util.IntToString(res[AVG]),
		util.IntToString(res[MIN]),
		util.IntToString(res[MAX]),
		util.IntToString(res[PER50TH]),
		util.IntToString(res[PER90TH]),
		util.IntToString(res[PER95TH]),
		util.IntToString(res[PER99TH]),
		util.IntToString(res[PER999TH]),
		util.IntToString(res[PER9999TH]),
	}
}

func (h *histogram) getInfo(hist *hdrhistogram.Histogram, startTime, endTime time.Time) map[string]interface{} {
	min := hist.Min()
	max := hist.Max()
	avg := int64(hist.Mean())
	count := hist.TotalCount()

	bounds := h.boundCounts.Keys()
	sort.Ints(bounds)

	per50 := hist.ValueAtPercentile(50)
	per90 := hist.ValueAtPercentile(90)
	per95 := hist.ValueAtPercentile(95)
	per99 := hist.ValueAtPercentile(99)
	per999 := hist.ValueAtPercentile(99.9)
	per9999 := hist.ValueAtPercentile(99.99)

	elapsed := endTime.Sub(startTime).Seconds()
	qps := float64(0)
	if elapsed > 0 {
		qps = float64(count) / elapsed
	}
	res := make(map[string]interface{})
	res[ELAPSED] = elapsed
	res[COUNT] = count
	res[QPS] = qps
	res[AVG] = avg
	res[MIN] = min
	res[MAX] = max
	res[PER50TH] = per50
	res[PER90TH] = per90
	res[PER95TH] = per95
	res[PER99TH] = per99
	res[PER999TH] = per999
	res[PER9999TH] = per9999

	return res
}
