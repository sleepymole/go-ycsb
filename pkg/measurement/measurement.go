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
	"bufio"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

var header = []string{"Operation", "Takes(s)", "Count", "OPS", "Avg(us)", "Min(us)", "Max(us)", "50th(us)", "90th(us)", "95th(us)", "99th(us)", "99.9th(us)", "99.99th(us)"}

type measurement struct {
	sync.RWMutex

	p *properties.Properties

	measurer   ycsb.Measurer
	prometheus *prometheusMetrics
}

// intervalSummarizer is implemented by measurers that can report and reset
// the measurements collected since the previous periodic summary. Measurers
// that do not implement it retain the historical cumulative Summary behavior.
type intervalSummarizer interface {
	IntervalSummary()
}

func (m *measurement) measure(table, op string, start time.Time, lan time.Duration) {
	m.Lock()
	m.measurer.Measure(op, start, lan)
	m.Unlock()

	// Prometheus collectors are concurrency-safe. Keep their work outside the
	// measurer lock so enabling metrics does not extend the global critical
	// section shared by all benchmark workers.
	if m.prometheus != nil {
		m.prometheus.observe(table, op, lan)
	}
}

func (m *measurement) output() {
	m.RLock()
	defer m.RUnlock()

	outFile := m.p.GetString(prop.MeasurementRawOutputFile, "")
	var w *bufio.Writer
	if outFile == "" {
		w = bufio.NewWriter(os.Stdout)
	} else {
		f, err := os.Create(outFile)
		if err != nil {
			panic("failed to create output file: " + err.Error())
		}
		defer f.Close()
		w = bufio.NewWriter(f)
	}

	err := globalMeasure.measurer.Output(w)
	if err != nil {
		panic("failed to write output: " + err.Error())
	}

	err = w.Flush()
	if err != nil {
		panic("failed to flush output: " + err.Error())
	}
}

func (m *measurement) summary() {
	m.Lock()
	defer m.Unlock()
	if interval, ok := m.measurer.(intervalSummarizer); ok {
		interval.IntervalSummary()
		return
	}
	m.measurer.Summary()
}

// InitMeasure initializes the global measurement.
func InitMeasure(p *properties.Properties) {
	globalMeasure = new(measurement)
	globalMeasure.p = p
	if p.GetString(prop.MetricsAddr, prop.MetricsAddrDefault) != "" {
		globalMeasure.prometheus = initPrometheusMetrics()
	} else {
		prometheusMu.Lock()
		prometheusRegistry = nil
		prometheusMu.Unlock()
	}
	measurementType := p.GetString(prop.MeasurementType, prop.MeasurementTypeDefault)
	switch measurementType {
	case "histogram":
		globalMeasure.measurer = InitHistograms(p)
	case "raw", "csv":
		globalMeasure.measurer = InitCSV()
	default:
		panic("unsupported measurement type: " + measurementType)
	}
	EnableWarmUp(p.GetInt64(prop.WarmUpTime, 0) > 0)
}

// Output prints the complete measurements.
func Output() {
	globalMeasure.measurer.GenerateExtendedOutputs()
	globalMeasure.output()
}

// Summary prints the measurement summary.
func Summary() {
	globalMeasure.summary()
}

// EnableWarmUp sets whether to enable warm-up.
func EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&warmUp, 1)
	} else {
		atomic.StoreInt32(&warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func IsWarmUpFinished() bool {
	return atomic.LoadInt32(&warmUp) == 0
}

// Measure measures the operation.
func Measure(op string, start time.Time, lan time.Duration) {
	table := globalMeasure.p.GetString(prop.TableName, prop.TableNameDefault)
	MeasureWithTable(table, op, start, lan)
}

// MeasureWithTable measures the operation and associates its Prometheus
// metrics with the table used by the request.
func MeasureWithTable(table, op string, start time.Time, lan time.Duration) {
	if IsWarmUpFinished() {
		globalMeasure.measure(table, op, start, lan)
	}
}

var globalMeasure *measurement
var warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
