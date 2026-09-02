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
	"io"
	"sync"
	"testing"
	"time"
)

type blockingIntervalMeasurer struct {
	entered chan struct{}
	release chan struct{}
}

func (m *blockingIntervalMeasurer) Measure(string, time.Time, time.Duration) {}
func (m *blockingIntervalMeasurer) Summary()                                 {}
func (m *blockingIntervalMeasurer) GenerateExtendedOutputs()                 {}
func (m *blockingIntervalMeasurer) Output(io.Writer) error                   { return nil }

func (m *blockingIntervalMeasurer) IntervalSummary() {
	m.entered <- struct{}{}
	<-m.release
}

func TestIntervalSummaryIsSerialized(t *testing.T) {
	measurer := &blockingIntervalMeasurer{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	m := &measurement{measurer: measurer}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.summary()
	}()
	<-measurer.entered

	secondStarted := make(chan struct{})
	go func() {
		defer wg.Done()
		close(secondStarted)
		m.summary()
	}()
	<-secondStarted

	concurrent := false
	select {
	case <-measurer.entered:
		concurrent = true
	case <-time.After(100 * time.Millisecond):
	}
	close(measurer.release)
	wg.Wait()

	if concurrent {
		t.Fatal("interval summaries ran concurrently")
	}
}
