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
	"testing"
	"time"
)

func TestIntervalSummaryResetsWithoutAffectingCumulative(t *testing.T) {
	h := newHistogram()
	intervalHist := h.intervalHist
	h.Measure(10 * time.Millisecond)

	first := h.IntervalSummary()
	if got, want := first[1], "1"; got != want {
		t.Fatalf("first interval count = %s, want %s", got, want)
	}

	h.Measure(20 * time.Millisecond)
	second := h.IntervalSummary()
	if got, want := second[1], "1"; got != want {
		t.Fatalf("second interval count = %s, want %s", got, want)
	}
	if h.intervalHist != intervalHist {
		t.Fatal("interval summary replaced the HDR histogram instead of resetting it")
	}

	cumulative := h.Summary()
	if got, want := cumulative[1], "2"; got != want {
		t.Fatalf("cumulative count = %s, want %s", got, want)
	}
}
