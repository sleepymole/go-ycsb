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

package main

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	clientrawkv "github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/pd/client/clients/router"
)

type fakeRegionScanner struct {
	mu       sync.Mutex
	regions  []*router.Region
	pageSize int
	calls    int
	limits   []int
}

func (s *fakeRegionScanner) ScanRegions(_ context.Context, startKey, endKey []byte, limit int) ([]*router.Region, error) {
	s.mu.Lock()
	s.calls++
	s.limits = append(s.limits, limit)
	s.mu.Unlock()
	if s.pageSize > 0 && s.pageSize < limit {
		limit = s.pageSize
	}
	var result []*router.Region
	for _, region := range s.regions {
		regionStart, regionEnd := region.Meta.StartKey, region.Meta.EndKey
		if len(regionEnd) > 0 && bytes.Compare(regionEnd, startKey) <= 0 {
			continue
		}
		if len(endKey) > 0 && bytes.Compare(regionStart, endKey) >= 0 {
			break
		}
		result = append(result, region)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *fakeRegionScanner) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newTestRegion(startKey, endKey string) *router.Region {
	return &router.Region{Meta: &metapb.Region{StartKey: []byte(startKey), EndKey: []byte(endKey)}}
}

func TestWalkRawKVRangeByRegions(t *testing.T) {
	scanner := &fakeRegionScanner{pageSize: 2, regions: []*router.Region{
		newTestRegion("", "m"),
		newTestRegion("m", "t"),
		newTestRegion("t", ""),
	}}
	var ranges []rawKVRange
	regionCount, err := walkRawKVRangeByRegions(
		context.Background(), scanner, []byte("b"), []byte("z"), func(keyRange rawKVRange) error {
			ranges = append(ranges, keyRange)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("split range: %v", err)
	}
	want := []rawKVRange{
		{startKey: []byte("b"), endKey: []byte("m")},
		{startKey: []byte("m"), endKey: []byte("t")},
		{startKey: []byte("t"), endKey: []byte("z")},
	}
	if len(ranges) != len(want) {
		t.Fatalf("range count = %d, want %d", len(ranges), len(want))
	}
	if regionCount != len(want) {
		t.Fatalf("reported region count = %d, want %d", regionCount, len(want))
	}
	if calls := scanner.callCount(); calls != 2 {
		t.Fatalf("ScanRegions calls = %d, want 2", calls)
	}
	scanner.mu.Lock()
	limits := append([]int(nil), scanner.limits...)
	scanner.mu.Unlock()
	for i, limit := range limits {
		if limit != regionScanPageSize {
			t.Fatalf("ScanRegions call %d limit = %d, want %d", i, limit, regionScanPageSize)
		}
	}
	for i := range want {
		if !bytes.Equal(ranges[i].startKey, want[i].startKey) || !bytes.Equal(ranges[i].endKey, want[i].endKey) {
			t.Fatalf("range %d = [%q, %q), want [%q, %q)", i,
				ranges[i].startKey, ranges[i].endKey, want[i].startKey, want[i].endKey)
		}
	}
}

type blockingChecksummer struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (c *blockingChecksummer) Checksum(
	ctx context.Context, _, _ []byte, _ ...clientrawkv.RawOption,
) (clientrawkv.RawChecksum, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		return clientrawkv.RawChecksum{}, nil
	case <-ctx.Done():
		return clientrawkv.RawChecksum{}, ctx.Err()
	}
}

func TestChecksumRawKVPaginatesWithBackpressure(t *testing.T) {
	scanner := &fakeRegionScanner{pageSize: 2, regions: []*router.Region{
		newTestRegion("", "g"),
		newTestRegion("g", "n"),
		newTestRegion("n", "u"),
		newTestRegion("u", ""),
	}}
	checksummer := &blockingChecksummer{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := checksumRawKV(
			context.Background(), checksummer, scanner, []byte("b"), []byte("z"), 1,
		)
		done <- err
	}()

	<-checksummer.started
	if calls := scanner.callCount(); calls != 1 {
		t.Fatalf("ScanRegions calls before the first page advances = %d, want 1", calls)
	}
	close(checksummer.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("checksum rawkv: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("checksum rawkv did not finish")
	}
	if calls := scanner.callCount(); calls != 2 {
		t.Fatalf("ScanRegions calls after completion = %d, want 2", calls)
	}
}

type concurrentChecksummer struct {
	mu        sync.Mutex
	active    int
	maxActive int
	checksums map[string]clientrawkv.RawChecksum
}

func (c *concurrentChecksummer) Checksum(
	_ context.Context, startKey, _ []byte, _ ...clientrawkv.RawOption,
) (clientrawkv.RawChecksum, error) {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()

	time.Sleep(20 * time.Millisecond)

	c.mu.Lock()
	c.active--
	checksum := c.checksums[string(startKey)]
	c.mu.Unlock()
	return checksum, nil
}

func TestChecksumRawKVByRegionConcurrently(t *testing.T) {
	scanner := &fakeRegionScanner{regions: []*router.Region{
		newTestRegion("", "g"),
		newTestRegion("g", "n"),
		newTestRegion("n", "u"),
		newTestRegion("u", ""),
	}}
	checksummer := &concurrentChecksummer{checksums: map[string]clientrawkv.RawChecksum{
		"b": {Crc64Xor: 1, TotalKvs: 2, TotalBytes: 20},
		"g": {Crc64Xor: 2, TotalKvs: 3, TotalBytes: 30},
		"n": {Crc64Xor: 4, TotalKvs: 5, TotalBytes: 50},
		"u": {Crc64Xor: 8, TotalKvs: 7, TotalBytes: 70},
	}}

	checksum, regions, err := checksumRawKV(
		context.Background(), checksummer, scanner, []byte("b"), []byte("z"), 3,
	)
	if err != nil {
		t.Fatalf("checksum rawkv: %v", err)
	}
	if checksum.Crc64Xor != 15 || checksum.TotalKvs != 17 || checksum.TotalBytes != 170 {
		t.Fatalf("checksum = %+v, want crc64=15 total_kvs=17 total_bytes=170", checksum)
	}
	if regions != 4 {
		t.Fatalf("regions = %d, want 4", regions)
	}
	checksummer.mu.Lock()
	maxActive := checksummer.maxActive
	checksummer.mu.Unlock()
	if maxActive < 2 {
		t.Fatalf("maximum concurrent checksums = %d, want at least 2", maxActive)
	}
}
