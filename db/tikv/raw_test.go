// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package tikv

import (
	"context"
	"errors"
	"strings"
	"testing"

	perrors "github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/rawkv"
)

type rawCASClientStub struct {
	calls    int
	previous []byte
	value    []byte
	succeed  bool
	err      error
}

func (c *rawCASClientStub) CompareAndSwap(_ context.Context, _, previous, value []byte, _ ...rawkv.RawOption) ([]byte, bool, error) {
	c.calls++
	c.previous = append([]byte(nil), previous...)
	c.value = append([]byte(nil), value...)
	return nil, c.succeed, c.err
}

func TestCompareAndSwap(t *testing.T) {
	client := &rawCASClientStub{succeed: true}
	if err := compareAndSwap(context.Background(), client, []byte("key"), []byte("old"), []byte("new"), "key"); err != nil {
		t.Fatalf("compareAndSwap: %v", err)
	}
	if string(client.previous) != "old" || string(client.value) != "new" {
		t.Fatalf("CAS args = previous %q, value %q", client.previous, client.value)
	}
}

func TestCompareAndSwapConflict(t *testing.T) {
	client := &rawCASClientStub{}
	err := compareAndSwap(context.Background(), client, []byte("key"), []byte("old"), []byte("new"), "key")
	if err == nil || perrors.Cause(err) != ErrCASConflict || !strings.Contains(err.Error(), "key key") {
		t.Fatalf("compareAndSwap error = %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("CompareAndSwap calls = %d, want 1", client.calls)
	}
}

func TestCompareAndSwapError(t *testing.T) {
	wantErr := errors.New("transport")
	client := &rawCASClientStub{err: wantErr}
	if err := compareAndSwap(context.Background(), client, []byte("key"), []byte("old"), []byte("new"), "key"); !errors.Is(err, wantErr) {
		t.Fatalf("compareAndSwap error = %v, want %v", err, wantErr)
	}
}

func TestMergeRawKVValuesInPlace(t *testing.T) {
	readValues := map[string][]byte{
		"field0": []byte("old-0"),
		"field1": []byte("old-1"),
	}
	updateValues := map[string][]byte{
		"field1": []byte("new-1"),
	}

	merged := mergeRawKVValues(readValues, updateValues)
	if string(merged["field0"]) != "old-0" {
		t.Fatalf("merged field0 = %q, want old-0", merged["field0"])
	}
	if string(merged["field1"]) != "new-1" {
		t.Fatalf("merged field1 = %q, want new-1", merged["field1"])
	}
	if string(readValues["field1"]) != "new-1" {
		t.Fatalf("read values were not reused: %#v", readValues)
	}

	merged = mergeRawKVValues(nil, updateValues)
	merged["field2"] = []byte("new-2")
	if string(updateValues["field2"]) != "new-2" {
		t.Fatalf("update values were not reused for a missing row: %#v", updateValues)
	}
}
