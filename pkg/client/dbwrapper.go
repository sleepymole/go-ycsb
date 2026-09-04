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

package client

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// DbWrapper stores the pointer to a implementation of ycsb.DB.
type DbWrapper struct {
	DB ycsb.DB
}

var _ ycsb.ReadModifyWriteDB = DbWrapper{}

func measure(start time.Time, table, op string, err error) {
	lan := time.Since(start)
	if err != nil {
		measurement.MeasureWithTable(table, fmt.Sprintf("%s_ERROR", op), start, lan)
		return
	}

	measurement.MeasureWithTable(table, op, start, lan)
	measurement.MeasureWithTable(table, "TOTAL", start, lan)
}

func (db DbWrapper) Close() error {
	return db.DB.Close()
}

func (db DbWrapper) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return db.DB.InitThread(ctx, threadID, threadCount)
}

func (db DbWrapper) CleanupThread(ctx context.Context) {
	db.DB.CleanupThread(ctx)
}

func (db DbWrapper) Read(ctx context.Context, table string, key string, fields []string) (_ map[string][]byte, err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "READ", err)
	}()

	return db.DB.Read(ctx, table, key, fields)
}

// ReadForUpdate forwards the optional read-modify-write capability while
// keeping the normal READ measurement. Databases without the capability still
// use Read and return a nil opaque value.
func (db DbWrapper) ReadForUpdate(ctx context.Context, table string, key string, fields []string) (_ map[string][]byte, _ []byte, err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "READ", err)
	}()

	if rmwDB, ok := db.DB.(ycsb.ReadModifyWriteDB); ok {
		return rmwDB.ReadForUpdate(ctx, table, key, fields)
	}
	values, err := db.DB.Read(ctx, table, key, fields)
	return values, nil, err
}

func (db DbWrapper) BatchRead(ctx context.Context, table string, keys []string, fields []string) (_ []map[string][]byte, err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(start, table, "BATCH_READ", err)
		}()
		return batchDB.BatchRead(ctx, table, keys, fields)
	}
	for _, key := range keys {
		_, err := db.DB.Read(ctx, table, key, fields)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (db DbWrapper) Scan(ctx context.Context, table string, startKey string, count int, fields []string) (_ []map[string][]byte, err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "SCAN", err)
	}()

	return db.DB.Scan(ctx, table, startKey, count, fields)
}

func (db DbWrapper) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "UPDATE", err)
	}()

	return db.DB.Update(ctx, table, key, values)
}

// UpdateWithRead implements the optional read-modify-write capability. It
// preserves the existing UPDATE measurement while allowing databases that can
// merge an already-read row to avoid an extra read.
func (db DbWrapper) UpdateWithRead(ctx context.Context, table string, key string, readValues map[string][]byte, readValue []byte, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "UPDATE", err)
	}()

	if rmwDB, ok := db.DB.(ycsb.ReadModifyWriteDB); ok {
		return rmwDB.UpdateWithRead(ctx, table, key, readValues, readValue, values)
	}
	return db.DB.Update(ctx, table, key, values)
}

func (db DbWrapper) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(start, table, "BATCH_UPDATE", err)
		}()
		return batchDB.BatchUpdate(ctx, table, keys, values)
	}
	for i := range keys {
		err := db.DB.Update(ctx, table, keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db DbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "INSERT", err)
	}()

	return db.DB.Insert(ctx, table, key, values)
}

func (db DbWrapper) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(start, table, "BATCH_INSERT", err)
		}()
		return batchDB.BatchInsert(ctx, table, keys, values)
	}
	for i := range keys {
		err := db.DB.Insert(ctx, table, keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db DbWrapper) Delete(ctx context.Context, table string, key string) (err error) {
	start := time.Now()
	defer func() {
		measure(start, table, "DELETE", err)
	}()

	return db.DB.Delete(ctx, table, key)
}

func (db DbWrapper) BatchDelete(ctx context.Context, table string, keys []string) (err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(start, table, "BATCH_DELETE", err)
		}()
		return batchDB.BatchDelete(ctx, table, keys)
	}
	for _, key := range keys {
		err := db.DB.Delete(ctx, table, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db DbWrapper) Analyze(ctx context.Context, table string) error {
	if analyzeDB, ok := db.DB.(ycsb.AnalyzeDB); ok {
		return analyzeDB.Analyze(ctx, table)
	}
	return nil
}
