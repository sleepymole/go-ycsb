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

package tikv

import (
	"context"
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/errors"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/rawkv"
)

type rawDB struct {
	db           *rawkv.Client
	r            *util.RowCodec
	bufPool      *util.BufPool
	atomicForCAS bool
}

type rawCASClient interface {
	CompareAndSwap(context.Context, []byte, []byte, []byte, ...rawkv.RawOption) ([]byte, bool, error)
}

// ErrCASConflict indicates that the value read before the update no longer
// matches the value stored in RawKV.
var ErrCASConflict = errors.New("rawkv compare-and-swap conflict")

var _ ycsb.ReadModifyWriteDB = (*rawDB)(nil)

func createRawDB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	apiVersionStr := strings.ToUpper(p.GetString(tikvAPIVersion, "V1"))
	apiVersion, ok := kvrpcpb.APIVersion_value[apiVersionStr]
	if !ok {
		return nil, errors.Errorf("Invalid tikv apiversion %s.", apiVersionStr)
	}
	db, err := rawkv.NewClientWithOpts(context.Background(), strings.Split(pdAddr, ","),
		rawkv.WithAPIVersion(kvrpcpb.APIVersion(apiVersion)))
	if err != nil {
		return nil, err
	}
	atomicForCAS := p.GetBool(tikvCAS, false)
	db.SetAtomicForCAS(atomicForCAS)

	bufPool := util.NewBufPool()

	return &rawDB{
		db:           db,
		r:            util.NewRowCodec(p),
		bufPool:      bufPool,
		atomicForCAS: atomicForCAS,
	}, nil
}

func (db *rawDB) Close() error {
	return db.db.Close()
}

func (db *rawDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rawDB) CleanupThread(ctx context.Context) {
}

func (db *rawDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *rawDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	values, _, err := db.read(ctx, table, key, fields, false)
	return values, err
}

func (db *rawDB) ReadForUpdate(ctx context.Context, table string, key string, _ []string) (map[string][]byte, []byte, error) {
	// RawKV stores the complete row as one value. Decode all fields so the
	// subsequent merge can write the whole row without dropping unselected
	// fields, regardless of the workload's readallfields setting.
	return db.read(ctx, table, key, nil, true)
}

func (db *rawDB) read(ctx context.Context, table string, key string, fields []string, keepRaw bool) (map[string][]byte, []byte, error) {
	row, err := db.db.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil, nil, err
	} else if row == nil {
		return nil, nil, nil
	}

	values, err := db.r.Decode(row, fields)
	if err != nil {
		return nil, nil, err
	}
	if !keepRaw {
		return values, nil, nil
	}
	// Return the exact bytes for CAS. Decode only creates slices into row, so
	// both results remain valid without copying the RawKV response.
	return values, row, nil
}

func (db *rawDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	values, err := db.db.BatchGet(ctx, rowKeys)
	if err != nil {
		return nil, err
	}
	rowValues := make([]map[string][]byte, len(keys))

	for i, value := range values {
		if len(value) > 0 {
			rowValues[i], err = db.r.Decode(value, fields)
		} else {
			rowValues[i] = nil
		}
	}
	return rowValues, nil
}

func (db *rawDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	_, rows, err := db.db.Scan(ctx, db.getRowKey(table, startKey), nil, count)
	if err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

func (db *rawDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	row, err := db.db.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil
	}

	data, err := db.r.Decode(row, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	return db.Insert(ctx, table, key, data)
}

// UpdateWithRead merges values into the row returned by the preceding read
// and writes the merged row directly. When CAS is enabled, readValue is sent
// as the compare value so a concurrent update cannot be overwritten silently.
func (db *rawDB) UpdateWithRead(ctx context.Context, table string, key string, readValues map[string][]byte, readValue []byte, values map[string][]byte) error {
	data := mergeRawKVValues(readValues, values)

	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()
	buf, err := db.r.Encode(buf, data)
	if err != nil {
		return err
	}

	rawKey := db.getRowKey(table, key)
	if !db.atomicForCAS {
		return db.db.Put(ctx, rawKey, buf)
	}

	return compareAndSwap(ctx, db.db, rawKey, readValue, buf, key)
}

func compareAndSwap(ctx context.Context, client rawCASClient, rawKey, readValue, newValue []byte, key string) error {
	_, swapped, err := client.CompareAndSwap(ctx, rawKey, readValue, newValue)
	if err != nil {
		return err
	}
	if !swapped {
		return errors.Annotatef(ErrCASConflict, "key %s", key)
	}
	return nil
}

func mergeRawKVValues(readValues, values map[string][]byte) map[string][]byte {
	if readValues == nil {
		return values
	}
	for field, value := range values {
		readValues[field] = value
	}
	return readValues
}

func (db *rawDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		// TODO should we check the key exist?
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.BatchPut(ctx, rawKeys, rawValues)
}

func (db *rawDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	return db.db.Put(ctx, db.getRowKey(table, key), buf)
}

func (db *rawDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.BatchPut(ctx, rawKeys, rawValues)
}

func (db *rawDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(ctx, db.getRowKey(table, key))
}

func (db *rawDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	return db.db.BatchDelete(ctx, rowKeys)
}
