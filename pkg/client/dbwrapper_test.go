// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package client

import (
	"context"
	"testing"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type wrapperReadModifyWriteDB struct {
	ycsb.DB
	called     bool
	readValues map[string][]byte
	readValue  []byte
	values     map[string][]byte
}

func (db *wrapperReadModifyWriteDB) ReadForUpdate(_ context.Context, _ string, _ string, _ []string) (map[string][]byte, []byte, error) {
	return db.readValues, db.readValue, nil
}

func (db *wrapperReadModifyWriteDB) UpdateWithRead(_ context.Context, _ string, _ string, readValues map[string][]byte, readValue []byte, values map[string][]byte) error {
	db.called = true
	db.readValues = readValues
	db.readValue = readValue
	db.values = values
	return nil
}

var _ ycsb.ReadModifyWriteDB = (*wrapperReadModifyWriteDB)(nil)

func TestDbWrapperReadForUpdate(t *testing.T) {
	measurement.InitMeasure(properties.NewProperties())
	db := &wrapperReadModifyWriteDB{
		readValues: map[string][]byte{"field0": []byte("old")},
		readValue:  []byte("encoded-old"),
	}

	values, readValue, err := (DbWrapper{DB: db}).ReadForUpdate(context.Background(), "usertable", "key", []string{"field0"})
	if err != nil {
		t.Fatalf("ReadForUpdate: %v", err)
	}
	if string(values["field0"]) != "old" || string(readValue) != "encoded-old" {
		t.Fatalf("ReadForUpdate = %#v, %q", values, readValue)
	}
}

func TestDbWrapperUpdateWithRead(t *testing.T) {
	measurement.InitMeasure(properties.NewProperties())
	readValues := map[string][]byte{"field0": []byte("old")}
	readValue := []byte("encoded-old")
	values := map[string][]byte{"field1": []byte("new")}
	db := &wrapperReadModifyWriteDB{}

	if err := (DbWrapper{DB: db}).UpdateWithRead(context.Background(), "usertable", "key", readValues, readValue, values); err != nil {
		t.Fatalf("UpdateWithRead: %v", err)
	}
	if !db.called {
		t.Fatal("underlying UpdateWithRead was not called")
	}
	if string(db.readValues["field0"]) != "old" {
		t.Fatalf("read values = %#v", db.readValues)
	}
	if string(db.readValue) != string(readValue) {
		t.Fatalf("read value = %q", db.readValue)
	}
	if string(db.values["field1"]) != "new" {
		t.Fatalf("values = %#v", db.values)
	}
}
