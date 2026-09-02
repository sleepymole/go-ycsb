/*
 *
 * core_test.go
 * workload
 *
 * Created by lintao on 2023/7/20 10:49
 * Copyright © 2020-2023 LINTAO. All rights reserved.
 *
 */

package workload

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

func Test_core_buildKeyName(t *testing.T) {

	type args struct {
		keyNum int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "buildKeyName",
			args: args{keyNum: 227},
			want: "user6284890712318570100",
		},
		{
			name: "buildKeyName1",
			args: args{keyNum: 154},
			want: "user6284898408899967577",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := core{p: properties.NewProperties()}
			if got := c.buildKeyName(tt.args.keyNum); got != tt.want {
				t.Errorf("buildKeyName() = %v, want %v", got, tt.want)
			}
		})
	}
}

type readModifyWriteDB struct {
	readErr      error
	updateErr    error
	updateCalled bool
}

func (*readModifyWriteDB) Close() error { return nil }
func (*readModifyWriteDB) InitThread(ctx context.Context, _, _ int) context.Context {
	return ctx
}
func (*readModifyWriteDB) CleanupThread(context.Context) {}
func (db *readModifyWriteDB) Read(context.Context, string, string, []string) (map[string][]byte, error) {
	return nil, db.readErr
}
func (*readModifyWriteDB) Scan(context.Context, string, string, int, []string) ([]map[string][]byte, error) {
	return nil, nil
}
func (db *readModifyWriteDB) Update(context.Context, string, string, map[string][]byte) error {
	db.updateCalled = true
	return db.updateErr
}
func (*readModifyWriteDB) Insert(context.Context, string, string, map[string][]byte) error {
	return nil
}
func (*readModifyWriteDB) Delete(context.Context, string, string) error { return nil }

var _ ycsb.DB = (*readModifyWriteDB)(nil)

func TestReadModifyWriteMetricsResult(t *testing.T) {
	readErr := errors.New("read failed")
	updateErr := errors.New("update failed")
	tests := []struct {
		name         string
		db           *readModifyWriteDB
		wantErr      error
		wantResult   string
		updateCalled bool
	}{
		{name: "success", db: &readModifyWriteDB{}, wantResult: "ok", updateCalled: true},
		{name: "read error", db: &readModifyWriteDB{readErr: readErr}, wantErr: readErr, wantResult: "error"},
		{name: "update error", db: &readModifyWriteDB{updateErr: updateErr}, wantErr: updateErr, wantResult: "error", updateCalled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := properties.NewProperties()
			p.Set(prop.RecordCount, "1")
			p.Set(prop.MetricsAddr, "127.0.0.1:0")
			workload, err := (coreCreator{}).Create(p)
			if err != nil {
				t.Fatalf("create workload: %v", err)
			}
			c := workload.(*core)
			ctx := c.InitThread(context.Background(), 0, 1)
			measurement.InitMeasure(p)

			err = c.doTransactionReadModifyWrite(ctx, tt.db, ctx.Value(stateKey).(*coreState))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if tt.db.updateCalled != tt.updateCalled {
				t.Fatalf("update called = %v, want %v", tt.db.updateCalled, tt.updateCalled)
			}

			req := httptest.NewRequest("GET", "/metrics", nil)
			resp := httptest.NewRecorder()
			measurement.MetricsHandler().ServeHTTP(resp, req)
			metric := `go_ycsb_operations_total{operation="READ_MODIFY_WRITE",result="` + tt.wantResult + `"} 1`
			if !strings.Contains(resp.Body.String(), metric) {
				t.Fatalf("metrics output does not contain %q:\n%s", metric, resp.Body.String())
			}
		})
	}
}
