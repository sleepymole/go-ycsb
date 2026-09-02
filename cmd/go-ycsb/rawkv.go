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
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	clientrawkv "github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/pd/client/clients/router"
)

const regionScanPageSize = 1024

type rawKVClient interface {
	Close() error
	Get(context.Context, []byte, ...clientrawkv.RawOption) ([]byte, error)
	Put(context.Context, []byte, []byte, ...clientrawkv.RawOption) error
	Scan(context.Context, []byte, []byte, int, ...clientrawkv.RawOption) ([][]byte, [][]byte, error)
	Delete(context.Context, []byte, ...clientrawkv.RawOption) error
	DeleteRange(context.Context, []byte, []byte, ...clientrawkv.RawOption) error
	Checksum(context.Context, []byte, []byte, ...clientrawkv.RawOption) (clientrawkv.RawChecksum, error)
	ScanRegions(context.Context, []byte, []byte, int) ([]*router.Region, error)
}

type tikvRawKVClient struct {
	*clientrawkv.Client
}

func (c *tikvRawKVClient) ScanRegions(ctx context.Context, startKey, endKey []byte, limit int) ([]*router.Region, error) {
	return c.GetPDClient().ScanRegions(ctx, startKey, endKey, limit)
}

type rawKVClientFactory func(context.Context, string, string) (rawKVClient, error)

type rawKVCommandOptions struct {
	pdAddr     string
	apiVersion string
	openClient rawKVClientFactory
}

func newRawKVCommand() *cobra.Command {
	return newRawKVCommandWithFactory(openRawKVClient)
}

func newRawKVCommandWithFactory(factory rawKVClientFactory) *cobra.Command {
	opts := &rawKVCommandOptions{openClient: factory}
	cmd := &cobra.Command{
		Use:   "rawkv",
		Short: "Operate on TiKV RawKV keys directly",
	}
	cmd.PersistentFlags().StringVar(&opts.pdAddr, "pd", "127.0.0.1:2379", "Comma-separated PD addresses")
	cmd.PersistentFlags().StringVar(&opts.apiVersion, "api-version", "V1", "TiKV RawKV API version (V1, V1TTL, or V2)")
	cmd.AddCommand(
		newRawKVGetCommand(opts),
		newRawKVPutCommand(opts),
		newRawKVScanCommand(opts),
		newRawKVDeleteCommand(opts),
		newRawKVDeleteRangeCommand(opts),
		newRawKVChecksumCommand(opts),
	)
	return cmd
}

func openRawKVClient(ctx context.Context, pdAddr, apiVersion string) (rawKVClient, error) {
	var pdAddrs []string
	for _, addr := range strings.Split(pdAddr, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			pdAddrs = append(pdAddrs, addr)
		}
	}
	if len(pdAddrs) == 0 {
		return nil, fmt.Errorf("at least one PD address is required")
	}

	version, ok := kvrpcpb.APIVersion_value[strings.ToUpper(apiVersion)]
	if !ok {
		return nil, fmt.Errorf("invalid TiKV API version %q", apiVersion)
	}
	client, err := clientrawkv.NewClientWithOpts(ctx, pdAddrs,
		clientrawkv.WithAPIVersion(kvrpcpb.APIVersion(version)))
	if err != nil {
		return nil, err
	}
	return &tikvRawKVClient{Client: client}, nil
}

func (o *rawKVCommandOptions) withClient(ctx context.Context, fn func(rawKVClient) error) (err error) {
	client, err := o.openClient(ctx, o.pdAddr, o.apiVersion)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := client.Close(); err == nil {
			err = closeErr
		}
	}()
	return fn(client)
}

func newRawKVGetCommand(opts *rawKVCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "get key",
		Short: "Get a raw key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return opts.withClient(cmd.Context(), func(client rawKVClient) error {
				value, err := client.Get(cmd.Context(), []byte(args[0]))
				if err != nil {
					return err
				}
				if value == nil {
					fmt.Fprintln(cmd.OutOrStdout(), "not found")
					return nil
				}
				fmt.Fprintln(cmd.OutOrStdout(), strconv.Quote(string(value)))
				return nil
			})
		},
	}
}

func newRawKVPutCommand(opts *rawKVCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "put key value",
		Short: "Put a raw key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return opts.withClient(cmd.Context(), func(client rawKVClient) error {
				if err := client.Put(cmd.Context(), []byte(args[0]), []byte(args[1])); err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), "OK")
				return nil
			})
		},
	}
}

func newRawKVScanCommand(opts *rawKVCommandOptions) *cobra.Command {
	var limit int
	var keysOnly bool
	cmd := &cobra.Command{
		Use:   "scan start-key [end-key]",
		Short: "Scan raw key-value pairs in [start-key, end-key)",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if limit <= 0 || limit > clientrawkv.MaxRawKVScanLimit {
				return fmt.Errorf("limit must be between 1 and %d", clientrawkv.MaxRawKVScanLimit)
			}
			startKey, endKey := rawKVArgsRange(args)
			if err := validateRawKVRange(startKey, endKey); err != nil {
				return err
			}
			return opts.withClient(cmd.Context(), func(client rawKVClient) error {
				var scanOptions []clientrawkv.RawOption
				if keysOnly {
					scanOptions = append(scanOptions, clientrawkv.ScanKeyOnly())
				}
				keys, values, err := client.Scan(cmd.Context(), startKey, endKey, limit, scanOptions...)
				if err != nil {
					return err
				}
				for i, key := range keys {
					fmt.Fprint(cmd.OutOrStdout(), strconv.Quote(string(key)))
					if !keysOnly {
						fmt.Fprint(cmd.OutOrStdout(), "\t", strconv.Quote(string(values[i])))
					}
					fmt.Fprintln(cmd.OutOrStdout())
				}
				return nil
			})
		},
	}
	cmd.Flags().IntVarP(&limit, "limit", "l", 100, "Maximum number of pairs to return")
	cmd.Flags().BoolVar(&keysOnly, "keys-only", false, "Return keys without values")
	return cmd
}

func newRawKVDeleteCommand(opts *rawKVCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "delete key",
		Short: "Delete a raw key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return opts.withClient(cmd.Context(), func(client rawKVClient) error {
				if err := client.Delete(cmd.Context(), []byte(args[0])); err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), "OK")
				return nil
			})
		},
	}
}

func newRawKVDeleteRangeCommand(opts *rawKVCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:     "delete_range start-key end-key",
		Aliases: []string{"delete-range"},
		Short:   "Delete raw keys in [start-key, end-key)",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			startKey, endKey := []byte(args[0]), []byte(args[1])
			if err := validateRawKVRange(startKey, endKey); err != nil {
				return err
			}
			return opts.withClient(cmd.Context(), func(client rawKVClient) error {
				if err := client.DeleteRange(cmd.Context(), startKey, endKey); err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), "OK")
				return nil
			})
		},
	}
}

func newRawKVChecksumCommand(opts *rawKVCommandOptions) *cobra.Command {
	concurrency := runtime.GOMAXPROCS(0)
	cmd := &cobra.Command{
		Use:   "checksum start-key [end-key]",
		Short: "Checksum raw keys in [start-key, end-key) concurrently by region",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if concurrency <= 0 {
				return fmt.Errorf("concurrency must be greater than zero")
			}
			startKey, endKey := rawKVArgsRange(args)
			if err := validateRawKVRange(startKey, endKey); err != nil {
				return err
			}
			return opts.withClient(cmd.Context(), func(client rawKVClient) error {
				checksum, regions, err := checksumRawKV(cmd.Context(), client, client, startKey, endKey, concurrency)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "crc64_xor=%d total_kvs=%d total_bytes=%d regions=%d\n",
					checksum.Crc64Xor, checksum.TotalKvs, checksum.TotalBytes, regions)
				return nil
			})
		},
	}
	cmd.Flags().IntVarP(&concurrency, "concurrency", "c", concurrency, "Maximum number of concurrent region tasks")
	return cmd
}

func rawKVArgsRange(args []string) ([]byte, []byte) {
	startKey := []byte(args[0])
	if len(args) == 1 {
		return startKey, nil
	}
	return startKey, []byte(args[1])
}

func validateRawKVRange(startKey, endKey []byte) error {
	if len(endKey) > 0 && bytes.Compare(startKey, endKey) >= 0 {
		return fmt.Errorf("start key must be less than end key")
	}
	return nil
}

type rawKVRegionScanner interface {
	ScanRegions(context.Context, []byte, []byte, int) ([]*router.Region, error)
}

type rawKVChecksummer interface {
	Checksum(context.Context, []byte, []byte, ...clientrawkv.RawOption) (clientrawkv.RawChecksum, error)
}

type rawKVRange struct {
	startKey []byte
	endKey   []byte
}

func walkRawKVRangeByRegions(
	ctx context.Context,
	scanner rawKVRegionScanner,
	startKey, endKey []byte,
	visit func(rawKVRange) error,
) (int, error) {
	if err := validateRawKVRange(startKey, endKey); err != nil {
		return 0, err
	}

	cursor := cloneRawKVKey(startKey)
	regionCount := 0
	for {
		regions, err := scanner.ScanRegions(ctx, cursor, endKey, regionScanPageSize)
		if err != nil {
			return regionCount, err
		}
		if len(regions) == 0 {
			return regionCount, fmt.Errorf("PD returned no region containing key %q", cursor)
		}

		progressed := false
		for _, region := range regions {
			if region == nil || region.Meta == nil {
				return regionCount, fmt.Errorf("PD returned a region without metadata")
			}
			regionStart, regionEnd := region.Meta.StartKey, region.Meta.EndKey
			if bytes.Compare(regionStart, cursor) > 0 {
				return regionCount, fmt.Errorf("region gap before key %q", cursor)
			}
			if len(regionEnd) > 0 && bytes.Compare(regionEnd, cursor) <= 0 {
				continue
			}

			taskEnd := regionEnd
			if len(endKey) > 0 && (len(taskEnd) == 0 || bytes.Compare(taskEnd, endKey) > 0) {
				taskEnd = endKey
			}
			keyRange := rawKVRange{
				startKey: cloneRawKVKey(cursor),
				endKey:   cloneRawKVKey(taskEnd),
			}
			if err := visit(keyRange); err != nil {
				return regionCount, err
			}
			regionCount++
			cursor = cloneRawKVKey(taskEnd)
			progressed = true

			if len(cursor) == 0 || (len(endKey) > 0 && bytes.Compare(cursor, endKey) >= 0) {
				return regionCount, nil
			}
		}
		if !progressed {
			return regionCount, fmt.Errorf("region scan made no progress at key %q", cursor)
		}
	}
}

func cloneRawKVKey(key []byte) []byte {
	return append([]byte(nil), key...)
}

func checksumRawKV(
	ctx context.Context,
	checksummer rawKVChecksummer,
	scanner rawKVRegionScanner,
	startKey, endKey []byte,
	concurrency int,
) (clientrawkv.RawChecksum, int, error) {
	if concurrency <= 0 {
		return clientrawkv.RawChecksum{}, 0, fmt.Errorf("concurrency must be greater than zero")
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan rawKVRange)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var combined clientrawkv.RawChecksum
	var firstErr error

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-workerCtx.Done():
				return
			case keyRange, ok := <-jobs:
				if !ok {
					return
				}
				checksum, err := checksummer.Checksum(workerCtx, keyRange.startKey, keyRange.endKey)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
					cancel()
				} else if err == nil {
					combined.Crc64Xor ^= checksum.Crc64Xor
					combined.TotalKvs += checksum.TotalKvs
					combined.TotalBytes += checksum.TotalBytes
				}
				mu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go worker()
	}

	regionCount, scanErr := walkRawKVRangeByRegions(workerCtx, scanner, startKey, endKey, func(keyRange rawKVRange) error {
		select {
		case jobs <- keyRange:
			return nil
		case <-workerCtx.Done():
			return workerCtx.Err()
		}
	})
	if scanErr != nil {
		cancel()
	}
	close(jobs)
	wg.Wait()

	mu.Lock()
	workerErr := firstErr
	mu.Unlock()
	if workerErr != nil && (scanErr == nil || errors.Is(scanErr, context.Canceled)) {
		return clientrawkv.RawChecksum{}, regionCount, workerErr
	}
	if err := ctx.Err(); err != nil {
		return clientrawkv.RawChecksum{}, regionCount, err
	}
	if scanErr != nil {
		return clientrawkv.RawChecksum{}, regionCount, scanErr
	}
	if workerErr != nil {
		return clientrawkv.RawChecksum{}, regionCount, workerErr
	}
	return combined, regionCount, nil
}
