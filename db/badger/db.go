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

package badger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	badgerDir                     = "badger.dir"
	badgerValueDir                = "badger.valuedir"
	badgerSyncWrites              = "badger.sync_writes"
	badgerNumVersionsToKeep       = "badger.num_versions_to_keep"
	badgerMaxTableSize            = "badger.max_table_size"
	badgerLevelSizeMultiplier     = "badger.level_size_multiplier"
	badgerMaxLevels               = "badger.max_levels"
	badgerValueThreshold          = "badger.value_threshold"
	badgerNumMemtables            = "badger.num_memtables"
	badgerNumLevelZeroTables      = "badger.num_level0_tables"
	badgerNumLevelZeroTablesStall = "badger.num_level0_tables_stall"
	badgerLevelOneSize            = "badger.level_one_size"
	badgerValueLogFileSize        = "badger.value_log_file_size"
	badgerValueLogMaxEntries      = "badger.value_log_max_entries"
	badgerNumCompactors           = "badger.num_compactors"
	badgerDoNotCompact            = "badger.do_not_compact"
	badgerTableLoadingMode        = "badger.table_loading_mode"
	badgerValueLogLoadingMode     = "badger.value_log_loading_mode"
	badgerValueLogGCInterval      = "badger.value_log_gc_interval"
	badgerValueLogGCDiscardRatio  = "badger.value_log_gc_discard_ratio"
	// TODO: add more configurations
)

type badgerCreator struct {
}

type badgerDB struct {
	p *properties.Properties

	db *badger.DB

	r       *util.RowCodec
	bufPool *util.BufPool

	gcCancel context.CancelFunc
	gcDone   chan struct{}
}

type contextKey string

const stateKey = contextKey("badgerDB")

type badgerState struct {
}

func (c badgerCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(opts.Dir)
		os.RemoveAll(opts.ValueDir)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	out := &badgerDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}
	if err := out.startValueLogGC(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return out, nil
}

func getOptions(p *properties.Properties) badger.Options {
	opts := badger.DefaultOptions(p.GetString(badgerDir, "/tmp/badger"))
	opts.ValueDir = p.GetString(badgerValueDir, opts.Dir)

	opts.SyncWrites = p.GetBool(badgerSyncWrites, false)
	opts.NumVersionsToKeep = p.GetInt(badgerNumVersionsToKeep, 1)
	opts.BaseTableSize = p.GetInt64(badgerMaxTableSize, opts.BaseTableSize)
	opts.LevelSizeMultiplier = p.GetInt(badgerLevelSizeMultiplier, 10)
	opts.MaxLevels = p.GetInt(badgerMaxLevels, 7)
	opts.ValueThreshold = p.GetInt64(badgerValueThreshold, opts.ValueThreshold)
	opts.NumMemtables = p.GetInt(badgerNumMemtables, 5)
	opts.NumLevelZeroTables = p.GetInt(badgerNumLevelZeroTables, 5)
	opts.NumLevelZeroTablesStall = p.GetInt(badgerNumLevelZeroTablesStall, 15)
	opts.BaseLevelSize = p.GetInt64(badgerLevelOneSize, opts.BaseLevelSize)
	opts.ValueLogFileSize = p.GetInt64(badgerValueLogFileSize, 1<<30)
	opts.ValueLogMaxEntries = uint32(p.GetUint64(badgerValueLogMaxEntries, 1000000))
	opts.NumCompactors = p.GetInt(badgerNumCompactors, 3)
	return opts
}

func (db *badgerDB) Close() error {
	if db.gcCancel != nil {
		db.gcCancel()
	}
	if db.gcDone != nil {
		<-db.gcDone
	}
	return db.db.Close()
}

func (db *badgerDB) startValueLogGC() error {
	interval := db.p.GetDuration(badgerValueLogGCInterval, 0)
	if interval == 0 {
		return nil
	}
	if interval < 0 {
		return fmt.Errorf("%s must be >= 0", badgerValueLogGCInterval)
	}

	discardRatio := db.p.GetFloat64(badgerValueLogGCDiscardRatio, 0.5)
	if discardRatio <= 0 || discardRatio >= 1 {
		return fmt.Errorf("%s must be in (0,1)", badgerValueLogGCDiscardRatio)
	}

	ctx, cancel := context.WithCancel(context.Background())
	db.gcCancel = cancel
	db.gcDone = make(chan struct{})

	go func() {
		defer close(db.gcDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runValueLogGC(db.db, discardRatio)
			}
		}
	}()

	return nil
}

func runValueLogGC(db *badger.DB, discardRatio float64) {
	// Keep semantics close to badger/cmd/write_bench.go: invoke one GC attempt per tick.
	err := db.RunValueLogGC(discardRatio)
	if err == nil || errors.Is(err, badger.ErrNoRewrite) {
		return
	}
}

func (db *badgerDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *badgerDB) CleanupThread(_ context.Context) {
}

func (db *badgerDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *badgerDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			m, err = db.r.Decode(v, fields)
			return err
		})
	})

	return m, err
}

func (db *badgerDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	err := db.db.View(func(txn *badger.Txn) error {
		rowStartKey := db.getRowKey(table, startKey)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		i := 0
		for it.Seek(rowStartKey); it.Valid() && i < count; it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			m, err := db.r.Decode(value, fields)
			if err != nil {
				return err
			}

			res[i] = m
			i++
		}

		return nil
	})

	return res, err
}

func (db *badgerDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}

		var data map[string][]byte
		if err := item.Value(func(v []byte) error {
			data, err = db.r.Decode(v, nil)
			return err
		}); err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, buf)
	})
	return err
}

func (db *badgerDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err := db.r.Encode(buf, values)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, buf)
	})

	return err
}

func (db *badgerDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(db.getRowKey(table, key))
	})

	return err
}

func init() {
	ycsb.RegisterDBCreator("badger", badgerCreator{})
}
