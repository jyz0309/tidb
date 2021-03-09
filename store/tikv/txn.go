// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
<<<<<<< HEAD
	"math/rand"
=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
<<<<<<< HEAD
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/util"
=======
	"github.com/pingcap/tidb/kv/memdb"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	"github.com/pingcap/tidb/util/execdetails"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

<<<<<<< HEAD
// SchemaAmender is used by pessimistic transactions to amend commit mutations for schema change during 2pc.
type SchemaAmender interface {
	// AmendTxn is the amend entry, new mutations will be generated based on input mutations using schema change info.
	// The returned results are mutations need to prewrite and mutations need to cleanup.
	AmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange, mutations CommitterMutations) (CommitterMutations, error)
}
=======
var (
	tikvTxnCmdHistogramWithCommit   = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblCommit)
	tikvTxnCmdHistogramWithRollback = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblRollback)
	tikvTxnCmdHistogramWithBatchGet = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblBatchGet)
	tikvTxnCmdHistogramWithGet      = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblGet)
	tikvTxnCmdHistogramWithLockKeys = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblLockKeys)
)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

// SchemaAmender is used by pessimistic transactions to amend commit mutations for schema change during 2pc.
type SchemaAmender interface {
	// AmendTxn is the amend entry, new mutations will be generated based on input mutations using schema change info.
	// The returned results are mutations need to prewrite and mutations need to cleanup.
	AmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange, mutations CommitterMutations) (*CommitterMutations, error)
}

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *KVStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
<<<<<<< HEAD
=======
	lockKeys  [][]byte
	lockedMap map[string]bool
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	mu        sync.Mutex // For thread-safe LockKeys function.
	setCnt    int64
	vars      *kv.Variables
	committer *twoPhaseCommitter
	lockedCnt int

	valid bool
<<<<<<< HEAD
=======
	dirty bool
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

	// txnInfoSchema is the infoSchema fetched at startTS.
	txnInfoSchema SchemaVer
	// SchemaAmender is used amend pessimistic txn commit mutations for schema change
	schemaAmender SchemaAmender
	// commitCallback is called after current transaction gets committed
	commitCallback func(info kv.TxnInfo, err error)
}

<<<<<<< HEAD
func newTiKVTxn(store *KVStore, txnScope string) (*tikvTxn, error) {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := store.getTimestampWithRetry(bo, txnScope)
=======
func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := store.getTimestampWithRetry(bo)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTiKVTxnWithStartTS(store, txnScope, startTS, store.nextReplicaReadSeed())
}

// newTiKVTxnWithStartTS creates a txn with startTS.
func newTiKVTxnWithStartTS(store *KVStore, txnScope string, startTS uint64, replicaReadSeed uint32) (*tikvTxn, error) {
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver, replicaReadSeed)
	newTiKVTxn := &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
<<<<<<< HEAD
=======
		lockedMap: make(map[string]bool),
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
		vars:      kv.DefaultVars,
	}
	newTiKVTxn.SetOption(kv.TxnScope, txnScope)
	return newTiKVTxn, nil
}

func newTiKVTxnWithExactStaleness(store *KVStore, txnScope string, prevSec uint64) (*tikvTxn, error) {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := store.getStalenessTimestamp(bo, txnScope, prevSec)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTiKVTxnWithStartTS(store, txnScope, startTS, store.nextReplicaReadSeed())
}

// SetSuccess is used to probe if kv variables are set or not. It is ONLY used in test cases.
var SetSuccess = false

// SetSuccess is used to probe if kv variables are set or not. It is ONLY used in test cases.
var SetSuccess = false

func (txn *tikvTxn) SetVars(vars *kv.Variables) {
	txn.vars = vars
	txn.snapshot.vars = vars
	failpoint.Inject("probeSetVars", func(val failpoint.Value) {
		if val.(bool) {
			SetSuccess = true
		}
	})
<<<<<<< HEAD
}

func (txn *tikvTxn) GetVars() *kv.Variables {
	return txn.vars
=======
}

func (txn *tikvTxn) GetVars() *kv.Variables {
	return txn.vars
}

// tikvTxnStagingBuffer is the staging buffer returned to tikvTxn user.
// Because tikvTxn needs to maintain dirty state when Flush staging data into txn.
type tikvTxnStagingBuffer struct {
	kv.MemBuffer
	txn *tikvTxn
}

func (buf *tikvTxnStagingBuffer) Flush() (int, error) {
	cnt, err := buf.MemBuffer.Flush()
	if cnt != 0 {
		buf.txn.dirty = true
	}
	return cnt, err
}

func (txn *tikvTxn) NewStagingBuffer() kv.MemBuffer {
	return &tikvTxnStagingBuffer{
		MemBuffer: txn.us.NewStagingBuffer(),
		txn:       txn,
	}
}

func (txn *tikvTxn) Flush() (int, error) {
	return txn.us.Flush()
}

func (txn *tikvTxn) Discard() {
	txn.us.Discard()
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// Get implements transaction interface.
func (txn *tikvTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	ret, err := txn.us.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (txn *tikvTxn) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.BatchGet", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return kv.NewBufferBatchGetter(txn.GetMemBuffer(), nil, txn.snapshot).BatchGet(ctx, keys)
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	txn.setCnt++
	return txn.us.GetMemBuffer().Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	return txn.us.IterReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	return txn.us.GetMemBuffer().Delete(k)
}

func (txn *tikvTxn) DeleteWithNeedLock(k kv.Key) error {
	txn.dirty = true
	return txn.us.DeleteWithNeedLock(k)
}

func (txn *tikvTxn) GetFlags(ctx context.Context, k kv.Key) memdb.KeyFlags {
	return txn.us.GetFlags(ctx, k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
	txn.snapshot.SetOption(opt, val)
	switch opt {
<<<<<<< HEAD
=======
	case kv.Priority:
		txn.snapshot.priority = kvPriorityToCommandPri(val.(int))
	case kv.NotFillCache:
		txn.snapshot.notFillCache = val.(bool)
	case kv.SyncLog:
		txn.snapshot.syncLog = val.(bool)
	case kv.KeyOnly:
		txn.snapshot.keyOnly = val.(bool)
	case kv.SnapshotTS:
		txn.snapshot.setSnapshotTS(val.(uint64))
	case kv.CheckExists:
		txn.us.SetOption(kv.CheckExists, val.(map[string]struct{}))
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	case kv.InfoSchema:
		txn.txnInfoSchema = val.(SchemaVer)
	case kv.SchemaAmender:
		txn.schemaAmender = val.(SchemaAmender)
	case kv.CommitHook:
		txn.commitCallback = val.(func(info kv.TxnInfo, err error))
	}
}

func (txn *tikvTxn) GetOption(opt kv.Option) interface{} {
	return txn.us.GetOption(opt)
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) IsPessimistic() bool {
	return txn.us.GetOption(kv.Pessimistic) != nil
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.Commit", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	defer trace.StartRegion(ctx, "CommitTxn").End()

	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	defer txn.close()

	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			kv.MockCommitErrorDisable()
			failpoint.Return(errors.New("mock commit error"))
		}
	})

	start := time.Now()
	defer func() { metrics.TxnCmdHistogramWithCommit.Observe(time.Since(start).Seconds()) }()

	// sessionID is used for log.
	var sessionID uint64
	val := ctx.Value(util.SessionID)
	if val != nil {
		sessionID = val.(uint64)
	}

	var err error
	// If the txn use pessimistic lock, committer is initialized.
	committer := txn.committer
	if committer == nil {
		committer, err = newTwoPhaseCommitter(txn, sessionID)
		if err != nil {
			return errors.Trace(err)
		}
		txn.committer = committer
	}
	defer func() {
		// For async commit transactions, the ttl manager will be closed in the asynchronous commit goroutine.
		if !committer.isAsyncCommit() {
			committer.ttlManager.close()
		}
	}()

	initRegion := trace.StartRegion(ctx, "InitKeys")
	err = committer.initKeysAndMutations()
	initRegion.End()
	if err != nil {
		return errors.Trace(err)
	}
	if committer.mutations.Len() == 0 {
		return nil
	}

	defer func() {
		ctxValue := ctx.Value(execdetails.CommitDetailCtxKey)
		if ctxValue != nil {
			commitDetail := ctxValue.(**execdetails.CommitDetails)
			if *commitDetail != nil {
				(*commitDetail).TxnRetry++
			} else {
				*commitDetail = committer.getDetail()
			}
		}
	}()
	// latches disabled
	// pessimistic transaction should also bypass latch.
	if txn.store.txnLatches == nil || txn.IsPessimistic() {
		err = committer.execute(ctx)
<<<<<<< HEAD
		if val == nil || sessionID > 0 {
=======
		if val == nil || connID > 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			txn.onCommitted(err)
		}
		logutil.Logger(ctx).Debug("[kv] txnLatches disabled, 2pc directly", zap.Error(err))
		return errors.Trace(err)
	}

	// latches enabled
	// for transactions which need to acquire latches
	start = time.Now()
	lock := txn.store.txnLatches.Lock(committer.startTS, committer.mutations.GetKeys())
	commitDetail := committer.getDetail()
	commitDetail.LocalLatchTime = time.Since(start)
	if commitDetail.LocalLatchTime > 0 {
		metrics.TiKVLocalLatchWaitTimeHistogram.Observe(commitDetail.LocalLatchTime.Seconds())
	}
	defer txn.store.txnLatches.UnLock(lock)
	if lock.IsStale() {
		return kv.ErrWriteConflictInTiDB.FastGenByArgs(txn.startTS)
	}
	err = committer.execute(ctx)
<<<<<<< HEAD
	if val == nil || sessionID > 0 {
=======
	if val == nil || connID > 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		txn.onCommitted(err)
	}
	if err == nil {
		lock.SetCommitTS(committer.commitTS)
	}
	logutil.Logger(ctx).Debug("[kv] txnLatches enabled while txn retryable", zap.Error(err))
	return errors.Trace(err)
}

func (txn *tikvTxn) close() {
	txn.valid = false
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	start := time.Now()
	// Clean up pessimistic lock.
	if txn.IsPessimistic() && txn.committer != nil {
		err := txn.rollbackPessimisticLocks()
		txn.committer.ttlManager.close()
		if err != nil {
			logutil.BgLogger().Error(err.Error())
		}
	}
	txn.close()
	logutil.BgLogger().Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	metrics.TxnCmdHistogramWithRollback.Observe(time.Since(start).Seconds())
	return nil
}

func (txn *tikvTxn) rollbackPessimisticLocks() error {
	if txn.lockedCnt == 0 {
		return nil
	}
<<<<<<< HEAD
	bo := NewBackofferWithVars(context.Background(), cleanupMaxBackoff, txn.vars)
	keys := txn.collectLockedKeys()
	return txn.committer.pessimisticRollbackMutations(bo, &PlainMutations{keys: keys})
}

func (txn *tikvTxn) collectLockedKeys() [][]byte {
	keys := make([][]byte, 0, txn.lockedCnt)
	buf := txn.GetMemBuffer()
	var err error
	for it := buf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		if it.Flags().HasLocked() {
			keys = append(keys, it.Key())
		}
	}
	return keys
=======
	return txn.committer.pessimisticRollbackMutations(NewBackofferWithVars(context.Background(), cleanupMaxBackoff, txn.vars), CommitterMutations{keys: txn.lockKeys})
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

func (txn *tikvTxn) onCommitted(err error) {
	if txn.commitCallback != nil {
<<<<<<< HEAD
		info := kv.TxnInfo{TxnScope: txn.GetUnionStore().GetOption(kv.TxnScope).(string), StartTS: txn.startTS, CommitTS: txn.commitTS}
=======
		info := kv.TxnInfo{StartTS: txn.startTS, CommitTS: txn.commitTS}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if err != nil {
			info.ErrMsg = err.Error()
		}
		txn.commitCallback(info, err)
	}
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	// Exclude keys that are already locked.
	var err error
	keys := make([][]byte, 0, len(keysInput))
	startTime := time.Now()
<<<<<<< HEAD
	txn.mu.Lock()
	defer txn.mu.Unlock()
	defer func() {
		metrics.TxnCmdHistogramWithLockKeys.Observe(time.Since(startTime).Seconds())
=======
	defer func() {
		tikvTxnCmdHistogramWithLockKeys.Observe(time.Since(startTime).Seconds())
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if err == nil {
			if lockCtx.PessimisticLockWaited != nil {
				if atomic.LoadInt32(lockCtx.PessimisticLockWaited) > 0 {
					timeWaited := time.Since(lockCtx.WaitStartTime)
					atomic.StoreInt64(lockCtx.LockKeysDuration, int64(timeWaited))
					metrics.TiKVPessimisticLockKeysDuration.Observe(timeWaited.Seconds())
				}
			}
		}
		if lockCtx.LockKeysCount != nil {
			*lockCtx.LockKeysCount += int32(len(keys))
		}
		if lockCtx.Stats != nil {
			lockCtx.Stats.TotalTime = time.Since(startTime)
			ctxValue := ctx.Value(execdetails.LockKeysDetailCtxKey)
			if ctxValue != nil {
				lockKeysDetail := ctxValue.(**execdetails.LockKeysDetails)
				*lockKeysDetail = lockCtx.Stats
			}
		}
	}()
<<<<<<< HEAD
	memBuf := txn.us.GetMemBuffer()
	for _, key := range keysInput {
		// The value of lockedMap is only used by pessimistic transactions.
		var valueExist, locked, checkKeyExists bool
		if flags, err := memBuf.GetFlags(key); err == nil {
			locked = flags.HasLocked()
			valueExist = flags.HasLockedValueExists()
			checkKeyExists = flags.HasNeedCheckExists()
		}
=======
	for _, key := range keysInput {
		// The value of lockedMap is only used by pessimistic transactions.
		valueExist, locked := txn.lockedMap[string(key)]
		_, checkKeyExists := lockCtx.CheckKeyExists[string(key)]
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if !locked {
			keys = append(keys, key)
		} else if txn.IsPessimistic() {
			if checkKeyExists && valueExist {
<<<<<<< HEAD
				return txn.committer.extractKeyExistsErr(key)
=======
				existErrInfo := txn.us.GetKeyExistErrInfo(key)
				if existErrInfo == nil {
					logutil.Logger(ctx).Error("key exist error not found", zap.Uint64("connID", txn.committer.connID),
						zap.Stringer("key", key))
					return errors.Errorf("conn %d, existErr for key:%s should not be nil", txn.committer.connID, key)
				}
				return existErrInfo.Err()
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			}
		}
		if lockCtx.ReturnValues && locked {
			// An already locked key can not return values, we add an entry to let the caller get the value
			// in other ways.
			lockCtx.Values[string(key)] = kv.ReturnedValue{AlreadyLocked: true}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	keys = deduplicateKeys(keys)
	if txn.IsPessimistic() && lockCtx.ForUpdateTS > 0 {
		if txn.committer == nil {
			// sessionID is used for log.
			var sessionID uint64
			var err error
			val := ctx.Value(util.SessionID)
			if val != nil {
				sessionID = val.(uint64)
			}
			txn.committer, err = newTwoPhaseCommitter(txn, sessionID)
			if err != nil {
				return err
			}
		}
		var assignedPrimaryKey bool
		if txn.committer.primaryKey == nil {
			txn.committer.primaryKey = keys[0]
			assignedPrimaryKey = true
		}

		lockCtx.Stats = &execdetails.LockKeysDetails{
			LockKeys: int32(len(keys)),
		}
		bo := NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, txn.vars)
		txn.committer.forUpdateTS = lockCtx.ForUpdateTS
		// If the number of keys greater than 1, it can be on different region,
		// concurrently execute on multiple regions may lead to deadlock.
<<<<<<< HEAD
		txn.committer.isFirstLock = txn.lockedCnt == 0 && len(keys) == 1
		err = txn.committer.pessimisticLockMutations(bo, lockCtx, &PlainMutations{keys: keys})
=======
		txn.committer.isFirstLock = len(txn.lockKeys) == 0 && len(keys) == 1
		err = txn.committer.pessimisticLockMutations(bo, lockCtx, CommitterMutations{keys: keys})
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if bo.totalSleep > 0 {
			atomic.AddInt64(&lockCtx.Stats.BackoffTime, int64(bo.totalSleep)*int64(time.Millisecond))
			lockCtx.Stats.Mu.Lock()
			lockCtx.Stats.Mu.BackoffTypes = append(lockCtx.Stats.Mu.BackoffTypes, bo.types...)
			lockCtx.Stats.Mu.Unlock()
		}
		if lockCtx.Killed != nil {
			// If the kill signal is received during waiting for pessimisticLock,
			// pessimisticLockKeys would handle the error but it doesn't reset the flag.
			// We need to reset the killed flag here.
			atomic.CompareAndSwapUint32(lockCtx.Killed, 1, 0)
		}
		if err != nil {
			for _, key := range keys {
				if txn.us.HasPresumeKeyNotExists(key) {
					txn.us.UnmarkPresumeKeyNotExists(key)
				}
			}
			keyMayBeLocked := terror.ErrorNotEqual(kv.ErrWriteConflict, err) && terror.ErrorNotEqual(kv.ErrKeyExists, err)
			// If there is only 1 key and lock fails, no need to do pessimistic rollback.
			if len(keys) > 1 || keyMayBeLocked {
				wg := txn.asyncPessimisticRollback(ctx, keys)
				if dl, ok := errors.Cause(err).(*ErrDeadlock); ok && hashInKeys(dl.DeadlockKeyHash, keys) {
					dl.IsRetryable = true
					// Wait for the pessimistic rollback to finish before we retry the statement.
					wg.Wait()
					// Sleep a little, wait for the other transaction that blocked by this transaction to acquire the lock.
					time.Sleep(time.Millisecond * 5)
					failpoint.Inject("SingleStmtDeadLockRetrySleep", func() {
						time.Sleep(300 * time.Millisecond)
					})
				}
			}
			if assignedPrimaryKey {
				// unset the primary key if we assigned primary key when failed to lock it.
				txn.committer.primaryKey = nil
			}
			return err
		}
		if assignedPrimaryKey {
			txn.committer.ttlManager.run(txn.committer, lockCtx)
		}
	}
<<<<<<< HEAD
	for _, key := range keys {
		valExists := kv.SetKeyLockedValueExists
		// PointGet and BatchPointGet will return value in pessimistic lock response, the value may not exist.
		// For other lock modes, the locked key values always exist.
		if lockCtx.ReturnValues {
			val, _ := lockCtx.Values[string(key)]
			if len(val.Value) == 0 {
				valExists = kv.SetKeyLockedValueNotExists
			}
		}
		memBuf.UpdateFlags(key, kv.SetKeyLocked, kv.DelNeedCheckExists, valExists)
	}
	txn.lockedCnt += len(keys)
=======
	txn.lockKeys = append(txn.lockKeys, keys...)
	for _, key := range keys {
		// PointGet and BatchPointGet will return value in pessimistic lock response, the value may not exists.
		// For other lock modes, the locked key values always exist.
		if lockCtx.ReturnValues {
			val, _ := lockCtx.Values[string(key)]
			valExists := len(val.Value) > 0
			txn.lockedMap[string(key)] = valExists
		} else {
			txn.lockedMap[string(key)] = true
		}
	}
	txn.dirty = true
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	return nil
}

// deduplicateKeys deduplicate the keys, it use sort instead of map to avoid memory allocation.
func deduplicateKeys(keys [][]byte) [][]byte {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	deduped := keys[:1]
	for i := 1; i < len(keys); i++ {
		if !bytes.Equal(deduped[len(deduped)-1], keys[i]) {
			deduped = append(deduped, keys[i])
		}
	}
	return deduped
}

func (txn *tikvTxn) asyncPessimisticRollback(ctx context.Context, keys [][]byte) *sync.WaitGroup {
	// Clone a new committer for execute in background.
	committer := &twoPhaseCommitter{
		store:       txn.committer.store,
		sessionID:   txn.committer.sessionID,
		startTS:     txn.committer.startTS,
		forUpdateTS: txn.committer.forUpdateTS,
		primaryKey:  txn.committer.primaryKey,
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
<<<<<<< HEAD
		failpoint.Inject("beforeAsyncPessimisticRollback", func(val failpoint.Value) {
			if s, ok := val.(string); ok {
				if s == "skip" {
					logutil.Logger(ctx).Info("[failpoint] injected skip async pessimistic rollback",
						zap.Uint64("txnStartTS", txn.startTS))
					wg.Done()
					failpoint.Return()
				} else if s == "delay" {
					duration := time.Duration(rand.Int63n(int64(time.Second) * 2))
					logutil.Logger(ctx).Info("[failpoint] injected delay before async pessimistic rollback",
						zap.Uint64("txnStartTS", txn.startTS), zap.Duration("duration", duration))
					time.Sleep(duration)
				}
			}
		})

		err := committer.pessimisticRollbackMutations(NewBackofferWithVars(ctx, pessimisticRollbackMaxBackoff, txn.vars), &PlainMutations{keys: keys})
=======
		failpoint.Inject("AsyncRollBackSleep", func(sleepTimeMS failpoint.Value) {
			if tmp, ok := sleepTimeMS.(int); ok {
				if tmp < 10000 {
					logutil.Logger(ctx).Info("[failpoint] sleep before trigger asyncPessimisticRollback", zap.Int("sleep ms", tmp))
					time.Sleep(time.Duration(tmp) * time.Millisecond)
				} else {
					logutil.Logger(ctx).Info("[failpoint] async rollback return")
					failpoint.Return()
				}
			}
		})
		err := committer.pessimisticRollbackMutations(NewBackofferWithVars(ctx, pessimisticRollbackMaxBackoff, txn.vars), CommitterMutations{keys: keys})
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if err != nil {
			logutil.Logger(ctx).Warn("[kv] pessimisticRollback failed.", zap.Error(err))
		}
		wg.Done()
	}()
	return wg
}

func hashInKeys(deadlockKeyHash uint64, keys [][]byte) bool {
	for _, key := range keys {
		if farm.Fingerprint64(key) == deadlockKeyHash {
			return true
		}
	}
	return false
}

func (txn *tikvTxn) IsReadOnly() bool {
	return !txn.us.GetMemBuffer().Dirty()
}

func (txn *tikvTxn) StartTS() uint64 {
	return txn.startTS
}

func (txn *tikvTxn) Valid() bool {
	return txn.valid
}

func (txn *tikvTxn) Len() int {
	return txn.us.GetMemBuffer().Len()
}

func (txn *tikvTxn) Size() int {
	return txn.us.GetMemBuffer().Size()
}

func (txn *tikvTxn) Reset() {
	txn.us.GetMemBuffer().Reset()
}

func (txn *tikvTxn) GetUnionStore() kv.UnionStore {
	return txn.us
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return txn.us.GetMemBuffer()
}

func (txn *tikvTxn) GetMemBufferSnapshot() kv.MemBuffer {
	panic("unsupported operation")
}

func (txn *tikvTxn) GetSnapshot() kv.Snapshot {
	return txn.snapshot
}

func (txn *tikvTxn) ResetStmtKeyExistErrs() {
	txn.us.ResetStmtKeyExistErrs()
}

func (txn *tikvTxn) MergeStmtKeyExistErrs() {
	txn.us.MergeStmtKeyExistErrs()
}
