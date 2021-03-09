// Copyright 2018 PingCAP, Inc.

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

package session

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"strings"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

// TxnState wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a txnFuture before StartTS() is really need.
type TxnState struct {
	// States of a TxnState should be one of the followings:
	// Invalid: kv.Transaction == nil && txnFuture == nil
	// Pending: kv.Transaction == nil && txnFuture != nil
	// Valid:	kv.Transaction != nil && txnFuture == nil
	kv.Transaction
	txnFuture *txnFuture

<<<<<<< HEAD
	initCnt       int
	stagingHandle kv.StagingHandle
	mutations     map[int64]*binlog.TableMutation
=======
	stmtBuf      kv.MemBuffer
	mutations    map[int64]*binlog.TableMutation
	dirtyTableOP []dirtyTableOperation

	// If doNotCommit is not nil, Commit() will not commit the transaction.
	// doNotCommit flag may be set when StmtCommit fail.
	doNotCommit error
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

func (st *TxnState) init() {
	st.mutations = make(map[int64]*binlog.TableMutation)
}

func (st *TxnState) initStmtBuf() {
<<<<<<< HEAD
	if st.Transaction == nil {
		return
	}
	buf := st.Transaction.GetMemBuffer()
	st.initCnt = buf.Len()
	st.stagingHandle = buf.Staging()
}

// countHint is estimated count of mutations.
func (st *TxnState) countHint() int {
	if st.stagingHandle == kv.InvalidStagingHandle {
		return 0
	}
	return st.Transaction.GetMemBuffer().Len() - st.initCnt
}

func (st *TxnState) flushStmtBuf() {
	if st.stagingHandle == kv.InvalidStagingHandle {
		return
	}
	buf := st.Transaction.GetMemBuffer()
	buf.Release(st.stagingHandle)
	st.initCnt = buf.Len()
}

func (st *TxnState) cleanupStmtBuf() {
	if st.stagingHandle == kv.InvalidStagingHandle {
		return
	}
	buf := st.Transaction.GetMemBuffer()
	buf.Cleanup(st.stagingHandle)
	st.initCnt = buf.Len()
=======
	if st.stmtBuf == nil {
		st.stmtBuf = st.Transaction.NewStagingBuffer()
	}
}

func (st *TxnState) stmtBufLen() int {
	if st.stmtBuf == nil {
		return 0
	}
	return st.stmtBuf.Len()
}

func (st *TxnState) stmtBufSize() int {
	if st.stmtBuf == nil {
		return 0
	}
	return st.stmtBuf.Size()
}

func (st *TxnState) stmtBufGet(ctx context.Context, k kv.Key) ([]byte, error) {
	if st.stmtBuf == nil {
		return nil, kv.ErrNotExist
	}
	return st.stmtBuf.Get(ctx, k)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// Size implements the MemBuffer interface.
func (st *TxnState) Size() int {
<<<<<<< HEAD
	if st.Transaction == nil {
		return 0
=======
	size := st.stmtBufSize()
	if st.Transaction != nil {
		size += st.Transaction.Size()
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
	return st.Transaction.Size()
}

// NewStagingBuffer returns a new child write buffer.
func (st *TxnState) NewStagingBuffer() kv.MemBuffer {
	st.initStmtBuf()
	return st.stmtBuf.NewStagingBuffer()
}

// Flush flushes all staging kvs into parent buffer.
func (st *TxnState) Flush() (int, error) {
	if st.stmtBuf == nil {
		return 0, nil
	}
	return st.stmtBuf.Flush()
}

// Discard discards all staging kvs.
func (st *TxnState) Discard() {
	if st.stmtBuf == nil {
		return
	}
	st.stmtBuf.Discard()
}

// Valid implements the kv.Transaction interface.
func (st *TxnState) Valid() bool {
	return st.Transaction != nil && st.Transaction.Valid()
}

func (st *TxnState) pending() bool {
	return st.Transaction == nil && st.txnFuture != nil
}

func (st *TxnState) validOrPending() bool {
	return st.txnFuture != nil || st.Valid()
}

func (st *TxnState) String() string {
	if st.Transaction != nil {
		return st.Transaction.String()
	}
	if st.txnFuture != nil {
		return "txnFuture"
	}
	return "invalid transaction"
}

// GoString implements the "%#v" format for fmt.Printf.
func (st *TxnState) GoString() string {
	var s strings.Builder
	s.WriteString("Txn{")
	if st.pending() {
		s.WriteString("state=pending")
	} else if st.Valid() {
		s.WriteString("state=valid")
		fmt.Fprintf(&s, ", txnStartTS=%d", st.Transaction.StartTS())
		if len(st.mutations) > 0 {
			fmt.Fprintf(&s, ", len(mutations)=%d, %#v", len(st.mutations), st.mutations)
		}
<<<<<<< HEAD
=======
		if st.stmtBufLen() != 0 {
			fmt.Fprintf(&s, ", buf.length: %d, buf.size: %d", st.stmtBufLen(), st.stmtBufSize())
		}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	} else {
		s.WriteString("state=invalid")
	}

	s.WriteString("}")
	return s.String()
}

func (st *TxnState) changeInvalidToValid(txn kv.Transaction) {
	st.Transaction = txn
	st.initStmtBuf()
	st.txnFuture = nil
}

func (st *TxnState) changeInvalidToPending(future *txnFuture) {
	st.Transaction = nil
	st.txnFuture = future
}

func (st *TxnState) changePendingToValid(ctx context.Context) error {
	if st.txnFuture == nil {
		return errors.New("transaction future is not set")
	}

	future := st.txnFuture
	st.txnFuture = nil

	defer trace.StartRegion(ctx, "WaitTsoFuture").End()
	txn, err := future.wait()
	if err != nil {
		st.Transaction = nil
		return err
	}
	st.Transaction = txn
	st.initStmtBuf()
	return nil
}

func (st *TxnState) changeToInvalid() {
<<<<<<< HEAD
	if st.stagingHandle != kv.InvalidStagingHandle {
		st.Transaction.GetMemBuffer().Cleanup(st.stagingHandle)
	}
	st.stagingHandle = kv.InvalidStagingHandle
=======
	st.stmtBuf = nil
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	st.Transaction = nil
	st.txnFuture = nil
}

var hasMockAutoIncIDRetry = int64(0)

func enableMockAutoIncIDRetry() {
	atomic.StoreInt64(&hasMockAutoIncIDRetry, 1)
}

func mockAutoIncIDRetry() bool {
	return atomic.LoadInt64(&hasMockAutoIncIDRetry) == 1
}

var mockAutoRandIDRetryCount = int64(0)

func needMockAutoRandIDRetry() bool {
	return atomic.LoadInt64(&mockAutoRandIDRetryCount) > 0
}

func decreaseMockAutoRandIDRetryCount() {
	atomic.AddInt64(&mockAutoRandIDRetryCount, -1)
}

// ResetMockAutoRandIDRetryCount set the number of occurrences of
// `kv.ErrTxnRetryable` when calling TxnState.Commit().
func ResetMockAutoRandIDRetryCount(failTimes int64) {
	atomic.StoreInt64(&mockAutoRandIDRetryCount, failTimes)
}

// Commit overrides the Transaction interface.
func (st *TxnState) Commit(ctx context.Context) error {
	defer st.reset()
<<<<<<< HEAD
	if len(st.mutations) != 0 || st.countHint() != 0 {
=======
	if len(st.mutations) != 0 || len(st.dirtyTableOP) != 0 || st.stmtBufLen() != 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		logutil.BgLogger().Error("the code should never run here",
			zap.String("TxnState", st.GoString()),
			zap.Int("staging handler", int(st.stagingHandle)),
			zap.Stack("something must be wrong"))
		return errors.Trace(kv.ErrInvalidTxn)
	}

	// mockCommitError8942 is used for PR #8942.
	failpoint.Inject("mockCommitError8942", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	// mockCommitRetryForAutoIncID is used to mock an commit retry for adjustAutoIncrementDatum.
	failpoint.Inject("mockCommitRetryForAutoIncID", func(val failpoint.Value) {
		if val.(bool) && !mockAutoIncIDRetry() {
			enableMockAutoIncIDRetry()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	failpoint.Inject("mockCommitRetryForAutoRandID", func(val failpoint.Value) {
		if val.(bool) && needMockAutoRandIDRetry() {
			decreaseMockAutoRandIDRetryCount()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	return st.Transaction.Commit(ctx)
}

// Rollback overrides the Transaction interface.
func (st *TxnState) Rollback() error {
	defer st.reset()
	return st.Transaction.Rollback()
}

func (st *TxnState) reset() {
	st.cleanup()
	st.changeToInvalid()
}

<<<<<<< HEAD
func (st *TxnState) cleanup() {
	st.cleanupStmtBuf()
	st.initStmtBuf()
	for key := range st.mutations {
		delete(st.mutations, key)
	}
=======
// Get overrides the Transaction interface.
func (st *TxnState) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	val, err := st.stmtBufGet(ctx, k)
	if kv.IsErrNotFound(err) {
		val, err = st.Transaction.Get(ctx, k)
		if kv.IsErrNotFound(err) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, nil
}

// GetMemBuffer overrides the Transaction interface.
func (st *TxnState) GetMemBuffer() kv.MemBuffer {
	if st.stmtBuf == nil || st.stmtBuf.Size() == 0 {
		return st.Transaction.GetMemBuffer()
	}
	return kv.NewBufferStoreFrom(st.Transaction.GetMemBuffer(), st.stmtBuf)
}

// GetMemBufferSnapshot overrides the Transaction interface.
func (st *TxnState) GetMemBufferSnapshot() kv.MemBuffer {
	return st.Transaction.GetMemBuffer()
}

// BatchGet overrides the Transaction interface.
func (st *TxnState) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := st.stmtBufGet(ctx, key)
		if kv.IsErrNotFound(err) {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(val) != 0 {
			bufferValues[i] = val
		}
	}
	storageValues, err := st.Transaction.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if bufferValues[i] == nil {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}

// Set overrides the Transaction interface.
func (st *TxnState) Set(k kv.Key, v []byte) error {
	st.initStmtBuf()
	return st.stmtBuf.Set(k, v)
}

// Delete overrides the Transaction interface.
func (st *TxnState) Delete(k kv.Key) error {
	st.initStmtBuf()
	return st.stmtBuf.Delete(k)
}

// DeleteWithNeedLock overrides the Transaction interface.
func (st *TxnState) DeleteWithNeedLock(k kv.Key) error {
	st.initStmtBuf()
	return st.stmtBuf.DeleteWithNeedLock(k)
}

// Iter overrides the Transaction interface.
func (st *TxnState) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	retrieverIt, err := st.Transaction.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	if st.stmtBuf == nil {
		return retrieverIt, nil
	}
	bufferIt, err := st.stmtBuf.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse overrides the Transaction interface.
func (st *TxnState) IterReverse(k kv.Key) (kv.Iterator, error) {
	retrieverIt, err := st.Transaction.IterReverse(k)
	if err != nil {
		return nil, err
	}
	if st.stmtBuf == nil {
		return retrieverIt, nil
	}
	bufferIt, err := st.stmtBuf.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, true)
}

func (st *TxnState) cleanup() {
	if st.stmtBuf != nil {
		st.stmtBuf.Discard()
		st.stmtBuf = nil
	}
	for key := range st.mutations {
		delete(st.mutations, key)
	}
	if st.dirtyTableOP != nil {
		empty := dirtyTableOperation{}
		for i := 0; i < len(st.dirtyTableOP); i++ {
			st.dirtyTableOP[i] = empty
		}
		if len(st.dirtyTableOP) > 256 {
			// Reduce memory footprint for the large transaction.
			st.dirtyTableOP = nil
		} else {
			st.dirtyTableOP = st.dirtyTableOP[:0]
		}
	}
	if st.Transaction != nil {
		st.Transaction.ResetStmtKeyExistErrs()
	}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// KeysNeedToLock returns the keys need to be locked.
func (st *TxnState) KeysNeedToLock() ([]kv.Key, error) {
<<<<<<< HEAD
	if st.stagingHandle == kv.InvalidStagingHandle {
		return nil, nil
	}
	keys := make([]kv.Key, 0, st.countHint())
	buf := st.Transaction.GetMemBuffer()
	buf.InspectStage(st.stagingHandle, func(k kv.Key, flags kv.KeyFlags, v []byte) {
		if !keyNeedToLock(k, v, flags) {
			return
=======
	if st.stmtBufLen() == 0 {
		return nil, nil
	}
	keys := make([]kv.Key, 0, st.stmtBufLen())
	if err := kv.WalkMemBuffer(st.stmtBuf, func(k kv.Key, v []byte) error {
		if !st.stmtBuf.GetFlags(context.Background(), k).HasNeedLocked() && !keyNeedToLock(k, v) {
			return nil
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		}
		keys = append(keys, k)
	})
	return keys, nil
}

func keyNeedToLock(k, v []byte, flags kv.KeyFlags) bool {
	isTableKey := bytes.HasPrefix(k, tablecodec.TablePrefix())
	if !isTableKey {
		// meta key always need to lock.
		return true
	}
<<<<<<< HEAD
	if flags.HasPresumeKeyNotExists() {
		return true
	}

	// lock row key, primary key and unique index for delete operation,
	if len(v) == 0 {
		return flags.HasNeedLocked() || tablecodec.IsRecordKey(k)
=======

	// only need to delete row key here,
	// primary key and unique indexes are handled outside.
	if len(v) == 0 {
		return k[10] == 'r'
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}

	if tablecodec.IsUntouchedIndexKValue(k, v) {
		return false
	}
	isNonUniqueIndex := tablecodec.IsIndexKey(k) && len(v) == 1
	// Put row key and unique index need to lock.
	return !isNonUniqueIndex
}

func getBinlogMutation(ctx sessionctx.Context, tableID int64) *binlog.TableMutation {
	bin := binloginfo.GetPrewriteValue(ctx, true)
	for i := range bin.Mutations {
		if bin.Mutations[i].TableId == tableID {
			return &bin.Mutations[i]
		}
	}
	idx := len(bin.Mutations)
	bin.Mutations = append(bin.Mutations, binlog.TableMutation{TableId: tableID})
	return &bin.Mutations[idx]
}

func mergeToMutation(m1, m2 *binlog.TableMutation) {
	m1.InsertedRows = append(m1.InsertedRows, m2.InsertedRows...)
	m1.UpdatedRows = append(m1.UpdatedRows, m2.UpdatedRows...)
	m1.DeletedIds = append(m1.DeletedIds, m2.DeletedIds...)
	m1.DeletedPks = append(m1.DeletedPks, m2.DeletedPks...)
	m1.DeletedRows = append(m1.DeletedRows, m2.DeletedRows...)
	m1.Sequence = append(m1.Sequence, m2.Sequence...)
}

type txnFailFuture struct{}

func (txnFailFuture) Wait() (uint64, error) {
	return 0, errors.New("mock get timestamp fail")
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future   oracle.Future
	store    kv.Storage
	txnScope string
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.future.Wait()
	if err == nil {
		return tf.store.BeginWithStartTS(tf.txnScope, startTS)
	} else if config.GetGlobalConfig().Store == "unistore" {
		return nil, err
	}

	logutil.BgLogger().Warn("wait tso failed", zap.Error(err))
	// It would retry get timestamp.
	return tf.store.BeginWithTxnScope(tf.txnScope)
}

func (s *session) getTxnFuture(ctx context.Context) *txnFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := s.store.GetOracle()
	var tsFuture oracle.Future
	if s.sessionVars.LowResolutionTSO {
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx, &oracle.Option{TxnScope: s.sessionVars.CheckAndGetTxnScope()})
	} else {
		tsFuture = oracleStore.GetTimestampAsync(ctx, &oracle.Option{TxnScope: s.sessionVars.CheckAndGetTxnScope()})
	}
	ret := &txnFuture{future: tsFuture, store: s.store, txnScope: s.sessionVars.CheckAndGetTxnScope()}
	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		ret.future = txnFailFuture{}
	})
	return ret
}

// HasDirtyContent checks whether there's dirty update on the given table.
// Put this function here is to avoid cycle import.
func (s *session) HasDirtyContent(tid int64) bool {
	if s.txn.Transaction == nil {
		return false
	}
	seekKey := tablecodec.EncodeTablePrefix(tid)
	it, err := s.txn.GetMemBuffer().Iter(seekKey, nil)
	terror.Log(err)
	return it.Valid() && bytes.HasPrefix(it.Key(), seekKey)
}

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit() {
	defer func() {
		s.txn.cleanup()
	}()
<<<<<<< HEAD

	st := &s.txn
	st.flushStmtBuf()
=======

	st := &s.txn

	if _, err := st.Flush(); err != nil {
		return err
	}

	var err error
	failpoint.Inject("mockStmtCommitError", func(val failpoint.Value) {
		if val.(bool) && st.stmtBufLen() > 3 {
			err = errors.New("mock stmt commit error")
		}
	})
	if err != nil {
		st.doNotCommit = err
		return err
	}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

	// Need to flush binlog.
	for tableID, delta := range st.mutations {
		mutation := getBinlogMutation(s, tableID)
		mergeToMutation(mutation, delta)
	}
<<<<<<< HEAD
=======

	if len(st.dirtyTableOP) > 0 {
		dirtyDB := executor.GetDirtyDB(s)
		for _, op := range st.dirtyTableOP {
			mergeToDirtyDB(dirtyDB, op)
		}
	}
	st.MergeStmtKeyExistErrs()
	return nil
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// StmtRollback implements the sessionctx.Context interface.
func (s *session) StmtRollback() {
	s.txn.cleanup()
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *session) StmtGetMutation(tableID int64) *binlog.TableMutation {
	st := &s.txn
	if _, ok := st.mutations[tableID]; !ok {
		st.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	return st.mutations[tableID]
}
