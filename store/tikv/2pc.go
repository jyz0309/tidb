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
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
<<<<<<< HEAD
=======
	"sort"
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/prometheus/client_golang/prometheus"
	zap "go.uber.org/zap"
)

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchMutations) error
	tiKVTxnRegionsNumHistogram() prometheus.Observer
	String() string
}

<<<<<<< HEAD
=======
type actionPrewrite struct{}
type actionCommit struct{ retry bool }
type actionCleanup struct{}
type actionPessimisticLock struct {
	*kv.LockCtx
}
type actionPessimisticRollback struct{}

var (
	_ twoPhaseCommitAction = actionPrewrite{}
	_ twoPhaseCommitAction = actionCommit{}
	_ twoPhaseCommitAction = actionCleanup{}
	_ twoPhaseCommitAction = actionPessimisticLock{}
	_ twoPhaseCommitAction = actionPessimisticRollback{}
)

var (
	tikvSecondaryLockCleanupFailureCounterCommit   = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("commit")
	tikvSecondaryLockCleanupFailureCounterRollback = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("rollback")
	tiKVTxnHeartBeatHistogramOK                    = metrics.TiKVTxnHeartBeatHistogram.WithLabelValues("ok")
	tiKVTxnHeartBeatHistogramError                 = metrics.TiKVTxnHeartBeatHistogram.WithLabelValues("err")

	tiKVTxnRegionsNumHistogramPrewrite            = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("prewrite"))
	tiKVTxnRegionsNumHistogramCommit              = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("commit"))
	tiKVTxnRegionsNumHistogramCleanup             = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("cleanup"))
	tiKVTxnRegionsNumHistogramPessimisticLock     = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("pessimistic_lock"))
	tiKVTxnRegionsNumHistogramPessimisticRollback = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("pessimistic_rollback"))
)

>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store               *KVStore
	txn                 *tikvTxn
	startTS             uint64
	mutations           *memBufferMutations
	lockTTL             uint64
	commitTS            uint64
	priority            pb.CommandPri
	sessionID           uint64 // sessionID is used for log.
	cleanWg             sync.WaitGroup
	detail              unsafe.Pointer
	txnSize             int
	hasNoNeedCommitKeys bool

	primaryKey  []byte
	forUpdateTS uint64

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	syncLog bool
	// For pessimistic transaction
	isPessimistic bool
	isFirstLock   bool
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
	// Used by pessimistic transaction and large transaction.
	ttlManager

	testingKnobs struct {
		acAfterCommitPrimary chan struct{}
		bkAfterCommitPrimary chan struct{}
		noFallBack           bool
	}

	useAsyncCommit    uint32
	minCommitTS       uint64
	maxCommitTS       uint64
	prewriteStarted   bool
	prewriteCancelled uint32
	useOnePC          uint32
	onePCCommitTS     uint64

	// doingAmend means the amend prewrite is ongoing.
	doingAmend bool

	binlog BinlogExecutor
}

type memBufferMutations struct {
	storage kv.MemBuffer
	handles []kv.MemKeyHandle
}

func newMemBufferMutations(sizeHint int, storage kv.MemBuffer) *memBufferMutations {
	return &memBufferMutations{
		handles: make([]kv.MemKeyHandle, 0, sizeHint),
		storage: storage,
	}
}

func (m *memBufferMutations) Len() int {
	return len(m.handles)
}

func (m *memBufferMutations) GetKey(i int) []byte {
	return m.storage.GetKeyByHandle(m.handles[i])
}

func (m *memBufferMutations) GetKeys() [][]byte {
	ret := make([][]byte, m.Len())
	for i := range ret {
		ret[i] = m.GetKey(i)
	}
	return ret
}

func (m *memBufferMutations) GetValue(i int) []byte {
	v, _ := m.storage.GetValueByHandle(m.handles[i])
	return v
}

func (m *memBufferMutations) GetOp(i int) pb.Op {
	return pb.Op(m.handles[i].UserData >> 1)
}

<<<<<<< HEAD
func (m *memBufferMutations) IsPessimisticLock(i int) bool {
	return m.handles[i].UserData&1 != 0
}
=======
// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store            *tikvStore
	txn              *tikvTxn
	startTS          uint64
	mutations        CommitterMutations
	lockTTL          uint64
	commitTS         uint64
	priority         pb.CommandPri
	connID           uint64 // connID is used for log.
	cleanWg          sync.WaitGroup
	detail           unsafe.Pointer
	txnSize          int
	noNeedCommitKeys map[string]struct{}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

func (m *memBufferMutations) Slice(from, to int) CommitterMutations {
	return &memBufferMutations{
		handles: m.handles[from:to],
		storage: m.storage,
	}
}

func (m *memBufferMutations) Push(op pb.Op, isPessimisticLock bool, handle kv.MemKeyHandle) {
	aux := uint16(op) << 1
	if isPessimisticLock {
		aux |= 1
	}
<<<<<<< HEAD
	handle.UserData = aux
	m.handles = append(m.handles, handle)
}

// CommitterMutations contains the mutations to be submitted.
type CommitterMutations interface {
	Len() int
	GetKey(i int) []byte
	GetKeys() [][]byte
	GetOp(i int) pb.Op
	GetValue(i int) []byte
	IsPessimisticLock(i int) bool
	Slice(from, to int) CommitterMutations
}

// PlainMutations contains transaction operations.
type PlainMutations struct {
=======
	syncLog bool
	// For pessimistic transaction
	isPessimistic bool
	isFirstLock   bool
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
	// Used by pessimistic transaction and large transaction.
	ttlManager

	testingKnobs struct {
		acAfterCommitPrimary chan struct{}
		bkAfterCommitPrimary chan struct{}
	}

	// doingAmend means the amend prewrite is ongoing.
	doingAmend bool
}

// CommitterMutations contains transaction operations.
type CommitterMutations struct {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	ops               []pb.Op
	keys              [][]byte
	values            [][]byte
	isPessimisticLock []bool
}

<<<<<<< HEAD
// NewPlainMutations creates a PlainMutations object with sizeHint reserved.
func NewPlainMutations(sizeHint int) PlainMutations {
	return PlainMutations{
=======
// Mutation represents a single transaction operation.
type Mutation struct {
	KeyOp             pb.Op
	Key               []byte
	Value             []byte
	IsPessimisticLock bool
}

// NewCommiterMutations creates a CommitterMutations object with sizeHint reserved.
func NewCommiterMutations(sizeHint int) CommitterMutations {
	return CommitterMutations{
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		ops:               make([]pb.Op, 0, sizeHint),
		keys:              make([][]byte, 0, sizeHint),
		values:            make([][]byte, 0, sizeHint),
		isPessimisticLock: make([]bool, 0, sizeHint),
	}
}

<<<<<<< HEAD
// Slice return a sub mutations in range [from, to).
func (c *PlainMutations) Slice(from, to int) CommitterMutations {
	var res PlainMutations
=======
func (c *CommitterMutations) subRange(from, to int) CommitterMutations {
	var res CommitterMutations
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	res.keys = c.keys[from:to]
	if c.ops != nil {
		res.ops = c.ops[from:to]
	}
	if c.values != nil {
		res.values = c.values[from:to]
	}
	if c.isPessimisticLock != nil {
		res.isPessimisticLock = c.isPessimisticLock[from:to]
	}
	return &res
}

// Push another mutation into mutations.
<<<<<<< HEAD
func (c *PlainMutations) Push(op pb.Op, key []byte, value []byte, isPessimisticLock bool) {
=======
func (c *CommitterMutations) Push(op pb.Op, key []byte, value []byte, isPessimisticLock bool) {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	c.ops = append(c.ops, op)
	c.keys = append(c.keys, key)
	c.values = append(c.values, value)
	c.isPessimisticLock = append(c.isPessimisticLock, isPessimisticLock)
}

<<<<<<< HEAD
// Len returns the count of mutations.
func (c *PlainMutations) Len() int {
=======
func (c *CommitterMutations) len() int {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	return len(c.keys)
}

// GetKey returns the key at index.
func (c *PlainMutations) GetKey(i int) []byte {
	return c.keys[i]
}

// GetKeys returns the keys.
func (c *PlainMutations) GetKeys() [][]byte {
	return c.keys
}

// GetOps returns the key ops.
func (c *PlainMutations) GetOps() []pb.Op {
	return c.ops
}

// GetValues returns the key values.
func (c *PlainMutations) GetValues() [][]byte {
	return c.values
}

// GetPessimisticFlags returns the key pessimistic flags.
func (c *PlainMutations) GetPessimisticFlags() []bool {
	return c.isPessimisticLock
}

// GetOp returns the key op at index.
func (c *PlainMutations) GetOp(i int) pb.Op {
	return c.ops[i]
}

// GetValue returns the key value at index.
func (c *PlainMutations) GetValue(i int) []byte {
	if len(c.values) <= i {
		return nil
	}
	return c.values[i]
}

// IsPessimisticLock returns the key pessimistic flag at index.
func (c *PlainMutations) IsPessimisticLock(i int) bool {
	return c.isPessimisticLock[i]
}

// PlainMutation represents a single transaction operation.
type PlainMutation struct {
	KeyOp             pb.Op
	Key               []byte
	Value             []byte
	IsPessimisticLock bool
}

// MergeMutations append input mutations into current mutations.
func (c *PlainMutations) MergeMutations(mutations PlainMutations) {
	c.ops = append(c.ops, mutations.ops...)
	c.keys = append(c.keys, mutations.keys...)
	c.values = append(c.values, mutations.values...)
	c.isPessimisticLock = append(c.isPessimisticLock, mutations.isPessimisticLock...)
}

// AppendMutation merges a single Mutation into the current mutations.
func (c *PlainMutations) AppendMutation(mutation PlainMutation) {
	c.ops = append(c.ops, mutation.KeyOp)
	c.keys = append(c.keys, mutation.Key)
	c.values = append(c.values, mutation.Value)
	c.isPessimisticLock = append(c.isPessimisticLock, mutation.IsPessimisticLock)
}

// GetKeys returns the keys.
func (c *CommitterMutations) GetKeys() [][]byte {
	return c.keys
}

// GetOps returns the key ops.
func (c *CommitterMutations) GetOps() []pb.Op {
	return c.ops
}

// GetValues returns the key values.
func (c *CommitterMutations) GetValues() [][]byte {
	return c.values
}

// GetPessimisticFlags returns the key pessimistic flags.
func (c *CommitterMutations) GetPessimisticFlags() []bool {
	return c.isPessimisticLock
}

// MergeMutations append input mutations into current mutations.
func (c *CommitterMutations) MergeMutations(mutations CommitterMutations) {
	c.ops = append(c.ops, mutations.ops...)
	c.keys = append(c.keys, mutations.keys...)
	c.values = append(c.values, mutations.values...)
	c.isPessimisticLock = append(c.isPessimisticLock, mutations.isPessimisticLock...)
}

// AppendMutation merges a single Mutation into the current mutations.
func (c *CommitterMutations) AppendMutation(mutation Mutation) {
	c.ops = append(c.ops, mutation.KeyOp)
	c.keys = append(c.keys, mutation.Key)
	c.values = append(c.values, mutation.Value)
	c.isPessimisticLock = append(c.isPessimisticLock, mutation.IsPessimisticLock)
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, sessionID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		sessionID:     sessionID,
		regionTxnSize: map[uint64]int{},
		ttlManager: ttlManager{
			ch: make(chan struct{}),
		},
		isPessimistic: txn.IsPessimistic(),
<<<<<<< HEAD
		binlog: &binlogExecutor{
			txn: txn,
		},
=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}, nil
}

func (c *twoPhaseCommitter) extractKeyExistsErr(key kv.Key) error {
	if !c.txn.us.HasPresumeKeyNotExists(key) {
		return errors.Errorf("session %d, existErr for key:%s should not be nil", c.sessionID, key)
	}

	tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return c.genKeyExistsError("UNKNOWN", key.String(), err)
	}

	tblInfo := c.txn.us.GetTableInfo(tableID)
	if tblInfo == nil {
		return c.genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find table info"))
	}

	value, err := c.txn.us.GetMemBuffer().SelectValueHistory(key, func(value []byte) bool { return len(value) != 0 })
	if err != nil {
		return c.genKeyExistsError("UNKNOWN", key.String(), err)
	}

	if isRecord {
		return c.extractKeyExistsErrFromHandle(key, value, tblInfo)
	}
	return c.extractKeyExistsErrFromIndex(key, value, tblInfo, indexID)
}

func (c *twoPhaseCommitter) extractKeyExistsErrFromIndex(key kv.Key, value []byte, tblInfo *model.TableInfo, indexID int64) error {
	var idxInfo *model.IndexInfo
	for _, index := range tblInfo.Indices {
		if index.ID == indexID {
			idxInfo = index
		}
	}
	if idxInfo == nil {
		return c.genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find index info"))
	}
	name := idxInfo.Name.String()

	if len(value) == 0 {
		return c.genKeyExistsError(name, key.String(), errors.New("missing value"))
	}

	colInfo := make([]rowcodec.ColInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}

	values, err := tablecodec.DecodeIndexKV(key, value, len(idxInfo.Columns), tablecodec.HandleNotNeeded, colInfo)
	if err != nil {
		return c.genKeyExistsError(name, key.String(), err)
	}
	valueStr := make([]string, 0, len(values))
	for i, val := range values {
		d, err := tablecodec.DecodeColumnValue(val, colInfo[i].Ft, time.Local)
		if err != nil {
			return c.genKeyExistsError(name, key.String(), err)
		}
		str, err := d.ToString()
		if err != nil {
			return c.genKeyExistsError(name, key.String(), err)
		}
		valueStr = append(valueStr, str)
	}
	return c.genKeyExistsError(name, strings.Join(valueStr, "-"), nil)
}

func (c *twoPhaseCommitter) extractKeyExistsErrFromHandle(key kv.Key, value []byte, tblInfo *model.TableInfo) error {
	const name = "PRIMARY"
	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil {
		return c.genKeyExistsError(name, key.String(), err)
	}

	if handle.IsInt() {
		if pkInfo := tblInfo.GetPkColInfo(); pkInfo != nil {
			if mysql.HasUnsignedFlag(pkInfo.Flag) {
				handleStr := fmt.Sprintf("%d", uint64(handle.IntValue()))
				return c.genKeyExistsError(name, handleStr, nil)
			}
		}
		return c.genKeyExistsError(name, handle.String(), nil)
	}

	if len(value) == 0 {
		return c.genKeyExistsError(name, handle.String(), errors.New("missing value"))
	}

	idxInfo := tables.FindPrimaryIndex(tblInfo)
	if idxInfo == nil {
		return c.genKeyExistsError(name, handle.String(), errors.New("cannot find index info"))
	}

	cols := make(map[int64]*types.FieldType, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		cols[col.ID] = &col.FieldType
	}
	handleColIDs := make([]int64, 0, len(idxInfo.Columns))
	for _, col := range idxInfo.Columns {
		handleColIDs = append(handleColIDs, tblInfo.Columns[col.Offset].ID)
	}

	row, err := tablecodec.DecodeRowToDatumMap(value, cols, time.Local)
	if err != nil {
		return c.genKeyExistsError(name, handle.String(), err)
	}

	data, err := tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, cols, time.Local, row)
	if err != nil {
		return c.genKeyExistsError(name, handle.String(), err)
	}

	valueStr := make([]string, 0, len(data))
	for _, col := range idxInfo.Columns {
		d := data[tblInfo.Columns[col.Offset].ID]
		str, err := d.ToString()
		if err != nil {
			return c.genKeyExistsError(name, key.String(), err)
		}
		valueStr = append(valueStr, str)
	}
	return c.genKeyExistsError(name, strings.Join(valueStr, "-"), nil)
}

func (c *twoPhaseCommitter) genKeyExistsError(name string, value string, err error) error {
	if err != nil {
		logutil.BgLogger().Info("extractKeyExistsErr meets error", zap.Error(err))
	}
	return kv.ErrKeyExists.FastGenByArgs(value, name)
}

func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var size, putCnt, delCnt, lockCnt, checkCnt int

	txn := c.txn
<<<<<<< HEAD
	memBuf := txn.GetMemBuffer()
	sizeHint := txn.us.GetMemBuffer().Len()
	c.mutations = newMemBufferMutations(sizeHint, memBuf)
	c.isPessimistic = txn.IsPessimistic()

	var err error
	for it := memBuf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		key := it.Key()
		flags := it.Flags()
		var value []byte
		var op pb.Op

		if !it.HasValue() {
			if !flags.HasLocked() {
				continue
			}
			op = pb.Op_Lock
			lockCnt++
=======
	sizeHint := len(txn.lockKeys) + txn.us.Len()
	mutations := NewCommiterMutations(sizeHint)
	c.isPessimistic = txn.IsPessimistic()

	// Merge ordered lockKeys and pairs in the memBuffer into the mutations array
	sort.Slice(txn.lockKeys, func(i, j int) bool {
		return bytes.Compare(txn.lockKeys[i], txn.lockKeys[j]) < 0
	})
	lockIdx := 0
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		var (
			op                pb.Op
			value             []byte
			isPessimisticLock bool
		)
		if len(v) > 0 {
			if tablecodec.IsUntouchedIndexKValue(k, v) {
				if _, ok := c.txn.lockedMap[string(k)]; !ok {
					return nil
				}
				op = pb.Op_Lock
				value = v
				lockCnt++
			} else {
				op = pb.Op_Put
				if c := txn.us.GetKeyExistErrInfo(k); c != nil {
					op = pb.Op_Insert
				}
				value = v
				putCnt++
			}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		} else {
			value = it.Value()
			if len(value) > 0 {
				if tablecodec.IsUntouchedIndexKValue(key, value) {
					if !flags.HasLocked() {
						continue
					}
					op = pb.Op_Lock
					lockCnt++
				} else {
					op = pb.Op_Put
					if flags.HasPresumeKeyNotExists() {
						op = pb.Op_Insert
					}
					putCnt++
				}
			} else {
				if !txn.IsPessimistic() && flags.HasPresumeKeyNotExists() {
					// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
					// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
					op = pb.Op_CheckNotExists
					checkCnt++
					memBuf.UpdateFlags(key, kv.SetPrewriteOnly)
				} else {
					// normal delete keys in optimistic txn can be delete without not exists checking
					// delete-your-writes keys in pessimistic txn can ensure must be no exists so can directly delete them
					op = pb.Op_Del
					delCnt++
				}
			}
		}
<<<<<<< HEAD

		var isPessimistic bool
		if flags.HasLocked() {
			isPessimistic = c.isPessimistic
		}
		c.mutations.Push(op, isPessimistic, it.Handle())
		size += len(key) + len(value)

		if len(c.primaryKey) == 0 && op != pb.Op_CheckNotExists {
			c.primaryKey = key
		}
=======
		for lockIdx < len(txn.lockKeys) {
			lockKey := txn.lockKeys[lockIdx]
			ord := bytes.Compare(lockKey, k)
			if ord == 0 {
				isPessimisticLock = c.isPessimistic
				lockIdx++
				break
			} else if ord > 0 {
				break
			} else {
				mutations.Push(pb.Op_Lock, lockKey, nil, c.isPessimistic)
				lockCnt++
				size += len(lockKey)
				lockIdx++
			}
		}
		mutations.Push(op, k, value, isPessimisticLock)
		entrySize := len(k) + len(v)
		if uint64(entrySize) > kv.TxnEntrySizeLimit {
			return kv.ErrEntryTooLarge.GenWithStackByArgs(kv.TxnEntrySizeLimit, entrySize)
		}
		size += entrySize
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	// add the remaining locks to mutations and keys
	for _, lockKey := range txn.lockKeys[lockIdx:] {
		mutations.Push(pb.Op_Lock, lockKey, nil, c.isPessimistic)
		lockCnt++
		size += len(lockKey)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}

	if c.mutations.Len() == 0 {
		return nil
	}
	c.txnSize = size

	if len(c.primaryKey) == 0 {
		for i, op := range mutations.ops {
			if op != pb.Op_CheckNotExists {
				c.primaryKey = mutations.keys[i]
				break
			}
		}
	}

	if size > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(size)
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if c.mutations.Len() > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(c.mutations.GetKey(0))
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("session", c.sessionID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", c.mutations.Len()),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Int("checks", checkCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("session", c.sessionID),
			zap.Error(err))
		return errors.Trace(err)
	}

	commitDetail := &execdetails.CommitDetails{WriteSize: size, WriteKeys: c.mutations.Len()}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.hasNoNeedCommitKeys = checkCnt > 0
	c.lockTTL = txnLockTTL(txn.startTime, size)
	c.priority = getTxnPriority(txn)
	c.syncLog = getTxnSyncLog(txn)
	c.setDetail(commitDetail)
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.mutations.GetKey(0)
	}
	return c.primaryKey
}

// asyncSecondaries returns all keys that must be checked in the recovery phase of an async commit.
func (c *twoPhaseCommitter) asyncSecondaries() [][]byte {
	secondaries := make([][]byte, 0, c.mutations.Len())
	for i := 0; i < c.mutations.Len(); i++ {
		k := c.mutations.GetKey(i)
		if bytes.Equal(k, c.primary()) || c.mutations.GetOp(i) == pb.Op_CheckNotExists {
			continue
		}
		secondaries = append(secondaries, k)
	}
	return secondaries
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 4MiB, or 10MiB, ttl is 6s, 12s, 20s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > ManagedLockTTL {
			lockTTL = ManagedLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

var preSplitDetectThreshold uint32 = 100000
var preSplitSizeThreshold uint32 = 32 << 20

// doActionOnMutations groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
func (c *twoPhaseCommitter) doActionOnMutations(bo *Backoffer, action twoPhaseCommitAction, mutations CommitterMutations) error {
<<<<<<< HEAD
	if mutations.Len() == 0 {
=======
	if mutations.len() == 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		return nil
	}
	groups, err := c.groupMutations(bo, mutations)
	if err != nil {
		return errors.Trace(err)
	}

<<<<<<< HEAD
	// This is redundant since `doActionOnGroupMutations` will still split groups into batches and
	// check the number of batches. However we don't want the check fail after any code changes.
	c.checkOnePCFallBack(action, len(groups))

	return c.doActionOnGroupMutations(bo, action, groups)
}

// groupMutations groups mutations by region, then checks for any large groups and in that case pre-splits the region.
func (c *twoPhaseCommitter) groupMutations(bo *Backoffer, mutations CommitterMutations) ([]groupedMutations, error) {
	groups, err := c.store.regionCache.groupSortedMutationsByRegion(bo, mutations)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Pre-split regions to avoid too much write workload into a single region.
	// In the large transaction case, this operation is important to avoid TiKV 'server is busy' error.
	var didPreSplit bool
	preSplitDetectThresholdVal := atomic.LoadUint32(&preSplitDetectThreshold)
	for _, group := range groups {
		if uint32(group.mutations.Len()) >= preSplitDetectThresholdVal {
			logutil.BgLogger().Info("2PC detect large amount of mutations on a single region",
				zap.Uint64("region", group.region.GetID()),
				zap.Int("mutations count", group.mutations.Len()))
			// Use context.Background, this time should not add up to Backoffer.
			if c.store.preSplitRegion(context.Background(), group) {
				didPreSplit = true
=======
	// Pre-split regions to avoid too much write workload into a single region.
	// In the large transaction case, this operation is important to avoid TiKV 'server is busy' error.
	var preSplited bool
	preSplitDetectThresholdVal := atomic.LoadUint32(&preSplitDetectThreshold)
	for _, group := range groups {
		if uint32(group.mutations.len()) >= preSplitDetectThresholdVal {
			logutil.BgLogger().Info("2PC detect large amount of mutations on a single region",
				zap.Uint64("region", group.region.GetID()),
				zap.Int("mutations count", group.mutations.len()))
			// Use context.Background, this time should not add up to Backoffer.
			if preSplitAndScatterIn2PC(context.Background(), c.store, group) {
				preSplited = true
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			}
		}
	}
	// Reload region cache again.
<<<<<<< HEAD
	if didPreSplit {
		groups, err = c.store.regionCache.groupSortedMutationsByRegion(bo, mutations)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return groups, nil
}

// doActionOnGroupedMutations splits groups into batches (there is one group per region, and potentially many batches per group, but all mutations
// in a batch will belong to the same region).
=======
	if preSplited {
		groups, err = c.store.regionCache.GroupSortedMutationsByRegion(bo, mutations)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return c.doActionOnGroupMutations(bo, action, groups)
}

func preSplitAndScatterIn2PC(ctx context.Context, store *tikvStore, group groupedMutations) bool {
	splitKeys := make([][]byte, 0, 4)

	preSplitSizeThresholdVal := atomic.LoadUint32(&preSplitSizeThreshold)
	regionSize := 0
	keysLength := group.mutations.len()
	valsLength := len(group.mutations.values)
	// The value length maybe zero for pessimistic lock keys
	for i := 0; i < keysLength; i++ {
		regionSize = regionSize + len(group.mutations.keys[i])
		if i < valsLength {
			regionSize = regionSize + len(group.mutations.values[i])
		}
		// The second condition is used for testing.
		if regionSize >= int(preSplitSizeThresholdVal) {
			regionSize = 0
			splitKeys = append(splitKeys, group.mutations.keys[i])
		}
	}
	if len(splitKeys) == 0 {
		return false
	}

	regionIDs, err := store.SplitRegions(ctx, splitKeys, true, nil)
	if err != nil {
		logutil.BgLogger().Warn("2PC split regions failed", zap.Uint64("regionID", group.region.id),
			zap.Int("keys count", keysLength), zap.Int("values count", valsLength), zap.Error(err))
		return false
	}

	for _, regionID := range regionIDs {
		err := store.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("2PC wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}
	// Invalidate the old region cache information.
	store.regionCache.InvalidateCachedRegion(group.region)
	return true
}

>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
func (c *twoPhaseCommitter) doActionOnGroupMutations(bo *Backoffer, action twoPhaseCommitAction, groups []groupedMutations) error {
	action.tiKVTxnRegionsNumHistogram().Observe(float64(len(groups)))

	var sizeFunc = c.keySize

	switch act := action.(type) {
	case actionPrewrite:
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if len(bo.errors) == 0 {
			for _, group := range groups {
				c.regionTxnSize[group.region.id] = group.mutations.Len()
			}
		}
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.getDetail().PrewriteRegionNum, int32(len(groups)))
	case actionPessimisticLock:
		if act.LockCtx.Stats != nil {
			act.LockCtx.Stats.RegionNum = int32(len(groups))
		}
	}

	batchBuilder := newBatched(c.primary())
	for _, group := range groups {
		batchBuilder.appendBatchMutationsBySize(group.region, group.mutations, sizeFunc, txnCommitBatchSize)
	}
	firstIsPrimary := batchBuilder.setPrimary()

<<<<<<< HEAD
=======
	firstIsPrimary := false
	// If the batches include the primary key, put it to the first
	if primaryIdx >= 0 {
		batches[primaryIdx].isPrimary = true
		batches[0], batches[primaryIdx] = batches[primaryIdx], batches[0]
		firstIsPrimary = true
	}

>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	actionCommit, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	_, actionIsPessimiticLock := action.(actionPessimisticLock)

<<<<<<< HEAD
	c.checkOnePCFallBack(action, len(batchBuilder.allBatches()))

	var err error
	failpoint.Inject("skipKeyReturnOK", func(val failpoint.Value) {
		valStr, ok := val.(string)
		if ok && c.sessionID > 0 {
=======
	var err error
	failpoint.Inject("skipKeyReturnOK", func(val failpoint.Value) {
		valStr, ok := val.(string)
		if ok && c.connID > 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			if firstIsPrimary && actionIsPessimiticLock {
				logutil.Logger(bo.ctx).Warn("pessimisticLock failpoint", zap.String("valStr", valStr))
				switch valStr {
				case "pessimisticLockSkipPrimary":
<<<<<<< HEAD
					err = c.doActionOnBatches(bo, action, batchBuilder.allBatches())
					failpoint.Return(err)
				case "pessimisticLockSkipSecondary":
					err = c.doActionOnBatches(bo, action, batchBuilder.primaryBatch())
=======
					err = c.doActionOnBatches(bo, action, batches)
					failpoint.Return(err)
				case "pessimisticLockSkipSecondary":
					err = c.doActionOnBatches(bo, action, batches[:1])
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
					failpoint.Return(err)
				}
			}
		}
	})
	failpoint.Inject("pessimisticRollbackDoNth", func() {
		_, actionIsPessimisticRollback := action.(actionPessimisticRollback)
<<<<<<< HEAD
		if actionIsPessimisticRollback && c.sessionID > 0 {
=======
		if actionIsPessimisticRollback && c.connID > 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			logutil.Logger(bo.ctx).Warn("pessimisticRollbackDoNth failpoint")
			failpoint.Return(nil)
		}
	})

<<<<<<< HEAD
	if firstIsPrimary &&
		((actionIsCommit && !c.isAsyncCommit()) || actionIsCleanup || actionIsPessimiticLock) {
		// primary should be committed(not async commit)/cleanup/pessimistically locked first
		err = c.doActionOnBatches(bo, action, batchBuilder.primaryBatch())
=======
	if firstIsPrimary && (actionIsCommit || actionIsCleanup || actionIsPessimiticLock) {
		// primary should be committed/cleanup/pessimistically locked first
		err = c.doActionOnBatches(bo, action, batches[:1])
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if err != nil {
			return errors.Trace(err)
		}
		if actionIsCommit && c.testingKnobs.bkAfterCommitPrimary != nil && c.testingKnobs.acAfterCommitPrimary != nil {
			c.testingKnobs.acAfterCommitPrimary <- struct{}{}
			<-c.testingKnobs.bkAfterCommitPrimary
		}
<<<<<<< HEAD
		batchBuilder.forgetPrimary()
	}
	// Already spawned a goroutine for async commit transaction.
	if actionIsCommit && !actionCommit.retry && !c.isAsyncCommit() {
		secondaryBo := NewBackofferWithVars(context.Background(), int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
		go func() {
			if c.sessionID > 0 {
				failpoint.Inject("beforeCommitSecondaries", func(v failpoint.Value) {
					if s, ok := v.(string); !ok {
						logutil.Logger(bo.ctx).Info("[failpoint] sleep 2s before commit secondary keys",
							zap.Uint64("sessionID", c.sessionID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
						time.Sleep(2 * time.Second)
					} else if s == "skip" {
						logutil.Logger(bo.ctx).Info("[failpoint] injected skip committing secondaries",
							zap.Uint64("sessionID", c.sessionID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
=======
		batches = batches[1:]
	}
	if actionIsCommit && !actionCommit.retry {
		// Commit secondary batches in background goroutine to reduce latency.
		// The backoffer instance is created outside of the goroutine to avoid
		// potential data race in unit test since `CommitMaxBackoff` will be updated
		// by test suites.
		secondaryBo := NewBackofferWithVars(context.Background(), CommitMaxBackoff, c.txn.vars)
		go func() {
			if c.connID > 0 {
				failpoint.Inject("beforeCommitSecondaries", func(v failpoint.Value) {
					if s, ok := v.(string); !ok {
						logutil.Logger(bo.ctx).Info("[failpoint] sleep 2s before commit secondary keys",
							zap.Uint64("connID", c.connID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
						time.Sleep(2 * time.Second)
					} else if s == "skip" {
						logutil.Logger(bo.ctx).Info("[failpoint] injected skip committing secondaries",
							zap.Uint64("connID", c.connID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
						failpoint.Return()
					}
				})
			}

<<<<<<< HEAD
			e := c.doActionOnBatches(secondaryBo, action, batchBuilder.allBatches())
=======
			e := c.doActionOnBatches(secondaryBo, action, batches)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			if e != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("session", c.sessionID),
					zap.Stringer("action type", action),
					zap.Error(e))
				metrics.SecondaryLockCleanupFailureCounterCommit.Inc()
			}
		}()
	} else {
		err = c.doActionOnBatches(bo, action, batchBuilder.allBatches())
	}
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchMutations) error {
	if len(batches) == 0 {
		return nil
	}

	noNeedFork := len(batches) == 1
	if !noNeedFork {
		if ac, ok := action.(actionCommit); ok && ac.retry {
			noNeedFork = true
<<<<<<< HEAD
		}
	}
	if noNeedFork {
		for _, b := range batches {
			e := action.handleSingleBatch(c, bo, b)
			if e != nil {
				logutil.BgLogger().Debug("2PC doActionOnBatches failed",
					zap.Uint64("session", c.sessionID),
					zap.Stringer("action type", action),
					zap.Error(e),
					zap.Uint64("txnStartTS", c.startTS))
				return errors.Trace(e)
			}
		}
=======
		}
	}
	if noNeedFork {
		for _, b := range batches {
			e := action.handleSingleBatch(c, bo, b)
			if e != nil {
				logutil.BgLogger().Debug("2PC doActionOnBatches failed",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e),
					zap.Uint64("txnStartTS", c.startTS))
				return errors.Trace(e)
			}
		}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		return nil
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
<<<<<<< HEAD
	if rateLim > config.GetGlobalConfig().CommitterConcurrency {
		rateLim = config.GetGlobalConfig().CommitterConcurrency
=======
	if rateLim > config.GetGlobalConfig().Performance.CommitterConcurrency {
		rateLim = config.GetGlobalConfig().Performance.CommitterConcurrency
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
	batchExecutor := newBatchExecutor(rateLim, c, action, bo)
	err := batchExecutor.process(batches)
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key, value []byte) int {
	return len(key) + len(value)
}

func (c *twoPhaseCommitter) keySize(key, value []byte) int {
	return len(key)
}

type ttlManagerState uint32

const (
	stateUninitialized ttlManagerState = iota
	stateRunning
	stateClosed
)

type ttlManager struct {
	state   ttlManagerState
	ch      chan struct{}
	lockCtx *kv.LockCtx
}

func (tm *ttlManager) run(c *twoPhaseCommitter, lockCtx *kv.LockCtx) {
	// Run only once.
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateUninitialized), uint32(stateRunning)) {
		return
	}
	tm.lockCtx = lockCtx
	noKeepAlive := false
	failpoint.Inject("doNotKeepAlive", func() {
		noKeepAlive = true
	})

<<<<<<< HEAD
	if !noKeepAlive {
		go tm.keepAlive(c)
=======
	ttl := c.lockTTL

	if c.connID > 0 {
		failpoint.Inject("twoPCShortLockTTL", func() {
			ttl = 1
			keys := make([]string, 0, len(mutations))
			for _, m := range mutations {
				keys = append(keys, hex.EncodeToString(m.Key))
			}
			logutil.BgLogger().Info("[failpoint] injected lock ttl = 1 on prewrite",
				zap.Uint64("txnStartTS", c.startTS), zap.Strings("keys", keys))
		})
	}

	req := &pb.PrewriteRequest{
		Mutations:         mutations,
		PrimaryLock:       c.primary(),
		StartVersion:      c.startTS,
		LockTtl:           ttl,
		IsPessimisticLock: m.isPessimisticLock,
		ForUpdateTs:       c.forUpdateTS,
		TxnSize:           txnSize,
		MinCommitTs:       minCommitTS,
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
}

<<<<<<< HEAD
func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
=======
func (actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	if c.connID > 0 {
		failpoint.Inject("prewritePrimaryFail", func() {
			if batch.isPrimary {
				logutil.Logger(bo.ctx).Info("[failpoint] injected error on prewriting primary batch",
					zap.Uint64("txnStartTS", c.startTS))
				failpoint.Return(errors.New("injected error on prewriting primary batch"))
			}
		})
	}

	txnSize := uint64(c.regionTxnSize[batch.region.id])
	// When we retry because of a region miss, we don't know the transaction size. We set the transaction size here
	// to MaxUint64 to avoid unexpected "resolve lock lite".
	if len(bo.errors) > 0 {
		txnSize = math.MaxUint64
	}

	req := c.buildPrewriteRequest(batch, txnSize)
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.prewriteMutations(bo, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			if batch.isPrimary {
				// After writing the primary key, if the size of the transaction is large than 32M,
				// start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
				if c.txnSize > 32*1024*1024 {
					c.run(c, nil)
				}
			}
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				existErrInfo := c.txn.us.GetKeyExistErrInfo(key)
				if existErrInfo == nil {
					return errors.Errorf("conn %d, existErr for key:%s should not be nil", c.connID, key)
				}
				return existErrInfo.Err()
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			logutil.BgLogger().Info("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			locks = append(locks, lock)
		}
		start := time.Now()
		msBeforeExpired, err := c.store.lockResolver.resolveLocksForWrite(bo, c.startTS, locks)
		if err != nil {
			return errors.Trace(err)
		}
		atomic.AddInt64(&c.getDetail().ResolveLockTime, int64(time.Since(start)))
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

type ttlManagerState uint32

const (
	stateUninitialized ttlManagerState = iota
	stateRunning
	stateClosed
)

type ttlManager struct {
	state   ttlManagerState
	ch      chan struct{}
	lockCtx *kv.LockCtx
}

func (tm *ttlManager) run(c *twoPhaseCommitter, lockCtx *kv.LockCtx) {
	// Run only once.
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateUninitialized), uint32(stateRunning)) {
		return
	}
	tm.lockCtx = lockCtx
	go tm.keepAlive(c)
}

func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	close(tm.ch)
}

func (tm *ttlManager) keepAlive(c *twoPhaseCommitter) {
	// Ticker is set to 1/2 of the ManagedLockTTL.
	ticker := time.NewTicker(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond / 2)
	defer ticker.Stop()
	for {
		select {
		case <-tm.ch:
			return
		case <-ticker.C:
			// If kill signal is received, the ttlManager should exit.
			if tm.lockCtx != nil && tm.lockCtx.Killed != nil && atomic.LoadUint32(tm.lockCtx.Killed) != 0 {
				return
			}
			bo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, c.txn.vars)
<<<<<<< HEAD
			now, err := c.store.GetOracle().GetTimestamp(bo.ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
=======
			now, err := c.store.GetOracle().GetTimestamp(bo.ctx)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			if err != nil {
				err1 := bo.Backoff(BoPDRPC, err)
				if err1 != nil {
					logutil.Logger(bo.ctx).Warn("keepAlive get tso fail",
						zap.Error(err))
					return
				}
				continue
			}

			uptime := uint64(oracle.ExtractPhysical(now) - oracle.ExtractPhysical(c.startTS))
<<<<<<< HEAD
			if uptime > config.GetGlobalConfig().MaxTxnTTL {
=======
			if uptime > config.GetGlobalConfig().Performance.MaxTxnTTL {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
				// Checks maximum lifetime for the ttlManager, so when something goes wrong
				// the key will not be locked forever.
				logutil.Logger(bo.ctx).Info("ttlManager live up to its lifetime",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Uint64("uptime", uptime),
<<<<<<< HEAD
					zap.Uint64("maxTxnTTL", config.GetGlobalConfig().MaxTxnTTL))
=======
					zap.Uint64("maxTxnTTL", config.GetGlobalConfig().Performance.MaxTxnTTL))
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
				metrics.TiKVTTLLifeTimeReachCounter.Inc()
				// the pessimistic locks may expire if the ttl manager has timed out, set `LockExpired` flag
				// so that this transaction could only commit or rollback with no more statement executions
				if c.isPessimistic && tm.lockCtx != nil && tm.lockCtx.LockExpired != nil {
					atomic.StoreUint32(tm.lockCtx.LockExpired, 1)
				}
				return
			}

			newTTL := uptime + atomic.LoadUint64(&ManagedLockTTL)
			logutil.Logger(bo.ctx).Info("send TxnHeartBeat",
				zap.Uint64("startTS", c.startTS), zap.Uint64("newTTL", newTTL))
			startTime := time.Now()
			_, err = sendTxnHeartBeat(bo, c.store, c.primary(), c.startTS, newTTL)
			if err != nil {
				metrics.TxnHeartBeatHistogramError.Observe(time.Since(startTime).Seconds())
				logutil.Logger(bo.ctx).Warn("send TxnHeartBeat failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return
			}
			metrics.TxnHeartBeatHistogramOK.Observe(time.Since(startTime).Seconds())
		}
	}
}

<<<<<<< HEAD
func sendTxnHeartBeat(bo *Backoffer, store *KVStore, primary []byte, startTS, ttl uint64) (uint64, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdTxnHeartBeat, &pb.TxnHeartBeatRequest{
		PrimaryLock:   primary,
		StartVersion:  startTS,
		AdviseLockTtl: ttl,
	})
=======
func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	m := &batch.mutations
	mutations := make([]*pb.Mutation, m.len())
	for i := range m.keys {
		mut := &pb.Mutation{
			Op:  pb.Op_PessimisticLock,
			Key: m.keys[i],
		}
		existErr := c.txn.us.GetKeyExistErrInfo(m.keys[i])
		if existErr != nil || (c.doingAmend && m.GetOps()[i] == pb.Op_Insert) {
			mut.Assertion = pb.Assertion_NotExist
		}
		mutations[i] = mut
	}
	elapsed := uint64(time.Since(c.txn.startTime) / time.Millisecond)
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticLock, &pb.PessimisticLockRequest{
		Mutations:    mutations,
		PrimaryLock:  c.primary(),
		StartVersion: c.startTS,
		ForUpdateTs:  c.forUpdateTS,
		LockTtl:      elapsed + atomic.LoadUint64(&ManagedLockTTL),
		IsFirstLock:  c.isFirstLock,
		WaitTimeout:  action.LockWaitTime,
		ReturnValues: action.ReturnValues,
		MinCommitTs:  c.forUpdateTS + 1,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
	lockWaitStartTime := action.WaitStartTime
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	for {
		loc, err := store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return 0, errors.Trace(err)
		}
<<<<<<< HEAD
		resp, err := store.SendReq(bo, req, loc.Region, readTimeoutShort)
=======
		failpoint.Inject("PessimisticLockErrWriteConflict", func() error {
			time.Sleep(300 * time.Millisecond)
			return kv.ErrWriteConflict
		})
		startTime := time.Now()
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCTime, int64(time.Since(startTime)))
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCCount, 1)
		}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if err != nil {
			return 0, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
<<<<<<< HEAD
			return 0, errors.Trace(ErrBodyMissing)
=======
			return errors.Trace(ErrBodyMissing)
		}
		lockResp := resp.Resp.(*pb.PessimisticLockResponse)
		keyErrs := lockResp.GetErrors()
		if len(keyErrs) == 0 {
			if action.ReturnValues {
				action.ValuesLock.Lock()
				for i, mutation := range mutations {
					action.Values[string(mutation.Key)] = kv.ReturnedValue{Value: lockResp.Values[i]}
				}
				action.ValuesLock.Unlock()
			}
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				existErrInfo := c.txn.us.GetKeyExistErrInfo(key)
				if existErrInfo == nil {
					return errors.Errorf("conn %d, existErr for key:%s should not be nil", c.connID, key)
				}
				return existErrInfo.Err()
			}
			if deadlock := keyErr.Deadlock; deadlock != nil {
				return &ErrDeadlock{Deadlock: deadlock}
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			locks = append(locks, lock)
		}
		// Because we already waited on tikv, no need to Backoff here.
		// tikv default will wait 3s(also the maximum wait value) when lock error occurs
		startTime = time.Now()
		msBeforeTxnExpired, _, err := c.store.lockResolver.ResolveLocks(bo, 0, locks)
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.ResolveLockTime, int64(time.Since(startTime)))
		}
		if err != nil {
			return errors.Trace(err)
		}

		// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
		// the pessimistic lock. We should return acquire fail with nowait set or timeout error if necessary.
		if msBeforeTxnExpired > 0 {
			if action.LockWaitTime == kv.LockNoWait {
				return ErrLockAcquireFailAndNoWaitSet
			} else if action.LockWaitTime == kv.LockAlwaysWait {
				// do nothing but keep wait
			} else {
				// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
				if time.Since(lockWaitStartTime).Milliseconds() >= action.LockWaitTime {
					return errors.Trace(ErrLockWaitTimeout)
				}
			}
			if action.LockCtx.PessimisticLockWaited != nil {
				atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
			}
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		}
		cmdResp := resp.Resp.(*pb.TxnHeartBeatResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return 0, errors.Errorf("txn %d heartbeat fail, primary key = %v, err = %s", startTS, hex.EncodeToString(primary), extractKeyErr(keyErr))
		}
		return cmdResp.GetLockTtl(), nil
	}
}

// checkAsyncCommit checks if async commit protocol is available for current transaction commit, true is returned if possible.
func (c *twoPhaseCommitter) checkAsyncCommit() bool {
	// Disable async commit in local transactions
	txnScopeOption := c.txn.us.GetOption(kv.TxnScope)
	if txnScopeOption == nil || txnScopeOption.(string) != oracle.GlobalTxnScope {
		return false
	}

	enableAsyncCommitOption := c.txn.us.GetOption(kv.EnableAsyncCommit)
	enableAsyncCommit := enableAsyncCommitOption != nil && enableAsyncCommitOption.(bool)
	asyncCommitCfg := config.GetGlobalConfig().TiKVClient.AsyncCommit
	// TODO the keys limit need more tests, this value makes the unit test pass by now.
	// Async commit is not compatible with Binlog because of the non unique timestamp issue.
	if c.sessionID > 0 && enableAsyncCommit &&
		uint(c.mutations.Len()) <= asyncCommitCfg.KeysLimit &&
		!c.shouldWriteBinlog() {
		totalKeySize := uint64(0)
		for i := 0; i < c.mutations.Len(); i++ {
			totalKeySize += uint64(len(c.mutations.GetKey(i)))
			if totalKeySize > asyncCommitCfg.TotalKeySizeLimit {
				return false
			}
		}
		return true
	}
	return false
}

// checkOnePC checks if 1PC protocol is available for current transaction.
func (c *twoPhaseCommitter) checkOnePC() bool {
	// Disable 1PC in local transactions
	txnScopeOption := c.txn.us.GetOption(kv.TxnScope)
	if txnScopeOption == nil || txnScopeOption.(string) != oracle.GlobalTxnScope {
		return false
	}

	enable1PCOption := c.txn.us.GetOption(kv.Enable1PC)
	return c.sessionID > 0 && !c.shouldWriteBinlog() && enable1PCOption != nil && enable1PCOption.(bool)
}

func (c *twoPhaseCommitter) needLinearizability() bool {
	GuaranteeLinearizabilityOption := c.txn.us.GetOption(kv.GuaranteeLinearizability)
	// by default, guarantee
	return GuaranteeLinearizabilityOption == nil || GuaranteeLinearizabilityOption.(bool)
}

func (c *twoPhaseCommitter) isAsyncCommit() bool {
	return atomic.LoadUint32(&c.useAsyncCommit) > 0
}

<<<<<<< HEAD
func (c *twoPhaseCommitter) setAsyncCommit(val bool) {
	if val {
		atomic.StoreUint32(&c.useAsyncCommit, 1)
	} else {
		atomic.StoreUint32(&c.useAsyncCommit, 0)
=======
func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &pb.CommitRequest{
		StartVersion:  c.startTS,
		Keys:          batch.mutations.keys,
		CommitVersion: c.commitTS,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})

	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	if batch.isPrimary && sender.rpcError != nil {
		c.setUndeterminedErr(errors.Trace(sender.rpcError))
	}

	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// re-split keys and commit again.
		err = c.doActionOnMutations(bo, actionCommit{retry: true}, batch.mutations)
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	commitResp := resp.Resp.(*pb.CommitResponse)
	// Here we can make sure tikv has processed the commit primary key request. So
	// we can clean undetermined error.
	if batch.isPrimary {
		c.setUndeterminedErr(nil)
	}
	if keyErr := commitResp.GetError(); keyErr != nil {
		if rejected := keyErr.GetCommitTsExpired(); rejected != nil {
			logutil.Logger(bo.ctx).Info("2PC commitTS rejected by TiKV, retry with a newer commitTS",
				zap.Uint64("txnStartTS", c.startTS),
				zap.Stringer("info", logutil.Hex(rejected)))

			// Do not retry for a txn which has a too large MinCommitTs
			// 3600000 << 18 = 943718400000
			if rejected.MinCommitTs-rejected.AttemptedCommitTs > 943718400000 {
				err := errors.Errorf("2PC MinCommitTS is too large, we got MinCommitTS: %d, and AttemptedCommitTS: %d",
					rejected.MinCommitTs, rejected.AttemptedCommitTs)
				return errors.Trace(err)
			}

			// Update commit ts and retry.
			commitTS, err := c.store.getTimestampWithRetry(bo)
			if err != nil {
				logutil.Logger(bo.ctx).Warn("2PC get commitTS failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return errors.Trace(err)
			}

			c.mu.Lock()
			c.commitTS = commitTS
			c.mu.Unlock()
			return c.commitMutations(bo, batch.mutations)
		}

		c.mu.RLock()
		defer c.mu.RUnlock()
		err = extractKeyErr(keyErr)
		if c.mu.committed {
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			hexBatchKeys := func(keys [][]byte) []string {
				var res []string
				for _, k := range keys {
					res = append(res, hex.EncodeToString(k))
				}
				return res
			}
			logutil.Logger(bo.ctx).Error("2PC failed commit key after primary key committed",
				zap.Error(err),
				zap.Stringer("primaryKey", kv.Key(c.primaryKey)),
				zap.Uint64("txnStartTS", c.startTS),
				zap.Uint64("commitTS", c.commitTS),
				zap.Uint64("forUpdateTS", c.forUpdateTS),
				zap.Strings("keys", hexBatchKeys(batch.mutations.keys)))
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		logutil.Logger(bo.ctx).Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
}

func (c *twoPhaseCommitter) isOnePC() bool {
	return atomic.LoadUint32(&c.useOnePC) > 0
}

<<<<<<< HEAD
func (c *twoPhaseCommitter) setOnePC(val bool) {
	if val {
		atomic.StoreUint32(&c.useOnePC, 1)
	} else {
		atomic.StoreUint32(&c.useOnePC, 0)
=======
func (c *twoPhaseCommitter) prewriteMutations(bo *Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.prewriteMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
}

<<<<<<< HEAD
func (c *twoPhaseCommitter) checkOnePCFallBack(action twoPhaseCommitAction, batchCount int) {
	if _, ok := action.(actionPrewrite); ok {
		if batchCount > 1 {
			c.setOnePC(false)
		}
	}
}

func (c *twoPhaseCommitter) cleanup(ctx context.Context) {
	c.cleanWg.Add(1)
	go func() {
		failpoint.Inject("commitFailedSkipCleanup", func() {
			logutil.Logger(ctx).Info("[failpoint] injected skip cleanup secondaries on failure",
				zap.Uint64("txnStartTS", c.startTS))
			c.cleanWg.Done()
			failpoint.Return()
		})

		cleanupKeysCtx := context.WithValue(context.Background(), TxnStartKey, ctx.Value(TxnStartKey))
		err := c.cleanupMutations(NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
		if err != nil {
			metrics.SecondaryLockCleanupFailureCounterRollback.Inc()
			logutil.Logger(ctx).Info("2PC cleanup failed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
		} else {
			logutil.Logger(ctx).Info("2PC clean up done",
				zap.Uint64("txnStartTS", c.startTS))
		}
		c.cleanWg.Done()
	}()
=======
func (c *twoPhaseCommitter) commitMutations(bo *Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.commitMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doActionOnMutations(bo, actionCommit{}, mutations)
}

func (c *twoPhaseCommitter) cleanupMutations(bo *Backoffer, mutations CommitterMutations) error {
	return c.doActionOnMutations(bo, actionCleanup{}, mutations)
}

func (c *twoPhaseCommitter) pessimisticLockMutations(bo *Backoffer, lockCtx *kv.LockCtx, mutations CommitterMutations) error {
	if c.connID > 0 {
		failpoint.Inject("beforePessimisticLock", func(val failpoint.Value) {
			// Pass multiple instructions in one string, delimited by commas, to trigger multiple behaviors, like
			// `return("delay,fail")`. Then they will be executed sequentially at once.
			if v, ok := val.(string); ok {
				for _, action := range strings.Split(v, ",") {
					if action == "delay" {
						duration := time.Duration(rand.Int63n(int64(time.Second) * 5))
						logutil.Logger(bo.ctx).Info("[failpoint] injected delay at pessimistic lock",
							zap.Uint64("txnStartTS", c.startTS), zap.Duration("duration", duration))
						time.Sleep(duration)
					} else if action == "fail" {
						logutil.Logger(bo.ctx).Info("[failpoint] injected failure at pessimistic lock",
							zap.Uint64("txnStartTS", c.startTS))
						failpoint.Return(errors.New("injected failure at pessimistic lock"))
					}
				}
			}
		})
	}
	return c.doActionOnMutations(bo, actionPessimisticLock{lockCtx}, mutations)
}

func (c *twoPhaseCommitter) pessimisticRollbackMutations(bo *Backoffer, mutations CommitterMutations) error {
	return c.doActionOnMutations(bo, actionPessimisticRollback{}, mutations)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	var binlogSkipped bool
	defer func() {
<<<<<<< HEAD
		if c.isOnePC() {
			// The error means the 1PC transaction failed.
			if err != nil {
				metrics.OnePCTxnCounterError.Inc()
			} else {
				metrics.OnePCTxnCounterOk.Inc()
			}
		} else if c.isAsyncCommit() {
			// The error means the async commit should not succeed.
			if err != nil {
				if c.getUndeterminedErr() == nil {
					c.cleanup(ctx)
=======
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			c.cleanWg.Add(1)
			go func() {
				failpoint.Inject("commitFailedSkipCleanup", func() {
					logutil.Logger(ctx).Info("[failpoint] injected skip cleanup secondaries on failure",
						zap.Uint64("txnStartTS", c.startTS))
					c.cleanWg.Done()
					failpoint.Return()
				})

				cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
				err := c.cleanupMutations(NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
				if err != nil {
					tikvSecondaryLockCleanupFailureCounterRollback.Inc()
					logutil.Logger(ctx).Info("2PC cleanup failed",
						zap.Error(err),
						zap.Uint64("txnStartTS", c.startTS))
				} else {
					logutil.Logger(ctx).Info("2PC clean up done",
						zap.Uint64("txnStartTS", c.startTS))
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
				}
				metrics.AsyncCommitTxnCounterError.Inc()
			} else {
				metrics.AsyncCommitTxnCounterOk.Inc()
			}
		} else {
			// Always clean up all written keys if the txn does not commit.
			c.mu.RLock()
			committed := c.mu.committed
			undetermined := c.mu.undeterminedErr != nil
			c.mu.RUnlock()
			if !committed && !undetermined {
				c.cleanup(ctx)
			}
			c.txn.commitTS = c.commitTS
			if binlogSkipped {
				c.binlog.Skip()
				return
			}
			if !c.shouldWriteBinlog() {
				return
			}
			if err != nil {
				c.binlog.Commit(ctx, 0)
			} else {
				c.binlog.Commit(ctx, int64(c.commitTS))
			}
		}
	}()

<<<<<<< HEAD
	commitTSMayBeCalculated := false
	// Check async commit is available or not.
	if c.checkAsyncCommit() {
		commitTSMayBeCalculated = true
		c.setAsyncCommit(true)
	}
	// Check if 1PC is enabled.
	if c.checkOnePC() {
		commitTSMayBeCalculated = true
		c.setOnePC(true)
	}
	// If we want to use async commit or 1PC and also want linearizability across
	// all nodes, we have to make sure the commit TS of this transaction is greater
	// than the snapshot TS of all existent readers. So we get a new timestamp
	// from PD as our MinCommitTS.
	if commitTSMayBeCalculated && c.needLinearizability() {
		failpoint.Inject("getMinCommitTSFromTSO", nil)
		minCommitTS, err := c.store.oracle.GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		// If we fail to get a timestamp from PD, we just propagate the failure
		// instead of falling back to the normal 2PC because a normal 2PC will
		// also be likely to fail due to the same timestamp issue.
		if err != nil {
			return errors.Trace(err)
		}
		c.minCommitTS = minCommitTS
	}
	// Calculate maxCommitTS if necessary
	if commitTSMayBeCalculated {
		if err = c.calculateMaxCommitTS(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	failpoint.Inject("beforePrewrite", nil)

	c.prewriteStarted = true
	var binlogChan <-chan BinlogWriteResult
	if c.shouldWriteBinlog() {
		binlogChan = c.binlog.Prewrite(ctx, c.primary())
	}
=======
	binlogChan := c.prewriteBinlog(ctx)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
	start := time.Now()
	err = c.prewriteMutations(prewriteBo, c.mutations)

	if err != nil {
		// TODO: Now we return an undetermined error as long as one of the prewrite
		// RPCs fails. However, if there are multiple errors and some of the errors
		// are not RPC failures, we can return the actual error instead of undetermined.
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(terror.ErrResultUndetermined)
		}
	}

	commitDetail := c.getDetail()
	commitDetail.PrewriteTime = time.Since(start)
	if prewriteBo.totalSleep > 0 {
		atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(prewriteBo.totalSleep)*int64(time.Millisecond))
		commitDetail.Mu.Lock()
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, prewriteBo.types...)
		commitDetail.Mu.Unlock()
	}
	if binlogChan != nil {
		startWaitBinlog := time.Now()
		binlogWriteResult := <-binlogChan
		commitDetail.WaitPrewriteBinlogTime = time.Since(startWaitBinlog)
		if binlogWriteResult != nil {
			binlogSkipped = binlogWriteResult.Skipped()
			binlogErr := binlogWriteResult.GetError()
			if binlogErr != nil {
				return binlogErr
			}
		}
	}
	if err != nil {
		logutil.Logger(ctx).Debug("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// strip check_not_exists keys that no need to commit.
	c.stripNoNeedCommitKeys()

<<<<<<< HEAD
	var commitTS uint64

	if c.isOnePC() {
		if c.onePCCommitTS == 0 {
			err = errors.Errorf("session %d invalid onePCCommitTS for 1PC protocol after prewrite, startTS=%v", c.sessionID, c.startTS)
			return errors.Trace(err)
		}
		c.commitTS = c.onePCCommitTS
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Info("1PC protocol is used to commit this txn",
			zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
			zap.Uint64("session", c.sessionID))
		return nil
=======
	start = time.Now()
	logutil.Event(ctx, "start get commit ts")
	commitTS, err := c.store.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}

<<<<<<< HEAD
	if c.onePCCommitTS != 0 {
		logutil.Logger(ctx).Fatal("non 1PC transaction committed in 1PC",
			zap.Uint64("session", c.sessionID), zap.Uint64("startTS", c.startTS))
	}

	if c.isAsyncCommit() {
		if c.minCommitTS == 0 {
			err = errors.Errorf("session %d invalid minCommitTS for async commit protocol after prewrite, startTS=%v", c.sessionID, c.startTS)
			return errors.Trace(err)
		}
		commitTS = c.minCommitTS
	} else {
		start = time.Now()
		logutil.Event(ctx, "start get commit ts")
		commitTS, err = c.store.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars), c.txn.GetUnionStore().GetOption(kv.TxnScope).(string))
		if err != nil {
			logutil.Logger(ctx).Warn("2PC get commitTS failed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		commitDetail.GetCommitTsTime = time.Since(start)
		logutil.Event(ctx, "finish get commit ts")
		logutil.SetTag(ctx, "commitTs", commitTS)
	}

	if c.sessionID > 0 {
		failpoint.Inject("beforeSchemaCheck", func() {
			c.ttlManager.close()
			failpoint.Return()
		})
	}
=======
	tryAmend := c.isPessimistic && c.connID > 0
	if !tryAmend {
		_, _, err = c.checkSchemaValid(ctx, commitTS, c.txn.txnInfoSchema, false)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		relatedSchemaChange, memAmended, err := c.checkSchemaValid(ctx, commitTS, c.txn.txnInfoSchema, true)
		if err != nil {
			return errors.Trace(err)
		}
		if memAmended {
			// Get new commitTS and check schema valid again.
			newCommitTS, err := c.getCommitTS(ctx, commitDetail)
			if err != nil {
				return errors.Trace(err)
			}
			// If schema check failed between commitTS and newCommitTs, report schema change error.
			_, _, err = c.checkSchemaValid(ctx, newCommitTS, relatedSchemaChange.LatestInfoSchema, false)
			if err != nil {
				logutil.Logger(ctx).Info("schema check after amend failed, it means the schema version changed again",
					zap.Uint64("startTS", c.startTS),
					zap.Uint64("amendTS", c.commitTS),
					zap.Int64("amendedSchemaVersion", relatedSchemaChange.LatestInfoSchema.SchemaMetaVersion()),
					zap.Uint64("newCommitTS", newCommitTS))
				return errors.Trace(err)
			}
			commitTS = newCommitTS
		}
	}
	c.commitTS = commitTS
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

	if !c.isAsyncCommit() {
		tryAmend := c.isPessimistic && c.sessionID > 0 && c.txn.schemaAmender != nil
		if !tryAmend {
			_, _, err = c.checkSchemaValid(ctx, commitTS, c.txn.txnInfoSchema, false)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			relatedSchemaChange, memAmended, err := c.checkSchemaValid(ctx, commitTS, c.txn.txnInfoSchema, true)
			if err != nil {
				return errors.Trace(err)
			}
			if memAmended {
				// Get new commitTS and check schema valid again.
				newCommitTS, err := c.getCommitTS(ctx, commitDetail)
				if err != nil {
					return errors.Trace(err)
				}
				// If schema check failed between commitTS and newCommitTs, report schema change error.
				_, _, err = c.checkSchemaValid(ctx, newCommitTS, relatedSchemaChange.LatestInfoSchema, false)
				if err != nil {
					logutil.Logger(ctx).Info("schema check after amend failed, it means the schema version changed again",
						zap.Uint64("startTS", c.startTS),
						zap.Uint64("amendTS", c.commitTS),
						zap.Int64("amendedSchemaVersion", relatedSchemaChange.LatestInfoSchema.SchemaMetaVersion()),
						zap.Uint64("newCommitTS", newCommitTS))
					return errors.Trace(err)
				}
				commitTS = newCommitTS
			}
		}
	}
	c.commitTS = commitTS

	if c.store.oracle.IsExpired(c.startTS, kv.MaxTxnTimeUse, &oracle.Option{TxnScope: oracle.GlobalTxnScope}) {
		err = errors.Errorf("session %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.sessionID, c.startTS, c.commitTS)
		return err
	}

<<<<<<< HEAD
	if c.sessionID > 0 {
=======
	if c.connID > 0 {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		failpoint.Inject("beforeCommit", func(val failpoint.Value) {
			// Pass multiple instructions in one string, delimited by commas, to trigger multiple behaviors, like
			// `return("delay,fail")`. Then they will be executed sequentially at once.
			if v, ok := val.(string); ok {
				for _, action := range strings.Split(v, ",") {
					// Async commit transactions cannot return error here, since it's already successful.
<<<<<<< HEAD
					if action == "fail" && !c.isAsyncCommit() {
=======
					if action == "fail" {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
						logutil.Logger(ctx).Info("[failpoint] injected failure before commit", zap.Uint64("txnStartTS", c.startTS))
						failpoint.Return(errors.New("injected failure before commit"))
					} else if action == "delay" {
						duration := time.Duration(rand.Int63n(int64(time.Second) * 5))
						logutil.Logger(ctx).Info("[failpoint] injected delay before commit",
							zap.Uint64("txnStartTS", c.startTS), zap.Duration("duration", duration))
						time.Sleep(duration)
					}
				}
			}
		})
	}

<<<<<<< HEAD
	if c.isAsyncCommit() {
		// For async commit protocol, the commit is considered success here.
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Debug("2PC will use async commit protocol to commit this txn",
			zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
			zap.Uint64("sessionID", c.sessionID))
		go func() {
			defer c.ttlManager.close()
			failpoint.Inject("asyncCommitDoNothing", func() {
				failpoint.Return()
			})
			commitBo := NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
			err := c.commitMutations(commitBo, c.mutations)
			if err != nil {
				logutil.Logger(ctx).Warn("2PC async commit failed", zap.Uint64("sessionID", c.sessionID),
					zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS), zap.Error(err))
			}
		}()
		return nil
	}
	return c.commitTxn(ctx, commitDetail)
}

func (c *twoPhaseCommitter) commitTxn(ctx context.Context, commitDetail *execdetails.CommitDetails) error {
	c.txn.GetMemBuffer().DiscardValues()
	start := time.Now()

	commitBo := NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
	err := c.commitMutations(commitBo, c.mutations)
=======
	start = time.Now()
	commitBo := NewBackofferWithVars(ctx, CommitMaxBackoff, c.txn.vars)
	err = c.commitMutations(commitBo, c.mutations)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	commitDetail.CommitTime = time.Since(start)
	if commitBo.totalSleep > 0 {
		atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(commitBo.totalSleep)*int64(time.Millisecond))
		commitDetail.Mu.Lock()
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, commitBo.types...)
		commitDetail.Mu.Unlock()
	}
	if err != nil {
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.Trace(terror.ErrResultUndetermined)
		}
		if !c.mu.committed {
			logutil.Logger(ctx).Debug("2PC failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

func (c *twoPhaseCommitter) stripNoNeedCommitKeys() {
	if !c.hasNoNeedCommitKeys {
		return
	}
	m := c.mutations
	var newIdx int
	for oldIdx := range m.handles {
		key := m.GetKey(oldIdx)
		flags, err := c.txn.GetMemBuffer().GetFlags(key)
		if err == nil && flags.HasPrewriteOnly() {
			continue
		}
		m.handles[newIdx] = m.handles[oldIdx]
		newIdx++
	}
	c.mutations.handles = c.mutations.handles[:newIdx]
}

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer interface {
	// SchemaMetaVersion returns the meta schema version.
	SchemaMetaVersion() int64
}

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer interface {
	// SchemaMetaVersion returns the meta schema version.
	SchemaMetaVersion() int64
}

type schemaLeaseChecker interface {
	// CheckBySchemaVer checks if the schema has changed for the transaction related tables between the startSchemaVer
	// and the schema version at txnTS, all the related schema changes will be returned.
	CheckBySchemaVer(txnTS uint64, startSchemaVer SchemaVer) (*RelatedSchemaChange, error)
}

// RelatedSchemaChange contains information about schema diff between two schema versions.
type RelatedSchemaChange struct {
	PhyTblIDS        []int64
	ActionTypes      []uint64
	LatestInfoSchema SchemaVer
	Amendable        bool
}

<<<<<<< HEAD
func (c *twoPhaseCommitter) amendPessimisticLock(ctx context.Context, addMutations CommitterMutations) error {
	keysNeedToLock := NewPlainMutations(addMutations.Len())
	for i := 0; i < addMutations.Len(); i++ {
		if addMutations.IsPessimisticLock(i) {
			keysNeedToLock.Push(addMutations.GetOp(i), addMutations.GetKey(i), addMutations.GetValue(i), addMutations.IsPessimisticLock(i))
		}
	}
	// For unique index amend, we need to pessimistic lock the generated new index keys first.
	// Set doingAmend to true to force the pessimistic lock do the exist check for these keys.
	c.doingAmend = true
	defer func() { c.doingAmend = false }()
	if keysNeedToLock.Len() > 0 {
		lCtx := &kv.LockCtx{
			Killed:        c.lockCtx.Killed,
			ForUpdateTS:   c.forUpdateTS,
			LockWaitTime:  c.lockCtx.LockWaitTime,
			WaitStartTime: time.Now(),
		}
		tryTimes := uint(0)
		retryLimit := config.GetGlobalConfig().PessimisticTxn.MaxRetryCount
		var err error
		for tryTimes < retryLimit {
			pessimisticLockBo := NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, c.txn.vars)
			err = c.pessimisticLockMutations(pessimisticLockBo, lCtx, &keysNeedToLock)
			if err != nil {
				// KeysNeedToLock won't change, so don't async rollback pessimistic locks here for write conflict.
				if terror.ErrorEqual(kv.ErrWriteConflict, err) {
					newForUpdateTSVer, err := c.store.CurrentVersion(oracle.GlobalTxnScope)
					if err != nil {
						return errors.Trace(err)
					}
					lCtx.ForUpdateTS = newForUpdateTSVer.Ver
					c.forUpdateTS = newForUpdateTSVer.Ver
					logutil.Logger(ctx).Info("amend pessimistic lock pessimistic retry lock",
						zap.Uint("tryTimes", tryTimes), zap.Uint64("startTS", c.startTS),
						zap.Uint64("newForUpdateTS", c.forUpdateTS))
					tryTimes++
					continue
				}
				logutil.Logger(ctx).Warn("amend pessimistic lock has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
				return err
			}
			logutil.Logger(ctx).Info("amend pessimistic lock finished", zap.Uint64("startTS", c.startTS),
				zap.Uint64("forUpdateTS", c.forUpdateTS), zap.Int("keys", keysNeedToLock.Len()))
			break
		}
		if err != nil {
			logutil.Logger(ctx).Warn("amend pessimistic lock failed after retry",
				zap.Uint("tryTimes", tryTimes), zap.Uint64("startTS", c.startTS))
			return err
=======
func (c *twoPhaseCommitter) tryAmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange) (bool, error) {
	addMutations, err := c.txn.schemaAmender.AmendTxn(ctx, startInfoSchema, change, c.mutations)
	if err != nil {
		return false, err
	}
	// Prewrite new mutations.
	if addMutations != nil && len(addMutations.keys) > 0 {
		var keysNeedToLock CommitterMutations
		for i := 0; i < addMutations.len(); i++ {
			if addMutations.isPessimisticLock[i] {
				keysNeedToLock.Push(addMutations.ops[i], addMutations.keys[i], addMutations.values[i], addMutations.isPessimisticLock[i])
			}
		}
		// For unique index amend, we need to pessimistic lock the generated new index keys first.
		// Set doingAmend to true to force the pessimistic lock do the exist check for these keys.
		c.doingAmend = true
		defer func() { c.doingAmend = false }()
		if keysNeedToLock.len() > 0 {
			lCtx := &kv.LockCtx{
				Killed:        c.lockCtx.Killed,
				ForUpdateTS:   c.forUpdateTS,
				LockWaitTime:  c.lockCtx.LockWaitTime,
				WaitStartTime: time.Now(),
			}
			tryTimes := uint(0)
			retryLimit := config.GetGlobalConfig().PessimisticTxn.MaxRetryCount
			for tryTimes < retryLimit {
				pessimisticLockBo := NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, c.txn.vars)
				err = c.pessimisticLockMutations(pessimisticLockBo, lCtx, keysNeedToLock)
				if err != nil {
					// KeysNeedToLock won't change, so don't async rollback pessimistic locks here for write conflict.
					if terror.ErrorEqual(kv.ErrWriteConflict, err) {
						newForUpdateTSVer, err := c.store.CurrentVersion()
						if err != nil {
							return false, errors.Trace(err)
						}
						lCtx.ForUpdateTS = newForUpdateTSVer.Ver
						c.forUpdateTS = newForUpdateTSVer.Ver
						logutil.Logger(ctx).Info("amend pessimistic lock pessimistic retry lock",
							zap.Uint("tryTimes", tryTimes), zap.Uint64("startTS", c.startTS),
							zap.Uint64("newForUpdateTS", c.forUpdateTS))
						tryTimes++
						continue
					}
					logutil.Logger(ctx).Warn("amend pessimistic lock has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
					return false, err
				}
				logutil.Logger(ctx).Info("amend pessimistic lock finished", zap.Uint64("startTS", c.startTS),
					zap.Uint64("forUpdateTS", c.forUpdateTS), zap.Int("keys", keysNeedToLock.len()))
				break
			}
			if err != nil {
				logutil.Logger(ctx).Warn("amend pessimistic lock failed after retry",
					zap.Uint("tryTimes", tryTimes), zap.Uint64("startTS", c.startTS))
				return false, err
			}
		}
		prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
		err = c.prewriteMutations(prewriteBo, *addMutations)
		if err != nil {
			logutil.Logger(ctx).Warn("amend prewrite has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
			return false, err
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		}
		// Commit the amended secondary keys in the commit phase.
		c.mutations.MergeMutations(*addMutations)
		logutil.Logger(ctx).Info("amend prewrite finished", zap.Uint64("txnStartTS", c.startTS))
		return true, nil
	}
	return false, nil
}

func (c *twoPhaseCommitter) getCommitTS(ctx context.Context, commitDetail *execdetails.CommitDetails) (uint64, error) {
	start := time.Now()
	logutil.Event(ctx, "start get commit ts")
	commitTS, err := c.store.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return 0, errors.Trace(err)
	}
	commitDetail.GetCommitTsTime = time.Since(start)
	logutil.Event(ctx, "finish get commit ts")
	logutil.SetTag(ctx, "commitTS", commitTS)

	// Check commitTS.
	if commitTS <= c.startTS {
		err = errors.Errorf("conn %d invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return 0, errors.Trace(err)
	}
	return commitTS, nil
}

// checkSchemaValid checks if the schema has changed, if tryAmend is set to true, committer will try to amend
// this transaction using the related schema changes.
func (c *twoPhaseCommitter) checkSchemaValid(ctx context.Context, checkTS uint64, startInfoSchema SchemaVer,
	tryAmend bool) (*RelatedSchemaChange, bool, error) {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if !ok {
		if c.connID > 0 {
			logutil.Logger(ctx).Warn("schemaLeaseChecker is not set for this transaction, schema check skipped",
				zap.Uint64("connID", c.connID), zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", checkTS))
		}
		return nil, false, nil
	}
	relatedChanges, err := checker.CheckBySchemaVer(checkTS, startInfoSchema)
	if err != nil {
		if tryAmend && relatedChanges != nil && relatedChanges.Amendable && c.txn.schemaAmender != nil {
			memAmended, amendErr := c.tryAmendTxn(ctx, startInfoSchema, relatedChanges)
			if amendErr != nil {
				logutil.BgLogger().Info("txn amend has failed", zap.Uint64("connID", c.connID),
					zap.Uint64("startTS", c.startTS), zap.Error(amendErr))
				return nil, false, err
			}
			logutil.Logger(ctx).Info("amend txn successfully for pessimistic commit",
				zap.Uint64("connID", c.connID), zap.Uint64("txn startTS", c.startTS), zap.Bool("memAmended", memAmended),
				zap.Uint64("checkTS", checkTS), zap.Int64("startInfoSchemaVer", startInfoSchema.SchemaMetaVersion()),
				zap.Int64s("table ids", relatedChanges.PhyTblIDS), zap.Uint64s("action types", relatedChanges.ActionTypes))
			return relatedChanges, memAmended, nil
		}
		return nil, false, errors.Trace(err)
	}
	return nil, false, nil
}

func (c *twoPhaseCommitter) tryAmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange) (bool, error) {
	addMutations, err := c.txn.schemaAmender.AmendTxn(ctx, startInfoSchema, change, c.mutations)
	if err != nil {
		return false, err
	}
	// Add new mutations to the mutation list or prewrite them if prewrite already starts.
	if addMutations != nil && addMutations.Len() > 0 {
		err = c.amendPessimisticLock(ctx, addMutations)
		if err != nil {
			logutil.Logger(ctx).Info("amendPessimisticLock has failed", zap.Error(err))
			return false, err
		}
		if c.prewriteStarted {
			prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
			err = c.prewriteMutations(prewriteBo, addMutations)
			if err != nil {
				logutil.Logger(ctx).Warn("amend prewrite has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
				return false, err
			}
			logutil.Logger(ctx).Info("amend prewrite finished", zap.Uint64("txnStartTS", c.startTS))
			return true, nil
		}
		memBuf := c.txn.GetMemBuffer()
		for i := 0; i < addMutations.Len(); i++ {
			key := addMutations.GetKey(i)
			op := addMutations.GetOp(i)
			var err error
			if op == pb.Op_Del {
				err = memBuf.Delete(key)
			} else {
				err = memBuf.Set(key, addMutations.GetValue(i))
			}
			if err != nil {
				logutil.Logger(ctx).Warn("amend mutations has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
				return false, err
			}
			handle := c.txn.GetMemBuffer().IterWithFlags(key, nil).Handle()
			c.mutations.Push(op, addMutations.IsPessimisticLock(i), handle)
		}
	}
	return false, nil
}

func (c *twoPhaseCommitter) getCommitTS(ctx context.Context, commitDetail *execdetails.CommitDetails) (uint64, error) {
	start := time.Now()
	logutil.Event(ctx, "start get commit ts")
	commitTS, err := c.store.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars), c.txn.GetUnionStore().GetOption(kv.TxnScope).(string))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return 0, errors.Trace(err)
	}
	commitDetail.GetCommitTsTime = time.Since(start)
	logutil.Event(ctx, "finish get commit ts")
	logutil.SetTag(ctx, "commitTS", commitTS)

	// Check commitTS.
	if commitTS <= c.startTS {
		err = errors.Errorf("session %d invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.sessionID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return 0, errors.Trace(err)
	}
	return commitTS, nil
}

// checkSchemaValid checks if the schema has changed, if tryAmend is set to true, committer will try to amend
// this transaction using the related schema changes.
func (c *twoPhaseCommitter) checkSchemaValid(ctx context.Context, checkTS uint64, startInfoSchema SchemaVer,
	tryAmend bool) (*RelatedSchemaChange, bool, error) {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if !ok {
		if c.sessionID > 0 {
			logutil.Logger(ctx).Warn("schemaLeaseChecker is not set for this transaction",
				zap.Uint64("sessionID", c.sessionID),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("commitTS", checkTS))
		}
		return nil, false, nil
	}
	relatedChanges, err := checker.CheckBySchemaVer(checkTS, startInfoSchema)
	if err != nil {
		if tryAmend && relatedChanges != nil && relatedChanges.Amendable && c.txn.schemaAmender != nil {
			memAmended, amendErr := c.tryAmendTxn(ctx, startInfoSchema, relatedChanges)
			if amendErr != nil {
				logutil.BgLogger().Info("txn amend has failed", zap.Uint64("sessionID", c.sessionID),
					zap.Uint64("startTS", c.startTS), zap.Error(amendErr))
				return nil, false, err
			}
			logutil.Logger(ctx).Info("amend txn successfully",
				zap.Uint64("sessionID", c.sessionID), zap.Uint64("txn startTS", c.startTS), zap.Bool("memAmended", memAmended),
				zap.Uint64("checkTS", checkTS), zap.Int64("startInfoSchemaVer", startInfoSchema.SchemaMetaVersion()),
				zap.Int64s("table ids", relatedChanges.PhyTblIDS), zap.Uint64s("action types", relatedChanges.ActionTypes))
			return relatedChanges, memAmended, nil
		}
		return nil, false, errors.Trace(err)
	}
	return nil, false, nil
}

func (c *twoPhaseCommitter) calculateMaxCommitTS(ctx context.Context) error {
	// Amend txn with current time first, then we can make sure we have another SafeWindow time to commit
	currentTS := oracle.EncodeTSO(int64(time.Since(c.txn.startTime)/time.Millisecond)) + c.startTS
	_, _, err := c.checkSchemaValid(ctx, currentTS, c.txn.txnInfoSchema, true)
	if err != nil {
		logutil.Logger(ctx).Error("Schema changed for async commit txn",
			zap.Error(err),
			zap.Uint64("startTS", c.startTS))
		return errors.Trace(err)
	}

	safeWindow := config.GetGlobalConfig().TiKVClient.AsyncCommit.SafeWindow
	maxCommitTS := oracle.EncodeTSO(int64(safeWindow/time.Millisecond)) + currentTS
	logutil.BgLogger().Debug("calculate MaxCommitTS",
		zap.Time("startTime", c.txn.startTime),
		zap.Duration("safeWindow", safeWindow),
		zap.Uint64("startTS", c.startTS),
		zap.Uint64("maxCommitTS", maxCommitTS))

	c.maxCommitTS = maxCommitTS
	return nil
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	return c.txn.us.GetOption(kv.BinlogInfo) != nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

type batchMutations struct {
	region    RegionVerID
	mutations CommitterMutations
	isPrimary bool
}
type batched struct {
	batches    []batchMutations
	primaryIdx int
	primaryKey []byte
}

func newBatched(primaryKey []byte) *batched {
	return &batched{
		primaryIdx: -1,
		primaryKey: primaryKey,
	}
}

// appendBatchMutationsBySize appends mutations to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
<<<<<<< HEAD
func (b *batched) appendBatchMutationsBySize(region RegionVerID, mutations CommitterMutations, sizeFn func(k, v []byte) int, limit int) {
=======
func (c *twoPhaseCommitter) appendBatchMutationsBySize(b []batchMutations, region RegionVerID, mutations CommitterMutations, sizeFn func(k, v []byte) int, limit int, primaryIdx *int) []batchMutations {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	failpoint.Inject("twoPCRequestBatchSizeLimit", func() {
		limit = 1
	})

	var start, end int
	for start = 0; start < mutations.Len(); start = end {
		var size int
		for end = start; end < mutations.Len() && size < limit; end++ {
			var k, v []byte
			k = mutations.GetKey(end)
			v = mutations.GetValue(end)
			size += sizeFn(k, v)
			if b.primaryIdx < 0 && bytes.Equal(k, b.primaryKey) {
				b.primaryIdx = len(b.batches)
			}
		}
		b.batches = append(b.batches, batchMutations{
			region:    region,
			mutations: mutations.Slice(start, end),
		})
	}
}

func (b *batched) setPrimary() bool {
	// If the batches include the primary key, put it to the first
	if b.primaryIdx >= 0 {
		if len(b.batches) > 0 {
			b.batches[b.primaryIdx].isPrimary = true
			b.batches[0], b.batches[b.primaryIdx] = b.batches[b.primaryIdx], b.batches[0]
			b.primaryIdx = 0
		}
		return true
	}

	return false
}

func (b *batched) allBatches() []batchMutations {
	return b.batches
}

// primaryBatch returns the batch containing the primary key.
// Precondition: `b.setPrimary() == true`
func (b *batched) primaryBatch() []batchMutations {
	return b.batches[:1]
}

func (b *batched) forgetPrimary() {
	if len(b.batches) == 0 {
		return
	}
	b.batches = b.batches[1:]
}

// batchExecutor is txn controller providing rate control like utils
type batchExecutor struct {
	rateLim           int                  // concurrent worker numbers
	rateLimiter       *util.RateLimit      // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter   // here maybe more different type committer in the future
	action            twoPhaseCommitAction // the work action type
	backoffer         *Backoffer           // Backoffer
	tokenWaitDuration time.Duration        // get token wait time
}

// newBatchExecutor create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchExecutor(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitAction, backoffer *Backoffer) *batchExecutor {
	return &batchExecutor{rateLimit, nil, committer,
		action, backoffer, 1 * time.Millisecond}
}

// initUtils do initialize batchExecutor related policies like rateLimit util
func (batchExe *batchExecutor) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchExe.rateLimiter = util.NewRateLimit(batchExe.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchExe *batchExecutor) startWorker(exitCh chan struct{}, ch chan error, batches []batchMutations) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchExe.rateLimiter.GetToken(exitCh); !exit {
			batchExe.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchExe.rateLimiter.PutToken()
				var singleBatchBackoffer *Backoffer
				if _, ok := batchExe.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchExe.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchExe.backoffer.Fork()
					defer singleBatchCancel()
				}
				beforeSleep := singleBatchBackoffer.totalSleep
				ch <- batchExe.action.handleSingleBatch(batchExe.committer, singleBatchBackoffer, batch)
				commitDetail := batchExe.committer.getDetail()
				if commitDetail != nil { // lock operations of pessimistic-txn will let commitDetail be nil
					if delta := singleBatchBackoffer.totalSleep - beforeSleep; delta > 0 {
						atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(singleBatchBackoffer.totalSleep-beforeSleep)*int64(time.Millisecond))
						commitDetail.Mu.Lock()
						commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, singleBatchBackoffer.types...)
						commitDetail.Mu.Unlock()
					}
				}
			}()
		} else {
			logutil.Logger(batchExe.backoffer.ctx).Info("break startWorker",
				zap.Stringer("action", batchExe.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchExe *batchExecutor) process(batches []batchMutations) error {
	var err error
	err = batchExe.initUtils()
	if err != nil {
		logutil.Logger(batchExe.backoffer.ctx).Error("batchExecutor initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	var cancel context.CancelFunc
	if _, ok := batchExe.action.(actionPrewrite); ok {
		batchExe.backoffer, cancel = batchExe.backoffer.Fork()
		defer cancel()
	}
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchExe.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(batchExe.backoffer.ctx).Debug("2PC doActionOnBatch failed",
<<<<<<< HEAD
				zap.Uint64("session", batchExe.committer.sessionID),
=======
				zap.Uint64("conn", batchExe.committer.connID),
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
				zap.Stringer("action type", batchExe.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchExe.committer.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(batchExe.backoffer.ctx).Debug("2PC doActionOnBatch to cancel other actions",
<<<<<<< HEAD
					zap.Uint64("session", batchExe.committer.sessionID),
=======
					zap.Uint64("conn", batchExe.committer.connID),
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
					zap.Stringer("action type", batchExe.action),
					zap.Uint64("txnStartTS", batchExe.committer.startTS))
				atomic.StoreUint32(&batchExe.committer.prewriteCancelled, 1)
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	close(exitCh)
	metrics.TiKVTokenWaitDuration.Observe(batchExe.tokenWaitDuration.Seconds())
	return err
}

func getTxnPriority(txn *tikvTxn) pb.CommandPri {
	if pri := txn.us.GetOption(kv.Priority); pri != nil {
		return PriorityToPB(pri.(int))
	}
	return pb.CommandPri_Normal
}

func getTxnSyncLog(txn *tikvTxn) bool {
	if syncOption := txn.us.GetOption(kv.SyncLog); syncOption != nil {
		return syncOption.(bool)
	}
	return false
}

// PriorityToPB converts priority type to wire type.
func PriorityToPB(pri int) pb.CommandPri {
	switch pri {
	case kv.PriorityLow:
		return pb.CommandPri_Low
	case kv.PriorityHigh:
		return pb.CommandPri_High
	default:
		return pb.CommandPri_Normal
	}
}

func (c *twoPhaseCommitter) setDetail(d *execdetails.CommitDetails) {
	atomic.StorePointer(&c.detail, unsafe.Pointer(d))
}

func (c *twoPhaseCommitter) getDetail() *execdetails.CommitDetails {
	return (*execdetails.CommitDetails)(atomic.LoadPointer(&c.detail))
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}
