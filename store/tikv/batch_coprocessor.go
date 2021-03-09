// Copyright 2020 PingCAP, Inc.
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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
<<<<<<< HEAD
	"github.com/pingcap/failpoint"
=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
<<<<<<< HEAD
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
=======
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// batchCopTask comprises of multiple copTask that will send to same store.
type batchCopTask struct {
	storeAddr string
	cmdType   tikvrpc.CmdType

	copTasks []copTaskAndRPCContext
}

type batchCopResponse struct {
	pbResp *coprocessor.BatchResponse
	detail *CopRuntimeStats

	// batch Cop Response is yet to return startKey. So batchCop cannot retry partially.
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

// GetData implements the kv.ResultSubset GetData interface.
func (rs *batchCopResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *batchCopResponse) GetStartKey() kv.Key {
	return rs.startKey
}

// GetExecDetails is unavailable currently, because TiFlash has not collected exec details for batch cop.
// TODO: Will fix in near future.
func (rs *batchCopResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *batchCopResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *batchCopResponse) RespTime() time.Duration {
	return rs.respTime
}

type copTaskAndRPCContext struct {
	task *copTask
	ctx  *RPCContext
}

<<<<<<< HEAD
func buildBatchCopTasks(bo *Backoffer, cache *RegionCache, ranges *KeyRanges, storeType kv.StoreType) ([]*batchCopTask, error) {
	start := time.Now()
	const cmdType = tikvrpc.CmdBatchCop
	rangesLen := ranges.Len()
	for {
		var tasks []*copTask
		appendTask := func(regionWithRangeInfo *KeyLocation, ranges *KeyRanges) {
=======
func buildBatchCopTasks(bo *Backoffer, cache *RegionCache, ranges *copRanges, req *kv.Request) ([]*batchCopTask, error) {
	start := time.Now()
	const cmdType = tikvrpc.CmdBatchCop
	rangesLen := ranges.len()
	for {
		var tasks []*copTask
		appendTask := func(regionWithRangeInfo *KeyLocation, ranges *copRanges) {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			tasks = append(tasks, &copTask{
				region:    regionWithRangeInfo.Region,
				ranges:    ranges,
				cmdType:   cmdType,
<<<<<<< HEAD
				storeType: storeType,
			})
		}

		err := SplitKeyRanges(bo, cache, ranges, appendTask)
=======
				storeType: req.StoreType,
			})
		}

		err := splitRanges(bo, cache, ranges, appendTask)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		if err != nil {
			return nil, errors.Trace(err)
		}

		var batchTasks []*batchCopTask

		storeTaskMap := make(map[string]*batchCopTask)
		needRetry := false
		for _, task := range tasks {
			rpcCtx, err := cache.GetTiFlashRPCContext(bo, task.region)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We should retry and generate new tasks.
			if rpcCtx == nil {
				needRetry = true
				logutil.BgLogger().Info("retry for TiFlash peer with region missing", zap.Uint64("region id", task.region.GetID()))
				// Probably all the regions are invalid. Make the loop continue and mark all the regions invalid.
				// Then `splitRegion` will reloads these regions.
				continue
			}
			if batchCop, ok := storeTaskMap[rpcCtx.Addr]; ok {
				batchCop.copTasks = append(batchCop.copTasks, copTaskAndRPCContext{task: task, ctx: rpcCtx})
			} else {
				batchTask := &batchCopTask{
					storeAddr: rpcCtx.Addr,
					cmdType:   cmdType,
					copTasks:  []copTaskAndRPCContext{{task, rpcCtx}},
				}
				storeTaskMap[rpcCtx.Addr] = batchTask
			}
		}
		if needRetry {
			// Backoff once for each retry.
			err = bo.Backoff(BoRegionMiss, errors.New("Cannot find region with TiFlash peer"))
<<<<<<< HEAD
			// Actually ErrRegionUnavailable would be thrown out rather than ErrTiFlashServerTimeout. However, since currently
			// we don't have MockTiFlash, we inject ErrTiFlashServerTimeout to simulate the situation that TiFlash is down.
			if storeType == kv.TiFlash {
				failpoint.Inject("errorMockTiFlashServerTimeout", func(val failpoint.Value) {
					if val.(bool) {
						failpoint.Return(nil, errors.Trace(ErrTiFlashServerTimeout))
					}
				})
			}
=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		for _, task := range storeTaskMap {
			batchTasks = append(batchTasks, task)
		}

		if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
			logutil.BgLogger().Warn("buildBatchCopTasks takes too much time",
				zap.Duration("elapsed", elapsed),
				zap.Int("range len", rangesLen),
				zap.Int("task len", len(batchTasks)))
		}
<<<<<<< HEAD
		metrics.TxnRegionsNumHistogramWithBatchCoprocessor.Observe(float64(len(batchTasks)))
=======
		tikvTxnRegionsNumHistogramWithBatchCoprocessor.Observe(float64(len(batchTasks)))
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		return batchTasks, nil
	}
}

func (c *CopClient) sendBatch(ctx context.Context, req *kv.Request, vars *kv.Variables) kv.Response {
	if req.KeepOrder || req.Desc {
		return copErrorResponse{errors.New("batch coprocessor cannot prove keep order or desc property")}
	}
<<<<<<< HEAD
	ctx = context.WithValue(ctx, TxnStartKey, req.StartTs)
	bo := NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, vars)
	tasks, err := buildBatchCopTasks(bo, c.store.GetRegionCache(), NewKeyRanges(req.KeyRanges), req.StoreType)
=======
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, vars)
	tasks, err := buildBatchCopTasks(bo, c.store.regionCache, &copRanges{mid: req.KeyRanges}, req)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	if err != nil {
		return copErrorResponse{err}
	}
	it := &batchCopIterator{
<<<<<<< HEAD
		store:        c.store,
		req:          req,
		finishCh:     make(chan struct{}),
		vars:         vars,
		memTracker:   req.MemTracker,
		ClientHelper: NewClientHelper(c.store, util.NewTSSet(5)),
		rpcCancel:    NewRPCanceller(),
=======
		store:      c.store,
		req:        req,
		finishCh:   make(chan struct{}),
		vars:       vars,
		memTracker: req.MemTracker,
		clientHelper: clientHelper{
			LockResolver:      c.store.lockResolver,
			RegionCache:       c.store.regionCache,
			Client:            c.store.client,
			minCommitTSPushed: &minCommitTSPushed{data: make(map[uint64]struct{}, 5)},
		},
		rpcCancel: NewRPCanceller(),
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
	ctx = context.WithValue(ctx, RPCCancellerCtxKey{}, it.rpcCancel)
	it.tasks = tasks
	it.respChan = make(chan *batchCopResponse, 2048)
	go it.run(ctx)
	return it
}

type batchCopIterator struct {
<<<<<<< HEAD
	*ClientHelper

	store    *KVStore
=======
	clientHelper

	store    *tikvStore
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	req      *kv.Request
	finishCh chan struct{}

	tasks []*batchCopTask

	// Batch results are stored in respChan.
	respChan chan *batchCopResponse

	vars *kv.Variables

	memTracker *memory.Tracker

<<<<<<< HEAD
=======
	replicaReadSeed uint32

>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	rpcCancel *RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32
}

func (b *batchCopIterator) run(ctx context.Context) {
	// We run workers for every batch cop.
	for _, task := range b.tasks {
		b.wg.Add(1)
		bo := NewBackofferWithVars(ctx, copNextMaxBackoff, b.vars)
		go b.handleTask(ctx, bo, task)
	}
	b.wg.Wait()
	close(b.respChan)
}

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (b *batchCopIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *batchCopResponse
		ok     bool
		closed bool
	)

	// Get next fetched resp from chan
	resp, ok, closed = b.recvFromRespCh(ctx)
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := b.store.CheckVisibility(b.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (b *batchCopIterator) recvFromRespCh(ctx context.Context) (resp *batchCopResponse, ok bool, exit bool) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-b.respChan:
			return
		case <-ticker.C:
			if atomic.LoadUint32(b.vars.Killed) == 1 {
				resp = &batchCopResponse{err: ErrQueryInterrupted}
				ok = true
				return
			}
		case <-b.finishCh:
			exit = true
			return
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
				close(b.finishCh)
			}
			exit = true
			return
		}
	}
}

// Close releases the resource.
func (b *batchCopIterator) Close() error {
	if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		close(b.finishCh)
	}
	b.rpcCancel.CancelAll()
	b.wg.Wait()
	return nil
}

func (b *batchCopIterator) handleTask(ctx context.Context, bo *Backoffer, task *batchCopTask) {
<<<<<<< HEAD
=======
	logutil.BgLogger().Debug("handle batch task")
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	tasks := []*batchCopTask{task}
	for idx := 0; idx < len(tasks); idx++ {
		ret, err := b.handleTaskOnce(ctx, bo, tasks[idx])
		if err != nil {
			resp := &batchCopResponse{err: errors.Trace(err), detail: new(CopRuntimeStats)}
			b.sendToRespCh(resp)
			break
		}
		tasks = append(tasks, ret...)
	}
	b.wg.Done()
}

// Merge all ranges and request again.
func (b *batchCopIterator) retryBatchCopTask(ctx context.Context, bo *Backoffer, batchTask *batchCopTask) ([]*batchCopTask, error) {
<<<<<<< HEAD
	var ranges []kv.KeyRange
	for _, taskCtx := range batchTask.copTasks {
		taskCtx.task.ranges.Do(func(ran *kv.KeyRange) {
			ranges = append(ranges, *ran)
		})
	}
	return buildBatchCopTasks(bo, b.regionCache, NewKeyRanges(ranges), b.req.StoreType)
}

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	sender := NewRegionBatchRequestSender(b.store.GetRegionCache(), b.store.GetTiKVClient())
	var regionInfos []*coprocessor.RegionInfo
	for _, task := range task.copTasks {
		regionInfos = append(regionInfos, &coprocessor.RegionInfo{
			RegionId: task.task.region.GetID(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: task.task.region.GetConfVer(),
				Version: task.task.region.GetVer(),
			},
			Ranges: task.task.ranges.ToPBRanges(),
=======
	ranges := &copRanges{}
	for _, taskCtx := range batchTask.copTasks {
		taskCtx.task.ranges.do(func(ran *kv.KeyRange) {
			ranges.mid = append(ranges.mid, *ran)
		})
	}
	return buildBatchCopTasks(bo, b.RegionCache, ranges, b.req)
}

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	logutil.BgLogger().Debug("handle batch task once")
	sender := NewRegionBatchRequestSender(b.store.regionCache, b.store.client)
	var regionInfos []*coprocessor.RegionInfo
	for _, task := range task.copTasks {
		regionInfos = append(regionInfos, &coprocessor.RegionInfo{
			RegionId: task.task.region.id,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: task.task.region.confVer,
				Version: task.task.region.ver,
			},
			Ranges: task.task.ranges.toPBRanges(),
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		})
	}

	copReq := coprocessor.BatchRequest{
		Tp:        b.req.Tp,
		StartTs:   b.req.StartTs,
		Data:      b.req.Data,
		SchemaVer: b.req.SchemaVar,
		Regions:   regionInfos,
	}

	req := tikvrpc.NewRequest(task.cmdType, &copReq, kvrpcpb.Context{
<<<<<<< HEAD
		IsolationLevel: IsolationLevelToPB(b.req.IsolationLevel),
		Priority:       PriorityToPB(b.req.Priority),
=======
		IsolationLevel: pbIsolationLevel(b.req.IsolationLevel),
		Priority:       kvPriorityToCommandPri(b.req.Priority),
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
		NotFillCache:   b.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         b.req.TaskID,
	})
	req.StoreTp = kv.TiFlash

	logutil.BgLogger().Debug("send batch request to ", zap.String("req info", req.String()), zap.Int("cop task len", len(task.copTasks)))
	resp, retry, cancel, err := sender.sendStreamReqToAddr(bo, task.copTasks, req, ReadTimeoutUltraLong)
	// If there are store errors, we should retry for all regions.
	if retry {
		return b.retryBatchCopTask(ctx, bo, task)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer cancel()
	return nil, b.handleStreamedBatchCopResponse(ctx, bo, resp.Resp.(*tikvrpc.BatchCopStreamResponse), task)
}

func (b *batchCopIterator) handleStreamedBatchCopResponse(ctx context.Context, bo *Backoffer, response *tikvrpc.BatchCopStreamResponse, task *batchCopTask) (err error) {
	defer response.Close()
	resp := response.BatchResponse
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return
	}
	for {
		err = b.handleBatchCopResponse(bo, resp, task)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err = response.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}

<<<<<<< HEAD
			if err1 := bo.Backoff(BoTiKVRPC, errors.Errorf("recv stream response error: %v, task store addr: %s", err, task.storeAddr)); err1 != nil {
=======
			if err1 := bo.Backoff(boTiKVRPC, errors.Errorf("recv stream response error: %v, task store addr: %s", err, task.storeAddr)); err1 != nil {
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
				return errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}
}

func (b *batchCopIterator) handleBatchCopResponse(bo *Backoffer, response *coprocessor.BatchResponse, task *batchCopTask) (err error) {
	if otherErr := response.GetOtherError(); otherErr != "" {
		err = errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", b.req.StartTs),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return errors.Trace(err)
	}

	resp := batchCopResponse{
		pbResp: response,
		detail: new(CopRuntimeStats),
	}

<<<<<<< HEAD
	backoffTimes := bo.GetBackoffTimes()
	resp.detail.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(backoffTimes))
	for backoff := range backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
=======
	resp.detail.BackoffTime = time.Duration(bo.totalSleep) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(bo.backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(bo.backoffTimes))
	for backoff := range bo.backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = bo.backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.backoffSleepMS[backoff]) * time.Millisecond
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	}
	resp.detail.CalleeAddress = task.storeAddr

	b.sendToRespCh(&resp)

	return
}

func (b *batchCopIterator) sendToRespCh(resp *batchCopResponse) (exit bool) {
	select {
	case b.respChan <- resp:
	case <-b.finishCh:
		exit = true
	}
	return
}
