// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"context"
<<<<<<< HEAD
=======
)
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1

	"github.com/pingcap/parser/model"
)

// UnionStore is a store that wraps a snapshot for read and a MemBuffer for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
<<<<<<< HEAD
	Retriever

	// HasPresumeKeyNotExists returns whether the key presumed key not exists error for the lazy check.
	HasPresumeKeyNotExists(k Key) bool
	// UnmarkPresumeKeyNotExists deletes the key presume key not exists error flag for the lazy check.
	UnmarkPresumeKeyNotExists(k Key)
	// CacheIndexName caches the index name.
	// PresumeKeyNotExists will use this to help decode error message.
	CacheTableInfo(id int64, info *model.TableInfo)
	// GetIndexName returns the cached index name.
	// If there is no such index already inserted through CacheIndexName, it will return UNKNOWN.
	GetTableInfo(id int64) *model.TableInfo

=======
	MemBuffer
	// GetKeyExistErrInfo gets the key exist error info for the lazy check.
	GetKeyExistErrInfo(k Key) *existErrInfo
	// DeleteKeyExistErrInfo deletes the key exist error info for the lazy check.
	DeleteKeyExistErrInfo(k Key)
	// ResetStmtKeyExistErrs deletes all the key exist error infos for the current statement.
	ResetStmtKeyExistErrs()
	// MergeStmtKeyExistErrs merges all the key exist error infos for the current statement.
	MergeStmtKeyExistErrs()
	// WalkBuffer iterates all buffered kv pairs.
	WalkBuffer(f func(k Key, v []byte) error) error
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// GetOption gets an option.
	GetOption(opt Option) interface{}
	// GetMemBuffer return the MemBuffer binding to this unionStore.
	GetMemBuffer() MemBuffer
}

// AssertionType is the type of a assertion.
type AssertionType int

// The AssertionType constants.
const (
	None AssertionType = iota
	Exist
	NotExist
)

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

// unionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type unionStore struct {
<<<<<<< HEAD
	memBuffer    *memdb
	snapshot     Snapshot
	idxNameCache map[int64]*model.TableInfo
	opts         options
=======
	*BufferStore
	keyExistErrs     map[string]*existErrInfo // for the lazy check
	stmtKeyExistErrs map[string]*existErrInfo
	opts             options
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// NewUnionStore builds a new unionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	return &unionStore{
<<<<<<< HEAD
		snapshot:     snapshot,
		memBuffer:    newMemDB(),
		idxNameCache: make(map[int64]*model.TableInfo),
		opts:         make(map[Option]interface{}),
	}
}

// GetMemBuffer return the MemBuffer binding to this unionStore.
func (us *unionStore) GetMemBuffer() MemBuffer {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *unionStore) Get(ctx context.Context, k Key) ([]byte, error) {
	v, err := us.memBuffer.Get(ctx, k)
	if IsErrNotFound(err) {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, ErrNotExist
	}
	return v, nil
}

// Iter implements the Retriever interface.
func (us *unionStore) Iter(k Key, upperBound Key) (Iterator, error) {
	bufferIt, err := us.memBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (us *unionStore) IterReverse(k Key) (Iterator, error) {
	bufferIt, err := us.memBuffer.IterReverse(k)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// HasPresumeKeyNotExists gets the key exist error info for the lazy check.
func (us *unionStore) HasPresumeKeyNotExists(k Key) bool {
	flags, err := us.memBuffer.GetFlags(k)
	if err != nil {
		return false
	}
	return flags.HasPresumeKeyNotExists()
}

// DeleteKeyExistErrInfo deletes the key exist error info for the lazy check.
func (us *unionStore) UnmarkPresumeKeyNotExists(k Key) {
	us.memBuffer.UpdateFlags(k, DelPresumeKeyNotExists)
}

func (us *unionStore) GetTableInfo(id int64) *model.TableInfo {
	return us.idxNameCache[id]
}

func (us *unionStore) CacheTableInfo(id int64, info *model.TableInfo) {
	us.idxNameCache[id] = info
}

// SetOption implements the unionStore SetOption interface.
=======
		BufferStore:      NewBufferStore(snapshot),
		keyExistErrs:     make(map[string]*existErrInfo),
		stmtKeyExistErrs: make(map[string]*existErrInfo),
		opts:             make(map[Option]interface{}),
	}
}

// Get implements the Retriever interface.
func (us *unionStore) Get(ctx context.Context, k Key) ([]byte, error) {
	v, err := us.MemBuffer.Get(ctx, k)
	if IsErrNotFound(err) {
		if _, ok := us.opts.Get(PresumeKeyNotExists); ok {
			e, ok := us.opts.Get(PresumeKeyNotExistsError)
			if ok {
				us.stmtKeyExistErrs[string(k)] = e.(*existErrInfo)
				if val, ok := us.opts.Get(CheckExists); ok {
					checkExistMap := val.(map[string]struct{})
					checkExistMap[string(k)] = struct{}{}
				}
			}
			return nil, ErrNotExist
		}
		v, err = us.BufferStore.r.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, ErrNotExist
	}
	return v, nil
}

func (us *unionStore) GetKeyExistErrInfo(k Key) *existErrInfo {
	if c, ok := us.stmtKeyExistErrs[string(k)]; ok {
		return c
	}
	if c, ok := us.keyExistErrs[string(k)]; ok {
		return c
	}
	return nil
}

func (us *unionStore) DeleteKeyExistErrInfo(k Key) {
	delete(us.stmtKeyExistErrs, string(k))
	delete(us.keyExistErrs, string(k))
}

// SetOption implements the UnionStore SetOption interface.
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
func (us *unionStore) SetOption(opt Option, val interface{}) {
	us.opts[opt] = val
}

// DelOption implements the unionStore DelOption interface.
func (us *unionStore) DelOption(opt Option) {
	delete(us.opts, opt)
}

// GetOption implements the unionStore GetOption interface.
func (us *unionStore) GetOption(opt Option) interface{} {
	return us.opts[opt]
}

<<<<<<< HEAD
=======
// GetMemBuffer return the MemBuffer binding to this UnionStore.
func (us *unionStore) GetMemBuffer() MemBuffer {
	return us.BufferStore.MemBuffer
}

func (us *unionStore) NewStagingBuffer() MemBuffer {
	return us.BufferStore.NewStagingBuffer()
}

func (us *unionStore) Flush() (int, error) {
	return us.BufferStore.Flush()
}

func (us *unionStore) Discard() {
	us.BufferStore.Discard()
}

func (us *unionStore) ResetStmtKeyExistErrs() {
	us.stmtKeyExistErrs = make(map[string]*existErrInfo)
}

func (us *unionStore) MergeStmtKeyExistErrs() {
	for k, v := range us.stmtKeyExistErrs {
		us.keyExistErrs[k] = v
	}
	us.stmtKeyExistErrs = make(map[string]*existErrInfo)
}

>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
type options map[Option]interface{}

func (opts options) Get(opt Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
