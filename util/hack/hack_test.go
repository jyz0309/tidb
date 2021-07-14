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

package hack

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	t.Parallel()

	b := []byte("hello world")
	a := String(b)
	assert.EqualValues(t, "hello world", a)

	b[0] = 'a'
	assert.EqualValues(t, "aello world", a)

	b = append(b, "abc"...)
	assert.EqualValuesf(t, "aello world", a, "a:%v, b:%v", a, b)
}

func TestByte(t *testing.T) {
	t.Parallel()

	a := "hello world"
	b := Slice(a)
	assert.True(t, bytes.Equal(b, []byte("hello world")), string(b))
}

func TestMutable(t *testing.T) {
	t.Parallel()

	a := []byte{'a', 'b', 'c'}
	b := String(a) // b is a mutable string.
	c := string(b) // Warn, c is a mutable string
	assert.Equal(t, "abc", c)

	// c changed after a is modified
	a[0] = 's'
	assert.Equal(t, "sbc", c)
}
