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

// +build windows

package storage

import (
<<<<<<< HEAD
	"golang.org/x/sys/windows"
=======
	"syscall"
	"unsafe"
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
)

// GetTargetDirectoryCapacity get the capacity (bytes) of directory
func GetTargetDirectoryCapacity(path string) (uint64, error) {
<<<<<<< HEAD
	var freeBytes uint64
	err := windows.GetDiskFreeSpaceEx(windows.StringToUTF16Ptr(path), &freeBytes, nil, nil)
	if err != nil {
		return 0, err
	}
	return freeBytes, nil
=======
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")
	var freeBytes int64
	_, _, err := c.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&freeBytes)))
	if err != nil {
		return 0, err
	}
	return uint64(freeBytes), nil
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}
