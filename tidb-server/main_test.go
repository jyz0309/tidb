// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var isCoverageServer = "0"

// TestRunMain is a dummy test case, which contains only the main function of tidb-server,
// and it is used to generate coverage_server.
func TestRunMain(t *testing.T) {
	if isCoverageServer == "1" {
		main()
	}
}

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct{}

func (t *testMainSuite) TestSetGlobalVars(c *C) {
<<<<<<< HEAD
	c.Assert(variable.GetSysVar(variable.TiDBIsolationReadEngines).Value, Equals, "tikv, tiflash, tidb")
	c.Assert(variable.GetSysVar(variable.TIDBMemQuotaQuery).Value, Equals, "1073741824")
=======
	c.Assert(variable.SysVars[variable.TiDBIsolationReadEngines].Value, Equals, "tikv, tiflash, tidb")
	c.Assert(variable.SysVars[variable.TIDBMemQuotaQuery].Value, Equals, "1073741824")
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tikv", "tidb"}
		conf.MemQuotaQuery = 9999999
	})
	setGlobalVars()

<<<<<<< HEAD
	c.Assert(variable.GetSysVar(variable.TiDBIsolationReadEngines).Value, Equals, "tikv, tidb")
	c.Assert(variable.GetSysVar(variable.TIDBMemQuotaQuery).Value, Equals, "9999999")
=======
	c.Assert(variable.SysVars[variable.TiDBIsolationReadEngines].Value, Equals, "tikv, tidb")
	c.Assert(variable.SysVars[variable.TIDBMemQuotaQuery].Value, Equals, "9999999")
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}
