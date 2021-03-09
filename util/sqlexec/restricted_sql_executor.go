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

package sqlexec

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
)

// RestrictedSQLExecutor is an interface provides executing restricted sql statement.
// Why we need this interface?
// When we execute some management statements, we need to operate system tables.
// For example when executing create user statement, we need to check if the user already
// exists in the mysql.User table and insert a new row if not exists. In this case, we need
// a convenience way to manipulate system tables. The most simple way is executing sql statement.
// In order to execute sql statement in stmts package, we add this interface to solve dependence problem.
// And in the same time, we do not want this interface becomes a general way to run sql statement.
// We hope this could be used with some restrictions such as only allowing system tables as target,
// do not allowing recursion call.
// For more information please refer to the comments in session.ExecRestrictedSQL().
// This is implemented in session.go.
type RestrictedSQLExecutor interface {
	// ExecRestrictedSQL run sql statement in ctx with some restriction.
	ExecRestrictedSQL(sql string) ([]chunk.Row, []*ast.ResultField, error)
	// ExecRestrictedSQLWithContext run sql statement in ctx with some restriction.
	ExecRestrictedSQLWithContext(ctx context.Context, sql string, opts ...OptionFuncAlias) ([]chunk.Row, []*ast.ResultField, error)
	// ExecRestrictedSQLWithSnapshot run sql statement in ctx with some restriction and with snapshot.
	// If current session sets the snapshot timestamp, then execute with this snapshot timestamp.
	// Otherwise, execute with the current transaction start timestamp if the transaction is valid.
	ExecRestrictedSQLWithSnapshot(sql string) ([]chunk.Row, []*ast.ResultField, error)

	// The above methods are all deprecated.
	// After the refactor finish, they will be removed.

	// ParseWithParams is the parameterized version of Parse: it will try to prevent injection under utf8mb4.
	// It works like printf() in c, there are following format specifiers:
	// 1. %?: automatic conversion by the type of arguments. E.g. []string -> ('s1','s2'..)
	// 2. %%: output %
	// 3. %n: for identifiers, for example ("use %n", db)
	//
	// Attention: it does not prevent you from doing parse("select '%?", ";SQL injection!;") => "select '';SQL injection!;'".
	// One argument should be a standalone entity. It should not "concat" with other placeholders and characters.
	// This function only saves you from processing potentially unsafe parameters.
	ParseWithParams(ctx context.Context, sql string, args ...interface{}) (ast.StmtNode, error)
	// ExecRestrictedStmt run sql statement in ctx with some restriction.
	ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...OptionFuncAlias) ([]chunk.Row, []*ast.ResultField, error)
}

// ExecOption is a struct defined for ExecRestrictedSQLWithContext option.
type ExecOption struct {
	IgnoreWarning bool
	SnapshotTS    uint64
<<<<<<< HEAD
	AnalyzeVer    int
=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// OptionFuncAlias is defined for the optional paramater of ExecRestrictedSQLWithContext.
type OptionFuncAlias = func(option *ExecOption)

// ExecOptionIgnoreWarning tells ExecRestrictedSQLWithContext to ignore the warnings.
var ExecOptionIgnoreWarning OptionFuncAlias = func(option *ExecOption) {
	option.IgnoreWarning = true
}

<<<<<<< HEAD
// ExecOptionAnalyzeVer1 tells ExecRestrictedSQLWithContext to collect statistics with version1.
var ExecOptionAnalyzeVer1 OptionFuncAlias = func(option *ExecOption) {
	option.AnalyzeVer = 1
}

// ExecOptionAnalyzeVer2 tells ExecRestrictedSQLWithContext to collect statistics with version2.
var ExecOptionAnalyzeVer2 OptionFuncAlias = func(option *ExecOption) {
	option.AnalyzeVer = 2
}

=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
// ExecOptionWithSnapshot tells ExecRestrictedSQLWithContext to use a snapshot.
func ExecOptionWithSnapshot(snapshot uint64) OptionFuncAlias {
	return func(option *ExecOption) {
		option.SnapshotTS = snapshot
	}
}

// SQLExecutor is an interface provides executing normal sql statement.
// Why we need this interface? To break circle dependence of packages.
// For example, privilege/privileges package need execute SQL, if it use
// session.Session.Execute, then privilege/privileges and tidb would become a circle.
type SQLExecutor interface {
	// Execute is only used by plugins. It can be removed soon.
	Execute(ctx context.Context, sql string) ([]RecordSet, error)
	// ExecuteInternal means execute sql as the internal sql.
	ExecuteInternal(ctx context.Context, sql string, args ...interface{}) (RecordSet, error)
<<<<<<< HEAD
	ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (RecordSet, error)
=======
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// SQLParser is an interface provides parsing sql statement.
// To parse a sql statement, we could run parser.New() to get a parser object, and then run Parse method on it.
// But a session already has a parser bind in it, so we define this interface and use session as its implementation,
// thus avoid allocating new parser. See session.SQLParser for more information.
type SQLParser interface {
	ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error)
}

// Statement is an interface for SQL execution.
// NOTE: all Statement implementations must be safe for
// concurrent using by multiple goroutines.
// If the Exec method requires any Execution domain local data,
// they must be held out of the implementing instance.
type Statement interface {
	// OriginText gets the origin SQL text.
	OriginText() string

	// GetTextToLog gets the desensitization SQL text for logging.
	GetTextToLog() string

	// Exec executes SQL and gets a Recordset.
	Exec(ctx context.Context) (RecordSet, error)

	// IsPrepared returns whether this statement is prepared statement.
	IsPrepared() bool

	// IsReadOnly returns if the statement is read only. For example: SelectStmt without lock.
	IsReadOnly(vars *variable.SessionVars) bool

	// RebuildPlan rebuilds the plan of the statement.
	RebuildPlan(ctx context.Context) (schemaVersion int64, err error)
}

// RecordSet is an abstract result set interface to help get data from Plan.
type RecordSet interface {
	// Fields gets result fields.
	Fields() []*ast.ResultField

	// Next reads records into chunk.
	Next(ctx context.Context, req *chunk.Chunk) error

	// NewChunk create a chunk.
	NewChunk() *chunk.Chunk

	// Close closes the underlying iterator, call Next after Close will
	// restart the iteration.
	Close() error
}

// MultiQueryNoDelayResult is an interface for one no-delay result for one statement in multi-queries.
type MultiQueryNoDelayResult interface {
	// AffectedRows return affected row for one statement in multi-queries.
	AffectedRows() uint64
	// LastMessage return last message for one statement in multi-queries.
	LastMessage() string
	// WarnCount return warn count for one statement in multi-queries.
	WarnCount() uint16
	// Status return status when executing one statement in multi-queries.
	Status() uint16
	// LastInsertID return last insert id for one statement in multi-queries.
	LastInsertID() uint64
}
