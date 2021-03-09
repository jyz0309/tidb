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

package variable

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
<<<<<<< HEAD
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
	atomic2 "go.uber.org/atomic"
=======
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
)

// ScopeFlag is for system variable whether can be changed in global/session dynamically or not.
type ScopeFlag uint8

// TypeFlag is the SysVar type, which doesn't exactly match MySQL types.
type TypeFlag byte

const (
	// ScopeNone means the system variable can not be changed dynamically.
	ScopeNone ScopeFlag = 0
	// ScopeGlobal means the system variable can be changed globally.
	ScopeGlobal ScopeFlag = 1 << 0
	// ScopeSession means the system variable can only be changed in current session.
	ScopeSession ScopeFlag = 1 << 1

<<<<<<< HEAD
	// TypeStr is the default
	TypeStr TypeFlag = 0
	// TypeBool for boolean
	TypeBool TypeFlag = 1
	// TypeInt for integer
	TypeInt TypeFlag = 2
	// TypeEnum for Enum
	TypeEnum TypeFlag = 3
	// TypeFloat for Double
	TypeFloat TypeFlag = 4
	// TypeUnsigned for Unsigned integer
	TypeUnsigned TypeFlag = 5
	// TypeTime for time of day (a TiDB extension)
	TypeTime TypeFlag = 6
	// TypeDuration for a golang duration (a TiDB extension)
	TypeDuration TypeFlag = 7

	// BoolOff is the canonical string representation of a boolean false.
	BoolOff = "OFF"
	// BoolOn is the canonical string representation of a boolean true.
	BoolOn = "ON"

	// On is the canonical string for ON
	On = "ON"

	// Off is the canonical string for OFF
	Off = "OFF"

	// Nightly indicate the nightly version.
	Nightly = "NIGHTLY"
	// Warn means return warnings
=======
	// Off is the string OFF
	Off = "OFF"
	// On is the string ON
	On = "ON"
	// Warn is the string WARN
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
	Warn = "WARN"
)

// SysVar is for system variable.
type SysVar struct {
	// Scope is for whether can be changed or not
	Scope ScopeFlag
	// Name is the variable name.
	Name string
	// Value is the variable value.
	Value string
	// Type is the MySQL type (optional)
	Type TypeFlag
	// MinValue will automatically be validated when specified (optional)
	MinValue int64
	// MaxValue will automatically be validated when specified (optional)
	MaxValue uint64
	// AutoConvertNegativeBool applies to boolean types (optional)
	AutoConvertNegativeBool bool
	// AutoConvertOutOfRange applies to int and unsigned types.
	AutoConvertOutOfRange bool
	// ReadOnly applies to all types
	ReadOnly bool
	// PossibleValues applies to ENUM type
	PossibleValues []string
	// AllowEmpty is a special TiDB behavior which means "read value from config" (do not use)
	AllowEmpty bool
	// AllowEmptyAll is a special behavior that only applies to TiDBCapturePlanBaseline, TiDBTxnMode (do not use)
	AllowEmptyAll bool
	// AllowAutoValue means that the special value "-1" is permitted, even when outside of range.
	AllowAutoValue bool
	// Validation is a callback after the type validation has been performed
	Validation func(*SessionVars, string, string, ScopeFlag) (string, error)
	// IsHintUpdatable indicate whether it's updatable via SET_VAR() hint (optional)
	IsHintUpdatable bool
}

// ValidateFromType provides automatic validation based on the SysVar's type
func (sv *SysVar) ValidateFromType(vars *SessionVars, value string, scope ScopeFlag) (string, error) {
	// Some sysvars are read-only. Attempting to set should always fail.
	if sv.ReadOnly || sv.Scope == ScopeNone {
		return value, ErrIncorrectScope.GenWithStackByArgs(sv.Name, "read only")
	}
	// The string "DEFAULT" is a special keyword in MySQL, which restores
	// the compiled sysvar value. In which case we can skip further validation.
	if strings.EqualFold(value, "DEFAULT") {
		return sv.Value, nil
	}
	// Some sysvars in TiDB have a special behavior where the empty string means
	// "use the config file value". This needs to be cleaned up once the behavior
	// for instance variables is determined.
	if value == "" && ((sv.AllowEmpty && scope == ScopeSession) || sv.AllowEmptyAll) {
		return value, nil
	}
	// Provide validation using the SysVar struct
	switch sv.Type {
	case TypeUnsigned:
		return sv.checkUInt64SystemVar(value, vars)
	case TypeInt:
		return sv.checkInt64SystemVar(value, vars)
	case TypeBool:
		return sv.checkBoolSystemVar(value, vars)
	case TypeFloat:
		return sv.checkFloatSystemVar(value, vars)
	case TypeEnum:
		return sv.checkEnumSystemVar(value, vars)
	case TypeTime:
		return sv.checkTimeSystemVar(value, vars)
	case TypeDuration:
		return sv.checkDurationSystemVar(value, vars)
	}
	return value, nil // typeString
}

const (
	localDayTimeFormat = "15:04"
	// FullDayTimeFormat is the full format of analyze start time and end time.
	FullDayTimeFormat = "15:04 -0700"
)

func (sv *SysVar) checkTimeSystemVar(value string, vars *SessionVars) (string, error) {
	var t time.Time
	var err error
	if len(value) <= len(localDayTimeFormat) {
		t, err = time.ParseInLocation(localDayTimeFormat, value, vars.TimeZone)
	} else {
		t, err = time.ParseInLocation(FullDayTimeFormat, value, vars.TimeZone)
	}
	if err != nil {
		return "", err
	}
	return t.Format(FullDayTimeFormat), nil
}

func (sv *SysVar) checkDurationSystemVar(value string, vars *SessionVars) (string, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// Check for min/max violations
	if int64(d) < sv.MinValue {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if uint64(d) > sv.MaxValue {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// return a string representation of the duration
	return d.String(), nil
}

func (sv *SysVar) checkUInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	if !sv.AutoConvertOutOfRange {
		return sv.checkUint64SystemVarWithError(value)
	}
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
		}
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	if val > sv.MaxValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MaxValue), nil
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	if !sv.AutoConvertOutOfRange {
		return sv.checkInt64SystemVarWithError(value)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	if val > int64(sv.MaxValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MaxValue), nil
	}
	return value, nil
}

func (sv *SysVar) checkEnumSystemVar(value string, vars *SessionVars) (string, error) {
	// The value could be either a string or the ordinal position in the PossibleValues.
	// This allows for the behavior 0 = OFF, 1 = ON, 2 = DEMAND etc.
	var iStr string
	for i, v := range sv.PossibleValues {
		iStr = fmt.Sprintf("%d", i)
		if strings.EqualFold(value, v) || strings.EqualFold(value, iStr) {
			return v, nil
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkFloatSystemVar(value string, vars *SessionVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < float64(sv.MinValue) || val > float64(sv.MaxValue) {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

func (sv *SysVar) checkBoolSystemVar(value string, vars *SessionVars) (string, error) {
	if strings.EqualFold(value, "ON") {
		return BoolOn, nil
	} else if strings.EqualFold(value, "OFF") {
		return BoolOff, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		// There are two types of conversion rules for integer values.
		// The default only allows 0 || 1, but a subset of values convert any
		// negative integer to 1.
		if !sv.AutoConvertNegativeBool {
			if val == 0 {
				return BoolOff, nil
			} else if val == 1 {
				return BoolOn, nil
			}
		} else {
			if val == 1 || val < 0 {
				return BoolOn, nil
			} else if val == 0 {
				return BoolOff, nil
			}
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkUint64SystemVarWithError(value string) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		// // in strict it expects the error WrongValue, but in non-strict it returns WrongType
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) || val > sv.MaxValue {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVarWithError(value string) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue || val > int64(sv.MaxValue) {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

// ValidateFromHook calls the anonymous function on the sysvar if it exists.
func (sv *SysVar) ValidateFromHook(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
	if sv.Validation != nil {
		return sv.Validation(vars, normalizedValue, originalValue, scope)
	}
	return normalizedValue, nil
}

// GetNativeValType attempts to convert the val to the approx MySQL non-string type
func (sv *SysVar) GetNativeValType(val string) (types.Datum, byte, uint) {
	switch sv.Type {
	case TypeUnsigned:
		u, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			u = 0
		}
		return types.NewUintDatum(u), mysql.TypeLonglong, mysql.UnsignedFlag
	case TypeBool:
		optVal := int64(0) // OFF
		if TiDBOptOn(val) {
			optVal = 1
		}
		return types.NewIntDatum(optVal), mysql.TypeLong, 0
	}
	return types.NewStringDatum(val), mysql.TypeVarString, 0
}

var sysVars map[string]*SysVar
var sysVarsLock sync.RWMutex

// RegisterSysVar adds a sysvar to the SysVars list
func RegisterSysVar(sv *SysVar) {
	name := strings.ToLower(sv.Name)
	sysVarsLock.Lock()
	sysVars[name] = sv
	sysVarsLock.Unlock()
}

// UnregisterSysVar removes a sysvar from the SysVars list
// currently only used in tests.
func UnregisterSysVar(name string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	delete(sysVars, name)
	sysVarsLock.Unlock()
}

// GetSysVar returns sys var info for name as key.
func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	return sysVars[name]
}

// SetSysVar sets a sysvar. This will not propagate to the cluster, so it should only be
// used for instance scoped AUTO variables such as system_time_zone.
func SetSysVar(name string, value string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	defer sysVarsLock.Unlock()
	sysVars[name].Value = value
}

// GetSysVars returns the sysVars list under a RWLock
func GetSysVars() map[string]*SysVar {
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	return sysVars
}

// PluginVarNames is global plugin var names set.
var PluginVarNames []string

func init() {
	sysVars = make(map[string]*SysVar)
	for _, v := range defaultSysVars {
		RegisterSysVar(v)
	}
	for _, v := range noopSysVars {
		RegisterSysVar(v)
	}
	initSynonymsSysVariables()
}

// BoolToOnOff returns the string representation of a bool, i.e. "ON/OFF"
func BoolToOnOff(b bool) string {
	if b {
		return BoolOn
	}
	return BoolOff
}

func int32ToBoolStr(i int32) string {
	if i == 1 {
		return BoolOn
	}
	return BoolOff
}

func checkCharacterValid(normalizedValue string, argName string) (string, error) {
	if normalizedValue == "" {
		return normalizedValue, errors.Trace(ErrWrongValueForVar.GenWithStackByArgs(argName, "NULL"))
	}
	cht, _, err := charset.GetCharsetInfo(normalizedValue)
	if err != nil {
		return normalizedValue, errors.Trace(err)
	}
	return cht, nil
}

var defaultSysVars = []*SysVar{
<<<<<<< HEAD
	{Scope: ScopeGlobal, Name: MaxConnections, Value: "151", Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLSelectLimit, Value: "18446744073709551615", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: DefaultWeekFormat, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 7, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLModeVar, Value: mysql.DefaultSQLMode, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxExecutionTime, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationServer, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if _, err := collate.GetCollationByName(normalizedValue); err != nil {
			return normalizedValue, errors.Trace(err)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLLogBin, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TimeZone, Value: "SYSTEM", IsHintUpdatable: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if strings.EqualFold(normalizedValue, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(normalizedValue)
		return normalizedValue, err
	}},
	{Scope: ScopeNone, Name: SystemTimeZone, Value: "CST"},
	{Scope: ScopeGlobal | ScopeSession, Name: ForeignKeyChecks, Value: BoolOff, Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			// TiDB does not yet support foreign keys.
			// Return the original value in the warning, so that users are not confused.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue))
			return BoolOff, nil
		} else if !TiDBOptOn(normalizedValue) {
			return BoolOff, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue)
	}},
	{Scope: ScopeNone, Name: Hostname, Value: ServerHostname},
	{Scope: ScopeSession, Name: Timestamp, Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetFilesystem, Value: "binary", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterValid(normalizedValue, CharacterSetFilesystem)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationDatabase, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if _, err := collate.GetCollationByName(normalizedValue); err != nil {
			return normalizedValue, errors.Trace(err)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementIncrement, Value: strconv.FormatInt(DefAutoIncrementIncrement, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementOffset, Value: strconv.FormatInt(DefAutoIncrementOffset, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetClient, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterValid(normalizedValue, CharacterSetClient)
	}},
	{Scope: ScopeNone, Name: Port, Value: "4000", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: ScopeNone, Name: LowerCaseTableNames, Value: "2"},
	{Scope: ScopeNone, Name: LogBin, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetResults, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return normalizedValue, nil
		}
		return checkCharacterValid(normalizedValue, "")
	}},
	{Scope: ScopeNone, Name: VersionComment, Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 5.7 compatible"},
	{Scope: ScopeGlobal | ScopeSession, Name: TxnIsolation, Value: "REPEATABLE-READ", Type: TypeEnum, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
			if skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck); err != nil {
				return normalizedValue, err
			} else if !TiDBOptOn(skipIsolationLevelCheck) {
				return normalizedValue, ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
			}
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TransactionIsolation, Value: "REPEATABLE-READ", Type: TypeEnum, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
			if skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck); err != nil {
				return normalizedValue, err
			} else if !TiDBOptOn(skipIsolationLevelCheck) {
				return normalizedValue, returnErr
			}
			vars.StmtCtx.AppendWarning(returnErr)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationConnection, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if _, err := collate.GetCollationByName(normalizedValue); err != nil {
			return normalizedValue, errors.Trace(err)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeNone, Name: Version, Value: mysql.ServerVersion},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoCommit, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: CharsetDatabase, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterValid(normalizedValue, CharsetDatabase)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TxReadOnly, Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: TransactionReadOnly, Value: "0"},
	{Scope: ScopeGlobal, Name: MaxPreparedStmtCount, Value: strconv.FormatInt(DefMaxPreparedStmtCount, 10), Type: TypeInt, MinValue: -1, MaxValue: 1048576, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: DataDir, Value: "/usr/local/mysql/data/"},
	{Scope: ScopeGlobal | ScopeSession, Name: WaitTimeout, Value: strconv.FormatInt(DefWaitTimeout, 10), Type: TypeUnsigned, MinValue: 0, MaxValue: 31536000, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbLockWaitTimeout, Value: strconv.FormatInt(DefInnodbLockWaitTimeout, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 1073741824, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: GroupConcatMaxLen, Value: "1024", AutoConvertOutOfRange: true, IsHintUpdatable: true, Type: TypeUnsigned, MinValue: 4, MaxValue: math.MaxUint64, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
		// Minimum Value 4
		// Maximum Value (64-bit platforms) 18446744073709551615
		// Maximum Value (32-bit platforms) 4294967295
		if mathutil.IntBits == 32 {
			if val, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil {
				if val > uint64(math.MaxUint32) {
					vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(GroupConcatMaxLen, originalValue))
					return fmt.Sprintf("%d", math.MaxUint32), nil
				}
			}
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeNone, Name: Socket, Value: "/tmp/myssock"},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetConnection, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterValid(normalizedValue, CharacterSetConnection)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetServer, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterValid(normalizedValue, CharacterSetServer)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxAllowedPacket, Value: "67108864", Type: TypeUnsigned, MinValue: 1024, MaxValue: MaxOfMaxAllowedPacket, AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: WarningCount, Value: "0", ReadOnly: true},
	{Scope: ScopeSession, Name: ErrorCount, Value: "0", ReadOnly: true},
	{Scope: ScopeGlobal | ScopeSession, Name: WindowingUseHighPrecision, Value: BoolOn, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeSession, Name: TiDBTxnScope, Value: config.GetTxnScopeFromConfig()},
	/* TiDB specific variables */
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowMPPExecution, Type: TypeBool, Value: BoolToOnOff(DefTiDBAllowMPPExecution)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBCJThresholdCount, Value: strconv.Itoa(DefBroadcastJoinThresholdCount), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBCJThresholdSize, Value: strconv.Itoa(DefBroadcastJoinThresholdSize), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBSnapshot, Value: ""},
	{Scope: ScopeSession, Name: TiDBOptAggPushDown, Value: BoolToOnOff(DefOptAggPushDown), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptBCJ, Value: BoolToOnOff(DefOptBCJ), Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) && vars.AllowBatchCop == 0 {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("Can't set Broadcast Join to 1 but tidb_allow_batch_cop is 0, please active batch cop at first.")
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeSession, Name: TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBOptWriteRowID, Value: BoolToOnOff(DefOptWriteRowID)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBuildStatsConcurrency, Value: strconv.Itoa(DefBuildStatsConcurrency)},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeRatio, Value: strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeStartTime, Value: DefAutoAnalyzeStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeEndTime, Value: DefAutoAnalyzeEndTime, Type: TypeTime},
	{Scope: ScopeSession, Name: TiDBChecksumTableConcurrency, Value: strconv.Itoa(DefChecksumTableConcurrency)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBExecutorConcurrency, Value: strconv.Itoa(DefExecutorConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDistSQLScanConcurrency, Value: strconv.Itoa(DefDistSQLScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(DefOptInSubqToJoinAndAgg), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBOptPreferRangeScan, Value: BoolToOnOff(DefOptPreferRangeScan), Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationThreshold, Value: strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 1},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationExpFactor, Value: strconv.Itoa(DefOptCorrelationExpFactor), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCPUFactor, Value: strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCopCPUFactor, Value: strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptNetworkFactor, Value: strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptScanFactor, Value: strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDescScanFactor, Value: strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptSeekFactor, Value: strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptMemoryFactor, Value: strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDiskFactor, Value: strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexJoinBatchSize, Value: strconv.Itoa(DefIndexJoinBatchSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupSize, Value: strconv.Itoa(DefIndexLookupSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupConcurrency, Value: strconv.Itoa(DefIndexLookupConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupJoinConcurrency, Value: strconv.Itoa(DefIndexLookupJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexSerialScanConcurrency, Value: strconv.Itoa(DefIndexSerialScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipUTF8Check, Value: BoolToOnOff(DefSkipUTF8Check), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipASCIICheck, Value: BoolToOnOff(DefSkipASCIICheck), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBBatchInsert, Value: BoolToOnOff(DefBatchInsert), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBBatchDelete, Value: BoolToOnOff(DefBatchDelete), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBBatchCommit, Value: BoolToOnOff(DefBatchCommit), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDMLBatchSize, Value: strconv.Itoa(DefDMLBatchSize), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeSession, Name: TiDBCurrentTS, Value: strconv.Itoa(DefCurretTS), ReadOnly: true},
	{Scope: ScopeSession, Name: TiDBLastTxnInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true},
	{Scope: ScopeSession, Name: TiDBLastQueryInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxChunkSize, Value: strconv.Itoa(DefMaxChunkSize), Type: TypeUnsigned, MinValue: maxChunkSizeLowerBound, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowBatchCop, Value: strconv.Itoa(DefTiDBAllowBatchCop), Type: TypeInt, MinValue: 0, MaxValue: 2, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "0" && vars.AllowBCJ {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("Can't set batch cop 0 but tidb_opt_broadcast_join is 1, please set tidb_opt_broadcast_join 0 at first")
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBInitChunkSize, Value: strconv.Itoa(DefInitChunkSize), Type: TypeUnsigned, MinValue: 1, MaxValue: initChunkSizeUpperBound},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableCascadesPlanner, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMerge, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: TIDBMemQuotaQuery, Value: strconv.FormatInt(config.GetGlobalConfig().MemQuotaQuery, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaHashJoin, Value: strconv.FormatInt(DefTiDBMemQuotaHashJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaMergeJoin, Value: strconv.FormatInt(DefTiDBMemQuotaMergeJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaSort, Value: strconv.FormatInt(DefTiDBMemQuotaSort, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaTopn, Value: strconv.FormatInt(DefTiDBMemQuotaTopn, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaIndexLookupReader, Value: strconv.FormatInt(DefTiDBMemQuotaIndexLookupReader, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaIndexLookupJoin, Value: strconv.FormatInt(DefTiDBMemQuotaIndexLookupJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBEnableStreaming, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBEnableChunkRPC, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeSession, Name: TxnIsolationOneShot, Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableTablePartition, Value: BoolOn, Type: TypeEnum, PossibleValues: []string{BoolOff, BoolOn, "AUTO", Nightly}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashJoinConcurrency, Value: strconv.Itoa(DefTiDBHashJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBProjectionConcurrency, Value: strconv.Itoa(DefTiDBProjectionConcurrency), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggPartialConcurrency, Value: strconv.Itoa(DefTiDBHashAggPartialConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggFinalConcurrency, Value: strconv.Itoa(DefTiDBHashAggFinalConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBWindowConcurrency, Value: strconv.Itoa(DefTiDBWindowConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMergeJoinConcurrency, Value: strconv.Itoa(DefTiDBMergeJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStreamAggConcurrency, Value: strconv.Itoa(DefTiDBStreamAggConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableParallelApply, Value: BoolToOnOff(DefTiDBEnableParallelApply), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMemQuotaApplyCache, Value: strconv.Itoa(DefTiDBMemQuotaApplyCache)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackoffLockFast, Value: strconv.Itoa(kv.DefBackoffLockFast), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackOffWeight, Value: strconv.Itoa(kv.DefBackOffWeight), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRetryLimit, Value: strconv.Itoa(DefTiDBRetryLimit), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDisableTxnAutoRetry, Value: BoolToOnOff(DefTiDBDisableTxnAutoRetry), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBConstraintCheckInPlace, Value: BoolToOnOff(DefTiDBConstraintCheckInPlace), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnMode, Value: DefTiDBTxnMode, AllowEmptyAll: true, Type: TypeEnum, PossibleValues: []string{"pessimistic", "optimistic"}},
	{Scope: ScopeGlobal, Name: TiDBRowFormatVersion, Value: strconv.Itoa(DefTiDBRowFormatV1), Type: TypeUnsigned, MinValue: 1, MaxValue: 2},
	{Scope: ScopeSession, Name: TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(DefTiDBOptimizerSelectivityLevel), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableWindowFunction, Value: BoolToOnOff(DefEnableWindowFunction), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStrictDoubleTypeCheck, Value: BoolToOnOff(DefEnableStrictDoubleTypeCheck), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableVectorizedExpression, Value: BoolToOnOff(DefEnableVectorizedExpression), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableFastAnalyze, Value: BoolToOnOff(DefTiDBUseFastAnalyze), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipIsolationLevelCheck, Value: BoolToOnOff(DefTiDBSkipIsolationLevelCheck), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableRateLimitAction, Value: BoolToOnOff(DefTiDBEnableRateLimitAction), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableTiFlashFallbackTiKV, Value: BoolToOnOff(DefTiDBEnableTiFlashFallbackTiKV), Type: TypeBool},
	/* The following variable is defined as session scope but is actually server scope. */
	{Scope: ScopeSession, Name: TiDBGeneralLog, Value: BoolToOnOff(DefTiDBGeneralLog), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBPProfSQLCPU, Value: strconv.Itoa(DefTiDBPProfSQLCPU), Type: TypeInt, MinValue: 0, MaxValue: 1},
	{Scope: ScopeSession, Name: TiDBDDLSlowOprThreshold, Value: strconv.Itoa(DefTiDBDDLSlowOprThreshold)},
	{Scope: ScopeSession, Name: TiDBConfig, Value: "", ReadOnly: true},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgWorkerCount, Value: strconv.Itoa(DefTiDBDDLReorgWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgBatchSize, Value: strconv.Itoa(DefTiDBDDLReorgBatchSize), Type: TypeUnsigned, MinValue: int64(MinDDLReorgBatchSize), MaxValue: uint64(MaxDDLReorgBatchSize), AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: TiDBDDLErrorCountLimit, Value: strconv.Itoa(DefTiDBDDLErrorCountLimit), Type: TypeUnsigned, MinValue: 0, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBDDLReorgPriority, Value: "PRIORITY_LOW"},
	{Scope: ScopeGlobal, Name: TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(DefTiDBMaxDeltaSchemaCount), Type: TypeUnsigned, MinValue: 100, MaxValue: 16384, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableChangeColumnType, Value: BoolToOnOff(DefTiDBChangeColumnType), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableChangeMultiSchema, Value: BoolToOnOff(DefTiDBChangeMultiSchema), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnablePointGetCache, Value: BoolToOnOff(DefTiDBPointGetCache), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableAlterPlacement, Value: BoolToOnOff(DefTiDBEnableAlterPlacement), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBForcePriority, Value: mysql.Priority2Str[DefTiDBForcePriority]},
	{Scope: ScopeSession, Name: TiDBEnableRadixJoin, Value: BoolToOnOff(DefTiDBUseRadixJoin), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptJoinReorderThreshold, Value: strconv.Itoa(DefTiDBOptJoinReorderThreshold), Type: TypeUnsigned, MinValue: 0, MaxValue: 63},
	{Scope: ScopeSession, Name: TiDBSlowQueryFile, Value: ""},
	{Scope: ScopeGlobal, Name: TiDBScatterRegion, Value: BoolToOnOff(DefTiDBScatterRegion), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionFinish, Value: BoolToOnOff(DefTiDBWaitSplitRegionFinish), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionTimeout, Value: strconv.Itoa(DefWaitSplitRegionTimeout), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBLowResolutionTSO, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold), Type: TypeUnsigned, MinValue: int64(MinExpensiveQueryTimeThreshold), MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNoopFuncs, Value: BoolToOnOff(DefTiDBEnableNoopFuncs), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBReplicaRead, Value: "leader", Type: TypeEnum, PossibleValues: []string{"leader", "follower", "leader-and-follower"}},
	{Scope: ScopeSession, Name: TiDBAllowRemoveAutoInc, Value: BoolToOnOff(DefTiDBAllowRemoveAutoInc), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStmtSummary, Value: BoolToOnOff(config.GetGlobalConfig().StmtSummary.Enable), Type: TypeBool, AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(config.GetGlobalConfig().StmtSummary.EnableInternalQuery), Type: TypeBool, AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(config.GetGlobalConfig().StmtSummary.RefreshInterval), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt32), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryHistorySize, Value: strconv.Itoa(config.GetGlobalConfig().StmtSummary.HistorySize), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxUint8), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryMaxStmtCount, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), 10), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt16), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryMaxSQLLength, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxSQLLength), 10), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxInt32), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBCapturePlanBaseline, Value: BoolOff, Type: TypeBool, AllowEmptyAll: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBUsePlanBaselines, Value: BoolToOnOff(DefTiDBUsePlanBaselines), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEvolvePlanBaselines, Value: BoolToOnOff(DefTiDBEvolvePlanBaselines), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExtendedStats, Value: BoolToOnOff(false), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskMaxTime, Value: strconv.Itoa(DefTiDBEvolvePlanTaskMaxTime), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskStartTime, Value: DefTiDBEvolvePlanTaskStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskEndTime, Value: DefTiDBEvolvePlanTaskEndTime, Type: TypeTime},
	{Scope: ScopeSession, Name: TiDBIsolationReadEngines, Value: strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ", "), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		engines := strings.Split(normalizedValue, ",")
		var formatVal string
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			if i != 0 {
				formatVal += ","
			}
			switch {
			case strings.EqualFold(engine, kv.TiKV.Name()):
				formatVal += kv.TiKV.Name()
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				formatVal += kv.TiFlash.Name()
			case strings.EqualFold(engine, kv.TiDB.Name()):
				formatVal += kv.TiDB.Name()
			default:
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(TiDBIsolationReadEngines, normalizedValue)
			}
		}
		return formatVal, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBMetricSchemaStep, Value: strconv.Itoa(DefTiDBMetricSchemaStep), Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60},
	{Scope: ScopeSession, Name: TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(DefTiDBMetricSchemaRangeDuration), Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60},
	{Scope: ScopeSession, Name: TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBQueryLogMaxLen, Value: strconv.Itoa(logutil.DefaultQueryLogMaxLen), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().CheckMb4ValueInUTF8), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBFoundInPlanCache, Value: BoolToOnOff(DefTiDBFoundInPlanCache), Type: TypeBool, ReadOnly: true},
	{Scope: ScopeSession, Name: TiDBFoundInBinding, Value: BoolToOnOff(DefTiDBFoundInBinding), Type: TypeBool, ReadOnly: true},
	{Scope: ScopeSession, Name: TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(DefTiDBEnableCollectExecutionInfo), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowAutoRandExplicitInsert, Value: BoolToOnOff(DefTiDBAllowAutoRandExplicitInsert), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableClusteredIndex, Value: BoolToOnOff(DefTiDBEnableClusteredIndex), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPartitionPruneMode, Value: string(StaticOnly), Type: TypeStr, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if !PartitionPruneMode(normalizedValue).Valid() {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(TiDBPartitionPruneMode)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSlowLogMasking, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRedactLog, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBShardAllocateStep, Value: strconv.Itoa(DefTiDBShardAllocateStep), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: TiDBEnableTelemetry, Value: BoolToOnOff(DefTiDBEnableTelemetry), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAmendPessimisticTxn, Value: BoolToOnOff(DefTiDBEnableAmendPessimisticTxn), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAsyncCommit, Value: BoolToOnOff(DefTiDBEnableAsyncCommit), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnable1PC, Value: BoolToOnOff(DefTiDBEnable1PC), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBGuaranteeLinearizability, Value: BoolToOnOff(DefTiDBGuaranteeLinearizability), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeVersion, Value: strconv.Itoa(DefTiDBAnalyzeVersion), Type: TypeInt, MinValue: 1, MaxValue: 2, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "2" && FeedbackProbability.Load() > 0 {
			var original string
			var err error
			if scope == ScopeGlobal {
				original, err = vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBAnalyzeVersion)
				if err != nil {
					return normalizedValue, nil
				}
			} else {
				original = strconv.Itoa(vars.AnalyzeVersion)
			}
			vars.StmtCtx.AppendError(errors.New("variable tidb_analyze_version not updated because analyze version 2 is incompatible with query feedback. Please consider setting feedback-probability to 0.0 in config file to disable query feedback"))
			return original, nil
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMergeJoin, Value: BoolToOnOff(DefTiDBEnableIndexMergeJoin), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTrackAggregateMemoryUsage, Value: BoolToOnOff(DefTiDBTrackAggregateMemoryUsage), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMultiStatementMode, Value: Off, Type: TypeEnum, PossibleValues: []string{Off, On, Warn}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExchangePartition, Value: BoolToOnOff(DefTiDBEnableExchangePartition), Type: TypeBool},

	/* tikv gc metrics */
	{Scope: ScopeGlobal, Name: TiDBGCEnable, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBGCRunInterval, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBGCLifetime, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBGCConcurrency, Value: "-1", Type: TypeInt, MinValue: 1, MaxValue: 128, AllowAutoValue: true},
	{Scope: ScopeGlobal, Name: TiDBGCScanLockMode, Value: "PHYSICAL", Type: TypeEnum, PossibleValues: []string{"PHYSICAL", "LEGACY"}},
=======
	{ScopeGlobal, "gtid_mode", "OFF"},
	{ScopeGlobal, FlushTime, "0"},
	{ScopeSession, PseudoSlaveMode, ""},
	{ScopeNone, "performance_schema_max_mutex_classes", "200"},
	{ScopeGlobal | ScopeSession, LowPriorityUpdates, "0"},
	{ScopeGlobal | ScopeSession, SessionTrackGtids, "OFF"},
	{ScopeGlobal | ScopeSession, "ndbinfo_max_rows", ""},
	{ScopeGlobal | ScopeSession, "ndb_index_stat_option", ""},
	{ScopeGlobal | ScopeSession, OldPasswords, "0"},
	{ScopeNone, "innodb_version", "5.6.25"},
	{ScopeGlobal, MaxConnections, "151"},
	{ScopeGlobal | ScopeSession, BigTables, "0"},
	{ScopeNone, "skip_external_locking", "1"},
	{ScopeGlobal, "slave_pending_jobs_size_max", "16777216"},
	{ScopeNone, "innodb_sync_array_size", "1"},
	{ScopeSession, "rand_seed2", ""},
	{ScopeGlobal, ValidatePasswordCheckUserName, "0"},
	{ScopeGlobal, "validate_password_number_count", "1"},
	{ScopeSession, "gtid_next", ""},
	{ScopeGlobal | ScopeSession, SQLSelectLimit, "18446744073709551615"},
	{ScopeGlobal, "ndb_show_foreign_key_mock_tables", ""},
	{ScopeNone, "multi_range_count", "256"},
	{ScopeGlobal | ScopeSession, DefaultWeekFormat, "0"},
	{ScopeGlobal | ScopeSession, "binlog_error_action", "IGNORE_ERROR"},
	{ScopeGlobal, "slave_transaction_retries", "10"},
	{ScopeGlobal | ScopeSession, "default_storage_engine", "InnoDB"},
	{ScopeNone, "ft_query_expansion_limit", "20"},
	{ScopeGlobal, MaxConnectErrors, "100"},
	{ScopeGlobal, SyncBinlog, "0"},
	{ScopeNone, "max_digest_length", "1024"},
	{ScopeNone, "innodb_force_load_corrupted", "0"},
	{ScopeNone, "performance_schema_max_table_handles", "4000"},
	{ScopeGlobal, InnodbFastShutdown, "1"},
	{ScopeNone, "ft_max_word_len", "84"},
	{ScopeGlobal, "log_backward_compatible_user_definitions", ""},
	{ScopeNone, "lc_messages_dir", "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/"},
	{ScopeGlobal, "ft_boolean_syntax", "+ -><()~*:\"\"&|"},
	{ScopeGlobal, TableDefinitionCache, "-1"},
	{ScopeNone, SkipNameResolve, "0"},
	{ScopeNone, "performance_schema_max_file_handles", "32768"},
	{ScopeSession, "transaction_allow_batching", ""},
	{ScopeGlobal | ScopeSession, SQLModeVar, mysql.DefaultSQLMode},
	{ScopeNone, "performance_schema_max_statement_classes", "168"},
	{ScopeGlobal, "server_id", "0"},
	{ScopeGlobal, "innodb_flushing_avg_loops", "30"},
	{ScopeGlobal | ScopeSession, TmpTableSize, "16777216"},
	{ScopeGlobal, "innodb_max_purge_lag", "0"},
	{ScopeGlobal | ScopeSession, "preload_buffer_size", "32768"},
	{ScopeGlobal, "slave_checkpoint_period", "300"},
	{ScopeGlobal, CheckProxyUsers, "0"},
	{ScopeNone, "have_query_cache", "YES"},
	{ScopeGlobal, "innodb_flush_log_at_timeout", "1"},
	{ScopeGlobal, "innodb_max_undo_log_size", ""},
	{ScopeGlobal | ScopeSession, "range_alloc_block_size", "4096"},
	{ScopeGlobal, ConnectTimeout, "10"},
	{ScopeGlobal | ScopeSession, MaxExecutionTime, "0"},
	{ScopeGlobal | ScopeSession, CollationServer, mysql.DefaultCollationName},
	{ScopeNone, "have_rtree_keys", "YES"},
	{ScopeGlobal, "innodb_old_blocks_pct", "37"},
	{ScopeGlobal, "innodb_file_format", "Antelope"},
	{ScopeGlobal, "innodb_compression_failure_threshold_pct", "5"},
	{ScopeNone, "performance_schema_events_waits_history_long_size", "10000"},
	{ScopeGlobal, "innodb_checksum_algorithm", "innodb"},
	{ScopeNone, "innodb_ft_sort_pll_degree", "2"},
	{ScopeNone, "thread_stack", "262144"},
	{ScopeGlobal, "relay_log_info_repository", "FILE"},
	{ScopeGlobal | ScopeSession, SQLLogBin, "1"},
	{ScopeGlobal, SuperReadOnly, "0"},
	{ScopeGlobal | ScopeSession, "max_delayed_threads", "20"},
	{ScopeNone, "protocol_version", "10"},
	{ScopeGlobal | ScopeSession, "new", "OFF"},
	{ScopeGlobal | ScopeSession, "myisam_sort_buffer_size", "8388608"},
	{ScopeGlobal | ScopeSession, "optimizer_trace_offset", "-1"},
	{ScopeGlobal, InnodbBufferPoolDumpAtShutdown, "0"},
	{ScopeGlobal | ScopeSession, SQLNotes, "1"},
	{ScopeGlobal, InnodbCmpPerIndexEnabled, "0"},
	{ScopeGlobal, "innodb_ft_server_stopword_table", ""},
	{ScopeNone, "performance_schema_max_file_instances", "7693"},
	{ScopeNone, "log_output", "FILE"},
	{ScopeGlobal, "binlog_group_commit_sync_delay", ""},
	{ScopeGlobal, "binlog_group_commit_sync_no_delay_count", ""},
	{ScopeNone, "have_crypt", "YES"},
	{ScopeGlobal, "innodb_log_write_ahead_size", ""},
	{ScopeNone, "innodb_log_group_home_dir", "./"},
	{ScopeNone, "performance_schema_events_statements_history_size", "10"},
	{ScopeGlobal, GeneralLog, "0"},
	{ScopeGlobal, "validate_password_dictionary_file", ""},
	{ScopeGlobal, BinlogOrderCommits, "1"},
	{ScopeGlobal, MasterVerifyChecksum, "0"},
	{ScopeGlobal, "key_cache_division_limit", "100"},
	{ScopeGlobal, "rpl_semi_sync_master_trace_level", ""},
	{ScopeGlobal | ScopeSession, "max_insert_delayed_threads", "20"},
	{ScopeNone, "performance_schema_session_connect_attrs_size", "512"},
	{ScopeGlobal | ScopeSession, "time_zone", "SYSTEM"},
	{ScopeGlobal, "innodb_max_dirty_pages_pct", "75"},
	{ScopeGlobal, InnodbFilePerTable, "1"},
	{ScopeGlobal, InnodbLogCompressedPages, "1"},
	{ScopeGlobal, "master_info_repository", "FILE"},
	{ScopeGlobal, "rpl_stop_slave_timeout", "31536000"},
	{ScopeNone, "skip_networking", "0"},
	{ScopeGlobal, "innodb_monitor_reset", ""},
	{ScopeNone, "have_ssl", "DISABLED"},
	{ScopeNone, "have_openssl", "DISABLED"},
	{ScopeNone, "ssl_ca", ""},
	{ScopeNone, "ssl_cert", ""},
	{ScopeNone, "ssl_key", ""},
	{ScopeNone, "ssl_cipher", ""},
	{ScopeNone, "tls_version", "TLSv1,TLSv1.1,TLSv1.2"},
	{ScopeNone, "system_time_zone", "CST"},
	{ScopeGlobal, InnodbPrintAllDeadlocks, "0"},
	{ScopeNone, "innodb_autoinc_lock_mode", "1"},
	{ScopeGlobal, "slave_net_timeout", "3600"},
	{ScopeGlobal, "key_buffer_size", "8388608"},
	{ScopeGlobal | ScopeSession, ForeignKeyChecks, "OFF"},
	{ScopeGlobal, "host_cache_size", "279"},
	{ScopeGlobal, DelayKeyWrite, "ON"},
	{ScopeNone, "metadata_locks_cache_size", "1024"},
	{ScopeNone, "innodb_force_recovery", "0"},
	{ScopeGlobal, "innodb_file_format_max", "Antelope"},
	{ScopeGlobal | ScopeSession, "debug", ""},
	{ScopeGlobal, "log_warnings", "1"},
	{ScopeGlobal, OfflineMode, "0"},
	{ScopeGlobal | ScopeSession, InnodbStrictMode, "1"},
	{ScopeGlobal, "innodb_rollback_segments", "128"},
	{ScopeGlobal | ScopeSession, "join_buffer_size", "262144"},
	{ScopeNone, "innodb_mirrored_log_groups", "1"},
	{ScopeGlobal, "max_binlog_size", "1073741824"},
	{ScopeGlobal, "sync_master_info", "10000"},
	{ScopeGlobal, "concurrent_insert", "AUTO"},
	{ScopeGlobal, InnodbAdaptiveHashIndex, "1"},
	{ScopeGlobal, InnodbFtEnableStopword, "1"},
	{ScopeGlobal, "general_log_file", "/usr/local/mysql/data/localhost.log"},
	{ScopeGlobal | ScopeSession, InnodbSupportXA, "1"},
	{ScopeGlobal, "innodb_compression_level", "6"},
	{ScopeNone, "innodb_file_format_check", "1"},
	{ScopeNone, "myisam_mmap_size", "18446744073709551615"},
	{ScopeGlobal, "init_slave", ""},
	{ScopeNone, "innodb_buffer_pool_instances", "8"},
	{ScopeGlobal | ScopeSession, BlockEncryptionMode, "aes-128-ecb"},
	{ScopeGlobal | ScopeSession, "max_length_for_sort_data", "1024"},
	{ScopeNone, "character_set_system", "utf8"},
	{ScopeGlobal | ScopeSession, InteractiveTimeout, "28800"},
	{ScopeGlobal, InnodbOptimizeFullTextOnly, "0"},
	{ScopeNone, "character_sets_dir", "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/charsets/"},
	{ScopeGlobal | ScopeSession, QueryCacheType, "OFF"},
	{ScopeNone, "innodb_rollback_on_timeout", "0"},
	{ScopeGlobal | ScopeSession, "query_alloc_block_size", "8192"},
	{ScopeGlobal, SlaveCompressedProtocol, "0"},
	{ScopeGlobal | ScopeSession, InitConnect, ""},
	{ScopeGlobal, "rpl_semi_sync_slave_trace_level", ""},
	{ScopeNone, "have_compress", "YES"},
	{ScopeNone, "thread_concurrency", "10"},
	{ScopeGlobal | ScopeSession, "query_prealloc_size", "8192"},
	{ScopeNone, "relay_log_space_limit", "0"},
	{ScopeGlobal | ScopeSession, MaxUserConnections, "0"},
	{ScopeNone, "performance_schema_max_thread_classes", "50"},
	{ScopeGlobal, "innodb_api_trx_level", "0"},
	{ScopeNone, "disconnect_on_expired_password", "1"},
	{ScopeNone, "performance_schema_max_file_classes", "50"},
	{ScopeGlobal, "expire_logs_days", "0"},
	{ScopeGlobal | ScopeSession, BinlogRowQueryLogEvents, "0"},
	{ScopeGlobal, "default_password_lifetime", ""},
	{ScopeNone, "pid_file", "/usr/local/mysql/data/localhost.pid"},
	{ScopeNone, "innodb_undo_tablespaces", "0"},
	{ScopeGlobal, InnodbStatusOutputLocks, "0"},
	{ScopeNone, "performance_schema_accounts_size", "100"},
	{ScopeGlobal | ScopeSession, "max_error_count", "64"},
	{ScopeGlobal, "max_write_lock_count", "18446744073709551615"},
	{ScopeNone, "performance_schema_max_socket_instances", "322"},
	{ScopeNone, "performance_schema_max_table_instances", "12500"},
	{ScopeGlobal, "innodb_stats_persistent_sample_pages", "20"},
	{ScopeGlobal, "show_compatibility_56", ""},
	{ScopeGlobal, LogSlowSlaveStatements, "0"},
	{ScopeNone, "innodb_open_files", "2000"},
	{ScopeGlobal, "innodb_spin_wait_delay", "6"},
	{ScopeGlobal, "thread_cache_size", "9"},
	{ScopeGlobal, LogSlowAdminStatements, "0"},
	{ScopeNone, "innodb_checksums", "ON"},
	{ScopeNone, "hostname", ServerHostname},
	{ScopeGlobal | ScopeSession, "auto_increment_offset", "1"},
	{ScopeNone, "ft_stopword_file", "(built-in)"},
	{ScopeGlobal, "innodb_max_dirty_pages_pct_lwm", "0"},
	{ScopeGlobal, LogQueriesNotUsingIndexes, "0"},
	{ScopeSession, "timestamp", ""},
	{ScopeGlobal | ScopeSession, QueryCacheWlockInvalidate, "0"},
	{ScopeGlobal | ScopeSession, "sql_buffer_result", "OFF"},
	{ScopeGlobal | ScopeSession, "character_set_filesystem", "binary"},
	{ScopeGlobal | ScopeSession, "collation_database", mysql.DefaultCollationName},
	{ScopeGlobal | ScopeSession, AutoIncrementIncrement, strconv.FormatInt(DefAutoIncrementIncrement, 10)},
	{ScopeGlobal | ScopeSession, AutoIncrementOffset, strconv.FormatInt(DefAutoIncrementOffset, 10)},
	{ScopeGlobal | ScopeSession, "max_heap_table_size", "16777216"},
	{ScopeGlobal | ScopeSession, "div_precision_increment", "4"},
	{ScopeGlobal, "innodb_lru_scan_depth", "1024"},
	{ScopeGlobal, "innodb_purge_rseg_truncate_frequency", ""},
	{ScopeGlobal | ScopeSession, SQLAutoIsNull, "0"},
	{ScopeNone, "innodb_api_enable_binlog", "0"},
	{ScopeGlobal | ScopeSession, "innodb_ft_user_stopword_table", ""},
	{ScopeNone, "server_id_bits", "32"},
	{ScopeGlobal, "innodb_log_checksum_algorithm", ""},
	{ScopeNone, "innodb_buffer_pool_load_at_startup", "1"},
	{ScopeGlobal | ScopeSession, "sort_buffer_size", "262144"},
	{ScopeGlobal, "innodb_flush_neighbors", "1"},
	{ScopeNone, "innodb_use_sys_malloc", "1"},
	{ScopeSession, PluginLoad, ""},
	{ScopeSession, PluginDir, "/data/deploy/plugin"},
	{ScopeNone, "performance_schema_max_socket_classes", "10"},
	{ScopeNone, "performance_schema_max_stage_classes", "150"},
	{ScopeGlobal, "innodb_purge_batch_size", "300"},
	{ScopeNone, "have_profiling", "NO"},
	{ScopeGlobal, "slave_checkpoint_group", "512"},
	{ScopeGlobal | ScopeSession, "character_set_client", mysql.DefaultCharset},
	{ScopeNone, "slave_load_tmpdir", "/var/tmp/"},
	{ScopeGlobal, InnodbBufferPoolDumpNow, "0"},
	{ScopeGlobal, RelayLogPurge, "1"},
	{ScopeGlobal, "ndb_distribution", ""},
	{ScopeGlobal, "myisam_data_pointer_size", "6"},
	{ScopeGlobal, "ndb_optimization_delay", ""},
	{ScopeGlobal, "innodb_ft_num_word_optimize", "2000"},
	{ScopeGlobal | ScopeSession, "max_join_size", "18446744073709551615"},
	{ScopeNone, CoreFile, "0"},
	{ScopeGlobal | ScopeSession, "max_seeks_for_key", "18446744073709551615"},
	{ScopeNone, "innodb_log_buffer_size", "8388608"},
	{ScopeGlobal, "delayed_insert_timeout", "300"},
	{ScopeGlobal, "max_relay_log_size", "0"},
	{ScopeGlobal | ScopeSession, MaxSortLength, "1024"},
	{ScopeNone, "metadata_locks_hash_instances", "8"},
	{ScopeGlobal, "ndb_eventbuffer_free_percent", ""},
	{ScopeNone, "large_files_support", "1"},
	{ScopeGlobal, "binlog_max_flush_queue_time", "0"},
	{ScopeGlobal, "innodb_fill_factor", ""},
	{ScopeGlobal, "log_syslog_facility", ""},
	{ScopeNone, "innodb_ft_min_token_size", "3"},
	{ScopeGlobal | ScopeSession, "transaction_write_set_extraction", ""},
	{ScopeGlobal | ScopeSession, "ndb_blob_write_batch_bytes", ""},
	{ScopeGlobal, "automatic_sp_privileges", "1"},
	{ScopeGlobal, "innodb_flush_sync", ""},
	{ScopeNone, "performance_schema_events_statements_history_long_size", "10000"},
	{ScopeGlobal, "innodb_monitor_disable", ""},
	{ScopeNone, "innodb_doublewrite", "1"},
	{ScopeGlobal, "slave_parallel_type", ""},
	{ScopeNone, "log_bin_use_v1_row_events", "0"},
	{ScopeSession, "innodb_optimize_point_storage", ""},
	{ScopeNone, "innodb_api_disable_rowlock", "0"},
	{ScopeGlobal, "innodb_adaptive_flushing_lwm", "10"},
	{ScopeNone, "innodb_log_files_in_group", "2"},
	{ScopeGlobal, InnodbBufferPoolLoadNow, "0"},
	{ScopeNone, "performance_schema_max_rwlock_classes", "40"},
	{ScopeNone, "binlog_gtid_simple_recovery", "1"},
	{ScopeNone, Port, "4000"},
	{ScopeNone, "performance_schema_digests_size", "10000"},
	{ScopeGlobal | ScopeSession, Profiling, "0"},
	{ScopeNone, "lower_case_table_names", "2"},
	{ScopeSession, "rand_seed1", ""},
	{ScopeGlobal, "sha256_password_proxy_users", ""},
	{ScopeGlobal | ScopeSession, SQLQuoteShowCreate, "1"},
	{ScopeGlobal | ScopeSession, "binlogging_impossible_mode", "IGNORE_ERROR"},
	{ScopeGlobal | ScopeSession, QueryCacheSize, "1048576"},
	{ScopeGlobal, "innodb_stats_transient_sample_pages", "8"},
	{ScopeGlobal, InnodbStatsOnMetadata, "0"},
	{ScopeNone, "server_uuid", "00000000-0000-0000-0000-000000000000"},
	{ScopeNone, "open_files_limit", "5000"},
	{ScopeGlobal | ScopeSession, "ndb_force_send", ""},
	{ScopeNone, "skip_show_database", "0"},
	{ScopeGlobal, "log_timestamps", ""},
	{ScopeNone, "version_compile_machine", "x86_64"},
	{ScopeGlobal, "slave_parallel_workers", "0"},
	{ScopeGlobal, "event_scheduler", "OFF"},
	{ScopeGlobal | ScopeSession, "ndb_deferred_constraints", ""},
	{ScopeGlobal, "log_syslog_include_pid", ""},
	{ScopeSession, "last_insert_id", ""},
	{ScopeNone, "innodb_ft_cache_size", "8000000"},
	{ScopeNone, LogBin, "0"},
	{ScopeGlobal, InnodbDisableSortFileCache, "0"},
	{ScopeGlobal, "log_error_verbosity", ""},
	{ScopeNone, "performance_schema_hosts_size", "100"},
	{ScopeGlobal, "innodb_replication_delay", "0"},
	{ScopeGlobal, SlowQueryLog, "0"},
	{ScopeSession, "debug_sync", ""},
	{ScopeGlobal, InnodbStatsAutoRecalc, "1"},
	{ScopeGlobal | ScopeSession, "lc_messages", "en_US"},
	{ScopeGlobal | ScopeSession, "bulk_insert_buffer_size", "8388608"},
	{ScopeGlobal | ScopeSession, BinlogDirectNonTransactionalUpdates, "0"},
	{ScopeGlobal, "innodb_change_buffering", "all"},
	{ScopeGlobal | ScopeSession, SQLBigSelects, "1"},
	{ScopeGlobal | ScopeSession, CharacterSetResults, mysql.DefaultCharset},
	{ScopeGlobal, "innodb_max_purge_lag_delay", "0"},
	{ScopeGlobal | ScopeSession, "session_track_schema", ""},
	{ScopeGlobal, "innodb_io_capacity_max", "2000"},
	{ScopeGlobal, "innodb_autoextend_increment", "64"},
	{ScopeGlobal | ScopeSession, "binlog_format", "STATEMENT"},
	{ScopeGlobal | ScopeSession, "optimizer_trace", "enabled=off,one_line=off"},
	{ScopeGlobal | ScopeSession, "read_rnd_buffer_size", "262144"},
	{ScopeNone, "version_comment", "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 5.7 compatible"},
	{ScopeGlobal | ScopeSession, NetWriteTimeout, "60"},
	{ScopeGlobal, InnodbBufferPoolLoadAbort, "0"},
	{ScopeGlobal | ScopeSession, TxnIsolation, "REPEATABLE-READ"},
	{ScopeGlobal | ScopeSession, TransactionIsolation, "REPEATABLE-READ"},
	{ScopeGlobal | ScopeSession, "collation_connection", mysql.DefaultCollationName},
	{ScopeGlobal, "rpl_semi_sync_master_timeout", ""},
	{ScopeGlobal | ScopeSession, "transaction_prealloc_size", "4096"},
	{ScopeNone, "slave_skip_errors", "OFF"},
	{ScopeNone, "performance_schema_setup_objects_size", "100"},
	{ScopeGlobal, "sync_relay_log", "10000"},
	{ScopeGlobal, "innodb_ft_result_cache_limit", "2000000000"},
	{ScopeNone, "innodb_sort_buffer_size", "1048576"},
	{ScopeGlobal, "innodb_ft_enable_diag_print", "OFF"},
	{ScopeNone, "thread_handling", "one-thread-per-connection"},
	{ScopeGlobal, "stored_program_cache", "256"},
	{ScopeNone, "performance_schema_max_mutex_instances", "15906"},
	{ScopeGlobal, "innodb_adaptive_max_sleep_delay", "150000"},
	{ScopeNone, "large_pages", "OFF"},
	{ScopeGlobal | ScopeSession, "session_track_system_variables", ""},
	{ScopeGlobal, "innodb_change_buffer_max_size", "25"},
	{ScopeGlobal, LogBinTrustFunctionCreators, "0"},
	{ScopeNone, "innodb_write_io_threads", "4"},
	{ScopeGlobal, "mysql_native_password_proxy_users", ""},
	{ScopeGlobal, serverReadOnly, "0"},
	{ScopeNone, "large_page_size", "0"},
	{ScopeNone, "table_open_cache_instances", "1"},
	{ScopeGlobal, InnodbStatsPersistent, "1"},
	{ScopeGlobal | ScopeSession, "session_track_state_change", ""},
	{ScopeNone, "optimizer_switch", "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,subquery_materialization_cost_based=on,use_index_extensions=on"},
	{ScopeGlobal, "delayed_queue_size", "1000"},
	{ScopeNone, "innodb_read_only", "0"},
	{ScopeNone, "datetime_format", "%Y-%m-%d %H:%i:%s"},
	{ScopeGlobal, "log_syslog", ""},
	{ScopeNone, "version", mysql.ServerVersion},
	{ScopeGlobal | ScopeSession, "transaction_alloc_block_size", "8192"},
	{ScopeGlobal, "sql_slave_skip_counter", "0"},
	{ScopeGlobal, "innodb_large_prefix", "OFF"},
	{ScopeNone, "performance_schema_max_cond_classes", "80"},
	{ScopeGlobal, "innodb_io_capacity", "200"},
	{ScopeGlobal, "max_binlog_cache_size", "18446744073709547520"},
	{ScopeGlobal | ScopeSession, "ndb_index_stat_enable", ""},
	{ScopeGlobal, "executed_gtids_compression_period", ""},
	{ScopeNone, "time_format", "%H:%i:%s"},
	{ScopeGlobal | ScopeSession, OldAlterTable, "0"},
	{ScopeGlobal | ScopeSession, "long_query_time", "10.000000"},
	{ScopeNone, "innodb_use_native_aio", "0"},
	{ScopeGlobal, "log_throttle_queries_not_using_indexes", "0"},
	{ScopeNone, "locked_in_memory", "0"},
	{ScopeNone, "innodb_api_enable_mdl", "0"},
	{ScopeGlobal, "binlog_cache_size", "32768"},
	{ScopeGlobal, "innodb_compression_pad_pct_max", "50"},
	{ScopeGlobal, InnodbCommitConcurrency, "0"},
	{ScopeNone, "ft_min_word_len", "4"},
	{ScopeGlobal, EnforceGtidConsistency, "OFF"},
	{ScopeGlobal, SecureAuth, "1"},
	{ScopeNone, "max_tmp_tables", "32"},
	{ScopeGlobal, InnodbRandomReadAhead, "0"},
	{ScopeGlobal | ScopeSession, UniqueChecks, "1"},
	{ScopeGlobal, "internal_tmp_disk_storage_engine", ""},
	{ScopeGlobal | ScopeSession, "myisam_repair_threads", "1"},
	{ScopeGlobal, "ndb_eventbuffer_max_alloc", ""},
	{ScopeGlobal, "innodb_read_ahead_threshold", "56"},
	{ScopeGlobal, "key_cache_block_size", "1024"},
	{ScopeGlobal, "rpl_semi_sync_slave_enabled", ""},
	{ScopeNone, "ndb_recv_thread_cpu_mask", ""},
	{ScopeGlobal, "gtid_purged", ""},
	{ScopeGlobal, "max_binlog_stmt_cache_size", "18446744073709547520"},
	{ScopeGlobal | ScopeSession, "lock_wait_timeout", "31536000"},
	{ScopeGlobal | ScopeSession, "read_buffer_size", "131072"},
	{ScopeNone, "innodb_read_io_threads", "4"},
	{ScopeGlobal | ScopeSession, MaxSpRecursionDepth, "0"},
	{ScopeNone, "ignore_builtin_innodb", "0"},
	{ScopeGlobal, "rpl_semi_sync_master_enabled", ""},
	{ScopeGlobal, "slow_query_log_file", "/usr/local/mysql/data/localhost-slow.log"},
	{ScopeGlobal, "innodb_thread_sleep_delay", "10000"},
	{ScopeNone, "license", "Apache License 2.0"},
	{ScopeGlobal, "innodb_ft_aux_table", ""},
	{ScopeGlobal | ScopeSession, SQLWarnings, "0"},
	{ScopeGlobal | ScopeSession, KeepFilesOnCreate, "0"},
	{ScopeGlobal, "slave_preserve_commit_order", ""},
	{ScopeNone, "innodb_data_file_path", "ibdata1:12M:autoextend"},
	{ScopeNone, "performance_schema_setup_actors_size", "100"},
	{ScopeNone, "innodb_additional_mem_pool_size", "8388608"},
	{ScopeNone, "log_error", "/usr/local/mysql/data/localhost.err"},
	{ScopeGlobal, "slave_exec_mode", "STRICT"},
	{ScopeGlobal, "binlog_stmt_cache_size", "32768"},
	{ScopeNone, "relay_log_info_file", "relay-log.info"},
	{ScopeNone, "innodb_ft_total_cache_size", "640000000"},
	{ScopeNone, "performance_schema_max_rwlock_instances", "9102"},
	{ScopeGlobal, "table_open_cache", "2000"},
	{ScopeNone, "log_slave_updates", "0"},
	{ScopeNone, "performance_schema_events_stages_history_long_size", "10000"},
	{ScopeGlobal | ScopeSession, AutoCommit, "1"},
	{ScopeSession, "insert_id", ""},
	{ScopeGlobal | ScopeSession, "default_tmp_storage_engine", "InnoDB"},
	{ScopeGlobal | ScopeSession, "optimizer_search_depth", "62"},
	{ScopeGlobal, "max_points_in_geometry", ""},
	{ScopeGlobal, "innodb_stats_sample_pages", "8"},
	{ScopeGlobal | ScopeSession, "profiling_history_size", "15"},
	{ScopeGlobal | ScopeSession, "character_set_database", mysql.DefaultCharset},
	{ScopeNone, "have_symlink", "YES"},
	{ScopeGlobal | ScopeSession, "storage_engine", "InnoDB"},
	{ScopeGlobal | ScopeSession, "sql_log_off", "0"},
	// In MySQL, the default value of `explicit_defaults_for_timestamp` is `0`.
	// But In TiDB, it's set to `1` to be consistent with TiDB timestamp behavior.
	// See: https://github.com/pingcap/tidb/pull/6068 for details
	{ScopeNone, "explicit_defaults_for_timestamp", "1"},
	{ScopeNone, "performance_schema_events_waits_history_size", "10"},
	{ScopeGlobal, "log_syslog_tag", ""},
	{ScopeGlobal | ScopeSession, TxReadOnly, "0"},
	{ScopeGlobal | ScopeSession, TransactionReadOnly, "0"},
	{ScopeGlobal, "rpl_semi_sync_master_wait_point", ""},
	{ScopeGlobal, "innodb_undo_log_truncate", ""},
	{ScopeSession, "innodb_create_intrinsic", ""},
	{ScopeGlobal, "gtid_executed_compression_period", ""},
	{ScopeGlobal, "ndb_log_empty_epochs", ""},
	{ScopeGlobal, MaxPreparedStmtCount, strconv.FormatInt(DefMaxPreparedStmtCount, 10)},
	{ScopeNone, "have_geometry", "YES"},
	{ScopeGlobal | ScopeSession, "optimizer_trace_max_mem_size", "16384"},
	{ScopeGlobal | ScopeSession, "net_retry_count", "10"},
	{ScopeSession, "ndb_table_no_logging", ""},
	{ScopeGlobal | ScopeSession, "optimizer_trace_features", "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on"},
	{ScopeGlobal, "innodb_flush_log_at_trx_commit", "1"},
	{ScopeGlobal, "rewriter_enabled", ""},
	{ScopeGlobal, "query_cache_min_res_unit", "4096"},
	{ScopeGlobal | ScopeSession, "updatable_views_with_limit", "YES"},
	{ScopeGlobal | ScopeSession, "optimizer_prune_level", "1"},
	{ScopeGlobal, "slave_sql_verify_checksum", "1"},
	{ScopeGlobal | ScopeSession, "completion_type", "NO_CHAIN"},
	{ScopeGlobal, "binlog_checksum", "CRC32"},
	{ScopeNone, "report_port", "3306"},
	{ScopeGlobal | ScopeSession, ShowOldTemporals, "0"},
	{ScopeGlobal, "query_cache_limit", "1048576"},
	{ScopeGlobal, "innodb_buffer_pool_size", "134217728"},
	{ScopeGlobal, InnodbAdaptiveFlushing, "1"},
	{ScopeNone, "datadir", "/usr/local/mysql/data/"},
	{ScopeGlobal | ScopeSession, WaitTimeout, strconv.FormatInt(DefWaitTimeout, 10)},
	{ScopeGlobal, "innodb_monitor_enable", ""},
	{ScopeNone, "date_format", "%Y-%m-%d"},
	{ScopeGlobal, "innodb_buffer_pool_filename", "ib_buffer_pool"},
	{ScopeGlobal, "slow_launch_time", "2"},
	{ScopeGlobal, "slave_max_allowed_packet", "1073741824"},
	{ScopeGlobal | ScopeSession, "ndb_use_transactions", ""},
	{ScopeNone, "innodb_purge_threads", "1"},
	{ScopeGlobal, "innodb_concurrency_tickets", "5000"},
	{ScopeGlobal, "innodb_monitor_reset_all", ""},
	{ScopeNone, "performance_schema_users_size", "100"},
	{ScopeGlobal, "ndb_log_updated_only", ""},
	{ScopeNone, "basedir", "/usr/local/mysql"},
	{ScopeGlobal, "innodb_old_blocks_time", "1000"},
	{ScopeGlobal, "innodb_stats_method", "nulls_equal"},
	{ScopeGlobal | ScopeSession, InnodbLockWaitTimeout, strconv.FormatInt(DefInnodbLockWaitTimeout, 10)},
	{ScopeGlobal, LocalInFile, "1"},
	{ScopeGlobal | ScopeSession, "myisam_stats_method", "nulls_unequal"},
	{ScopeNone, "version_compile_os", "osx10.8"},
	{ScopeNone, "relay_log_recovery", "0"},
	{ScopeNone, "old", "0"},
	{ScopeGlobal | ScopeSession, InnodbTableLocks, "1"},
	{ScopeNone, PerformanceSchema, "0"},
	{ScopeNone, "myisam_recover_options", "OFF"},
	{ScopeGlobal | ScopeSession, NetBufferLength, "16384"},
	{ScopeGlobal, "rpl_semi_sync_master_wait_for_slave_count", ""},
	{ScopeGlobal | ScopeSession, "binlog_row_image", "FULL"},
	{ScopeNone, "innodb_locks_unsafe_for_binlog", "0"},
	{ScopeSession, "rbr_exec_mode", ""},
	{ScopeGlobal, "myisam_max_sort_file_size", "9223372036853727232"},
	{ScopeNone, "back_log", "80"},
	{ScopeNone, "lower_case_file_system", "1"},
	{ScopeGlobal, "rpl_semi_sync_master_wait_no_slave", ""},
	{ScopeGlobal | ScopeSession, GroupConcatMaxLen, "1024"},
	{ScopeSession, "pseudo_thread_id", ""},
	{ScopeNone, "socket", "/tmp/myssock"},
	{ScopeNone, "have_dynamic_loading", "YES"},
	{ScopeGlobal, "rewriter_verbose", ""},
	{ScopeGlobal, "innodb_undo_logs", "128"},
	{ScopeNone, "performance_schema_max_cond_instances", "3504"},
	{ScopeGlobal, "delayed_insert_limit", "100"},
	{ScopeGlobal, Flush, "0"},
	{ScopeGlobal | ScopeSession, "eq_range_index_dive_limit", "10"},
	{ScopeNone, "performance_schema_events_stages_history_size", "10"},
	{ScopeGlobal | ScopeSession, "character_set_connection", mysql.DefaultCharset},
	{ScopeGlobal, MyISAMUseMmap, "0"},
	{ScopeGlobal | ScopeSession, "ndb_join_pushdown", ""},
	{ScopeGlobal | ScopeSession, CharacterSetServer, mysql.DefaultCharset},
	{ScopeGlobal, "validate_password_special_char_count", "1"},
	{ScopeNone, "performance_schema_max_thread_instances", "402"},
	{ScopeGlobal, "slave_rows_search_algorithms", "TABLE_SCAN,INDEX_SCAN"},
	{ScopeGlobal | ScopeSession, "ndbinfo_show_hidden", ""},
	{ScopeGlobal | ScopeSession, "net_read_timeout", "30"},
	{ScopeNone, "innodb_page_size", "16384"},
	{ScopeGlobal | ScopeSession, MaxAllowedPacket, "67108864"},
	{ScopeNone, "innodb_log_file_size", "50331648"},
	{ScopeGlobal, "sync_relay_log_info", "10000"},
	{ScopeGlobal | ScopeSession, "optimizer_trace_limit", "1"},
	{ScopeNone, "innodb_ft_max_token_size", "84"},
	{ScopeGlobal, "validate_password_length", "8"},
	{ScopeGlobal, "ndb_log_binlog_index", ""},
	{ScopeGlobal, "innodb_api_bk_commit_interval", "5"},
	{ScopeNone, "innodb_undo_directory", "."},
	{ScopeNone, "bind_address", "*"},
	{ScopeGlobal, "innodb_sync_spin_loops", "30"},
	{ScopeGlobal | ScopeSession, SQLSafeUpdates, "0"},
	{ScopeNone, "tmpdir", "/var/tmp/"},
	{ScopeGlobal, "innodb_thread_concurrency", "0"},
	{ScopeGlobal, SlaveAllowBatching, "0"},
	{ScopeGlobal, "innodb_buffer_pool_dump_pct", ""},
	{ScopeGlobal | ScopeSession, "lc_time_names", "en_US"},
	{ScopeGlobal | ScopeSession, "max_statement_time", ""},
	{ScopeGlobal | ScopeSession, EndMakersInJSON, "0"},
	{ScopeGlobal, AvoidTemporalUpgrade, "0"},
	{ScopeGlobal, "key_cache_age_threshold", "300"},
	{ScopeGlobal, InnodbStatusOutput, "0"},
	{ScopeSession, "identity", ""},
	{ScopeGlobal | ScopeSession, "min_examined_row_limit", "0"},
	{ScopeGlobal, "sync_frm", "ON"},
	{ScopeGlobal, "innodb_online_alter_log_max_size", "134217728"},
	{ScopeSession, WarningCount, "0"},
	{ScopeSession, ErrorCount, "0"},
	{ScopeGlobal | ScopeSession, "information_schema_stats_expiry", "86400"},
	{ScopeGlobal, "thread_pool_size", "16"},
	{ScopeGlobal | ScopeSession, WindowingUseHighPrecision, "ON"},
	/* TiDB specific variables */
	{ScopeSession, TiDBSnapshot, ""},
	{ScopeSession, TiDBOptAggPushDown, BoolToIntStr(DefOptAggPushDown)},
	{ScopeGlobal | ScopeSession, TiDBOptBCJ, BoolToIntStr(DefOptBCJ)},
	{ScopeSession, TiDBOptDistinctAggPushDown, BoolToIntStr(config.GetGlobalConfig().Performance.DistinctAggPushDown)},
	{ScopeSession, TiDBOptWriteRowID, BoolToIntStr(DefOptWriteRowID)},
	{ScopeGlobal | ScopeSession, TiDBBuildStatsConcurrency, strconv.Itoa(DefBuildStatsConcurrency)},
	{ScopeGlobal, TiDBAutoAnalyzeRatio, strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64)},
	{ScopeGlobal, TiDBAutoAnalyzeStartTime, DefAutoAnalyzeStartTime},
	{ScopeGlobal, TiDBAutoAnalyzeEndTime, DefAutoAnalyzeEndTime},
	{ScopeSession, TiDBChecksumTableConcurrency, strconv.Itoa(DefChecksumTableConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBDistSQLScanConcurrency, strconv.Itoa(DefDistSQLScanConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBOptInSubqToJoinAndAgg, BoolToIntStr(DefOptInSubqToJoinAndAgg)},
	{ScopeGlobal | ScopeSession, TiDBOptCorrelationThreshold, strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptCorrelationExpFactor, strconv.Itoa(DefOptCorrelationExpFactor)},
	{ScopeGlobal | ScopeSession, TiDBOptCPUFactor, strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptTiFlashConcurrencyFactor, strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptCopCPUFactor, strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptNetworkFactor, strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptScanFactor, strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptDescScanFactor, strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptSeekFactor, strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptMemoryFactor, strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptDiskFactor, strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBOptConcurrencyFactor, strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBIndexJoinBatchSize, strconv.Itoa(DefIndexJoinBatchSize)},
	{ScopeGlobal | ScopeSession, TiDBIndexLookupSize, strconv.Itoa(DefIndexLookupSize)},
	{ScopeGlobal | ScopeSession, TiDBIndexLookupConcurrency, strconv.Itoa(DefIndexLookupConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBIndexLookupJoinConcurrency, strconv.Itoa(DefIndexLookupJoinConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBIndexSerialScanConcurrency, strconv.Itoa(DefIndexSerialScanConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBSkipUTF8Check, BoolToIntStr(DefSkipUTF8Check)},
	{ScopeSession, TiDBBatchInsert, BoolToIntStr(DefBatchInsert)},
	{ScopeSession, TiDBBatchDelete, BoolToIntStr(DefBatchDelete)},
	{ScopeSession, TiDBBatchCommit, BoolToIntStr(DefBatchCommit)},
	{ScopeSession, TiDBDMLBatchSize, strconv.Itoa(DefDMLBatchSize)},
	{ScopeSession, TiDBCurrentTS, strconv.Itoa(DefCurretTS)},
	{ScopeSession, TiDBLastTxnInfo, strconv.Itoa(DefCurretTS)},
	{ScopeGlobal | ScopeSession, TiDBMaxChunkSize, strconv.Itoa(DefMaxChunkSize)},
	{ScopeGlobal | ScopeSession, TiDBAllowBatchCop, strconv.Itoa(DefTiDBAllowBatchCop)},
	{ScopeGlobal | ScopeSession, TiDBInitChunkSize, strconv.Itoa(DefInitChunkSize)},
	{ScopeGlobal | ScopeSession, TiDBEnableCascadesPlanner, "0"},
	{ScopeGlobal | ScopeSession, TiDBEnableIndexMerge, "0"},
	{ScopeSession, TIDBMemQuotaQuery, strconv.FormatInt(config.GetGlobalConfig().MemQuotaQuery, 10)},
	{ScopeSession, TIDBMemQuotaHashJoin, strconv.FormatInt(DefTiDBMemQuotaHashJoin, 10)},
	{ScopeSession, TIDBMemQuotaMergeJoin, strconv.FormatInt(DefTiDBMemQuotaMergeJoin, 10)},
	{ScopeSession, TIDBMemQuotaSort, strconv.FormatInt(DefTiDBMemQuotaSort, 10)},
	{ScopeSession, TIDBMemQuotaTopn, strconv.FormatInt(DefTiDBMemQuotaTopn, 10)},
	{ScopeSession, TIDBMemQuotaIndexLookupReader, strconv.FormatInt(DefTiDBMemQuotaIndexLookupReader, 10)},
	{ScopeSession, TIDBMemQuotaIndexLookupJoin, strconv.FormatInt(DefTiDBMemQuotaIndexLookupJoin, 10)},
	{ScopeSession, TIDBMemQuotaNestedLoopApply, strconv.FormatInt(DefTiDBMemQuotaNestedLoopApply, 10)},
	{ScopeSession, TiDBEnableStreaming, "0"},
	{ScopeSession, TiDBEnableChunkRPC, "1"},
	{ScopeSession, TxnIsolationOneShot, ""},
	{ScopeGlobal | ScopeSession, TiDBEnableTablePartition, "on"},
	{ScopeGlobal | ScopeSession, TiDBHashJoinConcurrency, strconv.Itoa(DefTiDBHashJoinConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBProjectionConcurrency, strconv.Itoa(DefTiDBProjectionConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBHashAggPartialConcurrency, strconv.Itoa(DefTiDBHashAggPartialConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBHashAggFinalConcurrency, strconv.Itoa(DefTiDBHashAggFinalConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBWindowConcurrency, strconv.Itoa(DefTiDBWindowConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBUnionConcurrency, strconv.Itoa(DefTiDBUnionConcurrency)},
	{ScopeGlobal | ScopeSession, TiDBBackoffLockFast, strconv.Itoa(kv.DefBackoffLockFast)},
	{ScopeGlobal | ScopeSession, TiDBBackOffWeight, strconv.Itoa(kv.DefBackOffWeight)},
	{ScopeGlobal | ScopeSession, TiDBRetryLimit, strconv.Itoa(DefTiDBRetryLimit)},
	{ScopeGlobal | ScopeSession, TiDBDisableTxnAutoRetry, BoolToIntStr(DefTiDBDisableTxnAutoRetry)},
	{ScopeGlobal | ScopeSession, TiDBConstraintCheckInPlace, BoolToIntStr(DefTiDBConstraintCheckInPlace)},
	{ScopeGlobal | ScopeSession, TiDBTxnMode, DefTiDBTxnMode},
	{ScopeGlobal, TiDBRowFormatVersion, strconv.Itoa(DefTiDBRowFormatV1)},
	{ScopeSession, TiDBOptimizerSelectivityLevel, strconv.Itoa(DefTiDBOptimizerSelectivityLevel)},
	{ScopeGlobal | ScopeSession, TiDBEnableWindowFunction, BoolToIntStr(DefEnableWindowFunction)},
	{ScopeGlobal | ScopeSession, TiDBEnableVectorizedExpression, BoolToIntStr(DefEnableVectorizedExpression)},
	{ScopeGlobal | ScopeSession, TiDBEnableFastAnalyze, BoolToIntStr(DefTiDBUseFastAnalyze)},
	{ScopeGlobal | ScopeSession, TiDBSkipIsolationLevelCheck, BoolToIntStr(DefTiDBSkipIsolationLevelCheck)},

	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableRateLimitAction, Value: boolToOnOff(DefTiDBEnableRateLimitAction)},
	/* The following variable is defined as session scope but is actually server scope. */
	{ScopeSession, TiDBGeneralLog, strconv.Itoa(DefTiDBGeneralLog)},
	{ScopeSession, TiDBPProfSQLCPU, strconv.Itoa(DefTiDBPProfSQLCPU)},
	{ScopeSession, TiDBDDLSlowOprThreshold, strconv.Itoa(DefTiDBDDLSlowOprThreshold)},
	{ScopeSession, TiDBConfig, ""},
	{ScopeGlobal, TiDBDDLReorgWorkerCount, strconv.Itoa(DefTiDBDDLReorgWorkerCount)},
	{ScopeGlobal, TiDBDDLReorgBatchSize, strconv.Itoa(DefTiDBDDLReorgBatchSize)},
	{ScopeGlobal, TiDBDDLErrorCountLimit, strconv.Itoa(DefTiDBDDLErrorCountLimit)},
	{ScopeSession, TiDBDDLReorgPriority, "PRIORITY_LOW"},
	{ScopeGlobal, TiDBMaxDeltaSchemaCount, strconv.Itoa(DefTiDBMaxDeltaSchemaCount)},
	{ScopeSession, TiDBForcePriority, mysql.Priority2Str[DefTiDBForcePriority]},
	{ScopeSession, TiDBEnableRadixJoin, BoolToIntStr(DefTiDBUseRadixJoin)},
	{ScopeGlobal | ScopeSession, TiDBOptJoinReorderThreshold, strconv.Itoa(DefTiDBOptJoinReorderThreshold)},
	{ScopeSession, TiDBSlowQueryFile, ""},
	{ScopeGlobal, TiDBScatterRegion, BoolToIntStr(DefTiDBScatterRegion)},
	{ScopeSession, TiDBWaitSplitRegionFinish, BoolToIntStr(DefTiDBWaitSplitRegionFinish)},
	{ScopeSession, TiDBWaitSplitRegionTimeout, strconv.Itoa(DefWaitSplitRegionTimeout)},
	{ScopeSession, TiDBLowResolutionTSO, "0"},
	{ScopeSession, TiDBExpensiveQueryTimeThreshold, strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold)},
	{ScopeSession, TiDBMemoryUsageAlarmRatio, strconv.FormatFloat(config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio, 'f', -1, 64)},
	{ScopeGlobal | ScopeSession, TiDBEnableNoopFuncs, BoolToIntStr(DefTiDBEnableNoopFuncs)},
	{ScopeSession, TiDBReplicaRead, "leader"},
	{ScopeSession, TiDBAllowRemoveAutoInc, BoolToIntStr(DefTiDBAllowRemoveAutoInc)},
	{ScopeGlobal | ScopeSession, TiDBEnableStmtSummary, BoolToIntStr(config.GetGlobalConfig().StmtSummary.Enable)},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryInternalQuery, BoolToIntStr(config.GetGlobalConfig().StmtSummary.EnableInternalQuery)},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryRefreshInterval, strconv.Itoa(config.GetGlobalConfig().StmtSummary.RefreshInterval)},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryHistorySize, strconv.Itoa(config.GetGlobalConfig().StmtSummary.HistorySize)},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryMaxStmtCount, strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), 10)},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryMaxSQLLength, strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxSQLLength), 10)},
	{ScopeGlobal | ScopeSession, TiDBCapturePlanBaseline, "off"},
	{ScopeGlobal | ScopeSession, TiDBUsePlanBaselines, boolToOnOff(DefTiDBUsePlanBaselines)},
	{ScopeGlobal | ScopeSession, TiDBEvolvePlanBaselines, boolToOnOff(DefTiDBEvolvePlanBaselines)},
	{ScopeGlobal, TiDBEvolvePlanTaskMaxTime, strconv.Itoa(DefTiDBEvolvePlanTaskMaxTime)},
	{ScopeGlobal, TiDBEvolvePlanTaskStartTime, DefTiDBEvolvePlanTaskStartTime},
	{ScopeGlobal, TiDBEvolvePlanTaskEndTime, DefTiDBEvolvePlanTaskEndTime},
	{ScopeSession, TiDBIsolationReadEngines, strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ", ")},
	{ScopeGlobal | ScopeSession, TiDBStoreLimit, strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10)},
	{ScopeSession, TiDBMetricSchemaStep, strconv.Itoa(DefTiDBMetricSchemaStep)},
	{ScopeSession, TiDBMetricSchemaRangeDuration, strconv.Itoa(DefTiDBMetricSchemaRangeDuration)},
	{ScopeSession, TiDBSlowLogThreshold, strconv.Itoa(logutil.DefaultSlowThreshold)},
	{ScopeSession, TiDBRecordPlanInSlowLog, strconv.Itoa(logutil.DefaultRecordPlanInSlowLog)},
	{ScopeSession, TiDBEnableSlowLog, BoolToIntStr(logutil.DefaultTiDBEnableSlowLog)},
	{ScopeSession, TiDBQueryLogMaxLen, strconv.Itoa(logutil.DefaultQueryLogMaxLen)},
	{ScopeSession, TiDBCheckMb4ValueInUTF8, BoolToIntStr(config.GetGlobalConfig().CheckMb4ValueInUTF8)},
	{ScopeSession, TiDBFoundInPlanCache, BoolToIntStr(DefTiDBFoundInPlanCache)},
	{ScopeSession, TiDBEnableCollectExecutionInfo, BoolToIntStr(DefTiDBEnableCollectExecutionInfo)},
	{ScopeGlobal | ScopeSession, TiDBAllowAutoRandExplicitInsert, boolToOnOff(DefTiDBAllowAutoRandExplicitInsert)},
	{ScopeGlobal | ScopeSession, TiDBSlowLogMasking, BoolToIntStr(DefTiDBRedactLog)},
	{ScopeGlobal | ScopeSession, TiDBRedactLog, BoolToIntStr(DefTiDBRedactLog)},
	{ScopeGlobal, TiDBEnableTelemetry, BoolToIntStr(DefTiDBEnableTelemetry)},
	{ScopeGlobal | ScopeSession, TiDBEnableAmendPessimisticTxn, boolToOnOff(DefTiDBEnableAmendPessimisticTxn)},
	{ScopeGlobal | ScopeSession, TiDBMultiStatementMode, Warn},
>>>>>>> 32cf4b1785cbc9186057a26cb939a16cad94dba1
}

// FeedbackProbability points to the FeedbackProbability in statistics package.
// It's initialized in init() in feedback.go to solve import cycle.
var FeedbackProbability *atomic2.Float64

// SynonymsSysVariables is synonyms of system variables.
var SynonymsSysVariables = map[string][]string{}

func addSynonymsSysVariables(synonyms ...string) {
	for _, s := range synonyms {
		SynonymsSysVariables[s] = synonyms
	}
}

func initSynonymsSysVariables() {
	addSynonymsSysVariables(TxnIsolation, TransactionIsolation)
	addSynonymsSysVariables(TxReadOnly, TransactionReadOnly)
}

// SetNamesVariables is the system variable names related to set names statements.
var SetNamesVariables = []string{
	CharacterSetClient,
	CharacterSetConnection,
	CharacterSetResults,
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	CharacterSetClient,
	CharacterSetResults,
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	"character_set_client",
	"character_set_results",
}

const (
	// CharacterSetConnection is the name for character_set_connection system variable.
	CharacterSetConnection = "character_set_connection"
	// CollationConnection is the name for collation_connection system variable.
	CollationConnection = "collation_connection"
	// CharsetDatabase is the name for character_set_database system variable.
	CharsetDatabase = "character_set_database"
	// CollationDatabase is the name for collation_database system variable.
	CollationDatabase = "collation_database"
	// CharacterSetFilesystem is the name for character_set_filesystem system variable.
	CharacterSetFilesystem = "character_set_filesystem"
	// CharacterSetClient is the name for character_set_client system variable.
	CharacterSetClient = "character_set_client"
	// CharacterSetSystem is the name for character_set_system system variable.
	CharacterSetSystem = "character_set_system"
	// GeneralLog is the name for 'general_log' system variable.
	GeneralLog = "general_log"
	// AvoidTemporalUpgrade is the name for 'avoid_temporal_upgrade' system variable.
	AvoidTemporalUpgrade = "avoid_temporal_upgrade"
	// MaxPreparedStmtCount is the name for 'max_prepared_stmt_count' system variable.
	MaxPreparedStmtCount = "max_prepared_stmt_count"
	// BigTables is the name for 'big_tables' system variable.
	BigTables = "big_tables"
	// CheckProxyUsers is the name for 'check_proxy_users' system variable.
	CheckProxyUsers = "check_proxy_users"
	// CoreFile is the name for 'core_file' system variable.
	CoreFile = "core_file"
	// DefaultWeekFormat is the name for 'default_week_format' system variable.
	DefaultWeekFormat = "default_week_format"
	// GroupConcatMaxLen is the name for 'group_concat_max_len' system variable.
	GroupConcatMaxLen = "group_concat_max_len"
	// DelayKeyWrite is the name for 'delay_key_write' system variable.
	DelayKeyWrite = "delay_key_write"
	// EndMarkersInJSON is the name for 'end_markers_in_json' system variable.
	EndMarkersInJSON = "end_markers_in_json"
	// Hostname is the name for 'hostname' system variable.
	Hostname = "hostname"
	// InnodbCommitConcurrency is the name for 'innodb_commit_concurrency' system variable.
	InnodbCommitConcurrency = "innodb_commit_concurrency"
	// InnodbFastShutdown is the name for 'innodb_fast_shutdown' system variable.
	InnodbFastShutdown = "innodb_fast_shutdown"
	// InnodbLockWaitTimeout is the name for 'innodb_lock_wait_timeout' system variable.
	InnodbLockWaitTimeout = "innodb_lock_wait_timeout"
	// SQLLogBin is the name for 'sql_log_bin' system variable.
	SQLLogBin = "sql_log_bin"
	// LogBin is the name for 'log_bin' system variable.
	LogBin = "log_bin"
	// MaxSortLength is the name for 'max_sort_length' system variable.
	MaxSortLength = "max_sort_length"
	// MaxSpRecursionDepth is the name for 'max_sp_recursion_depth' system variable.
	MaxSpRecursionDepth = "max_sp_recursion_depth"
	// MaxUserConnections is the name for 'max_user_connections' system variable.
	MaxUserConnections = "max_user_connections"
	// OfflineMode is the name for 'offline_mode' system variable.
	OfflineMode = "offline_mode"
	// InteractiveTimeout is the name for 'interactive_timeout' system variable.
	InteractiveTimeout = "interactive_timeout"
	// FlushTime is the name for 'flush_time' system variable.
	FlushTime = "flush_time"
	// PseudoSlaveMode is the name for 'pseudo_slave_mode' system variable.
	PseudoSlaveMode = "pseudo_slave_mode"
	// LowPriorityUpdates is the name for 'low_priority_updates' system variable.
	LowPriorityUpdates = "low_priority_updates"
	// LowerCaseTableNames is the name for 'lower_case_table_names' system variable.
	LowerCaseTableNames = "lower_case_table_names"
	// SessionTrackGtids is the name for 'session_track_gtids' system variable.
	SessionTrackGtids = "session_track_gtids"
	// OldPasswords is the name for 'old_passwords' system variable.
	OldPasswords = "old_passwords"
	// MaxConnections is the name for 'max_connections' system variable.
	MaxConnections = "max_connections"
	// SkipNameResolve is the name for 'skip_name_resolve' system variable.
	SkipNameResolve = "skip_name_resolve"
	// ForeignKeyChecks is the name for 'foreign_key_checks' system variable.
	ForeignKeyChecks = "foreign_key_checks"
	// SQLSafeUpdates is the name for 'sql_safe_updates' system variable.
	SQLSafeUpdates = "sql_safe_updates"
	// WarningCount is the name for 'warning_count' system variable.
	WarningCount = "warning_count"
	// ErrorCount is the name for 'error_count' system variable.
	ErrorCount = "error_count"
	// SQLSelectLimit is the name for 'sql_select_limit' system variable.
	SQLSelectLimit = "sql_select_limit"
	// MaxConnectErrors is the name for 'max_connect_errors' system variable.
	MaxConnectErrors = "max_connect_errors"
	// TableDefinitionCache is the name for 'table_definition_cache' system variable.
	TableDefinitionCache = "table_definition_cache"
	// TmpTableSize is the name for 'tmp_table_size' system variable.
	TmpTableSize = "tmp_table_size"
	// Timestamp is the name for 'timestamp' system variable.
	Timestamp = "timestamp"
	// ConnectTimeout is the name for 'connect_timeout' system variable.
	ConnectTimeout = "connect_timeout"
	// SyncBinlog is the name for 'sync_binlog' system variable.
	SyncBinlog = "sync_binlog"
	// BlockEncryptionMode is the name for 'block_encryption_mode' system variable.
	BlockEncryptionMode = "block_encryption_mode"
	// WaitTimeout is the name for 'wait_timeout' system variable.
	WaitTimeout = "wait_timeout"
	// ValidatePasswordNumberCount is the name of 'validate_password_number_count' system variable.
	ValidatePasswordNumberCount = "validate_password_number_count"
	// ValidatePasswordLength is the name of 'validate_password_length' system variable.
	ValidatePasswordLength = "validate_password_length"
	// Version is the name of 'version' system variable.
	Version = "version"
	// VersionComment is the name of 'version_comment' system variable.
	VersionComment = "version_comment"
	// PluginDir is the name of 'plugin_dir' system variable.
	PluginDir = "plugin_dir"
	// PluginLoad is the name of 'plugin_load' system variable.
	PluginLoad = "plugin_load"
	// Port is the name for 'port' system variable.
	Port = "port"
	// DataDir is the name for 'datadir' system variable.
	DataDir = "datadir"
	// Profiling is the name for 'Profiling' system variable.
	Profiling = "profiling"
	// Socket is the name for 'socket' system variable.
	Socket = "socket"
	// BinlogOrderCommits is the name for 'binlog_order_commits' system variable.
	BinlogOrderCommits = "binlog_order_commits"
	// MasterVerifyChecksum is the name for 'master_verify_checksum' system variable.
	MasterVerifyChecksum = "master_verify_checksum"
	// ValidatePasswordCheckUserName is the name for 'validate_password_check_user_name' system variable.
	ValidatePasswordCheckUserName = "validate_password_check_user_name"
	// SuperReadOnly is the name for 'super_read_only' system variable.
	SuperReadOnly = "super_read_only"
	// SQLNotes is the name for 'sql_notes' system variable.
	SQLNotes = "sql_notes"
	// QueryCacheType is the name for 'query_cache_type' system variable.
	QueryCacheType = "query_cache_type"
	// SlaveCompressedProtocol is the name for 'slave_compressed_protocol' system variable.
	SlaveCompressedProtocol = "slave_compressed_protocol"
	// BinlogRowQueryLogEvents is the name for 'binlog_rows_query_log_events' system variable.
	BinlogRowQueryLogEvents = "binlog_rows_query_log_events"
	// LogSlowSlaveStatements is the name for 'log_slow_slave_statements' system variable.
	LogSlowSlaveStatements = "log_slow_slave_statements"
	// LogSlowAdminStatements is the name for 'log_slow_admin_statements' system variable.
	LogSlowAdminStatements = "log_slow_admin_statements"
	// LogQueriesNotUsingIndexes is the name for 'log_queries_not_using_indexes' system variable.
	LogQueriesNotUsingIndexes = "log_queries_not_using_indexes"
	// QueryCacheWlockInvalidate is the name for 'query_cache_wlock_invalidate' system variable.
	QueryCacheWlockInvalidate = "query_cache_wlock_invalidate"
	// SQLAutoIsNull is the name for 'sql_auto_is_null' system variable.
	SQLAutoIsNull = "sql_auto_is_null"
	// RelayLogPurge is the name for 'relay_log_purge' system variable.
	RelayLogPurge = "relay_log_purge"
	// AutomaticSpPrivileges is the name for 'automatic_sp_privileges' system variable.
	AutomaticSpPrivileges = "automatic_sp_privileges"
	// SQLQuoteShowCreate is the name for 'sql_quote_show_create' system variable.
	SQLQuoteShowCreate = "sql_quote_show_create"
	// SlowQueryLog is the name for 'slow_query_log' system variable.
	SlowQueryLog = "slow_query_log"
	// BinlogDirectNonTransactionalUpdates is the name for 'binlog_direct_non_transactional_updates' system variable.
	BinlogDirectNonTransactionalUpdates = "binlog_direct_non_transactional_updates"
	// SQLBigSelects is the name for 'sql_big_selects' system variable.
	SQLBigSelects = "sql_big_selects"
	// LogBinTrustFunctionCreators is the name for 'log_bin_trust_function_creators' system variable.
	LogBinTrustFunctionCreators = "log_bin_trust_function_creators"
	// OldAlterTable is the name for 'old_alter_table' system variable.
	OldAlterTable = "old_alter_table"
	// EnforceGtidConsistency is the name for 'enforce_gtid_consistency' system variable.
	EnforceGtidConsistency = "enforce_gtid_consistency"
	// SecureAuth is the name for 'secure_auth' system variable.
	SecureAuth = "secure_auth"
	// UniqueChecks is the name for 'unique_checks' system variable.
	UniqueChecks = "unique_checks"
	// SQLWarnings is the name for 'sql_warnings' system variable.
	SQLWarnings = "sql_warnings"
	// AutoCommit is the name for 'autocommit' system variable.
	AutoCommit = "autocommit"
	// KeepFilesOnCreate is the name for 'keep_files_on_create' system variable.
	KeepFilesOnCreate = "keep_files_on_create"
	// ShowOldTemporals is the name for 'show_old_temporals' system variable.
	ShowOldTemporals = "show_old_temporals"
	// LocalInFile is the name for 'local_infile' system variable.
	LocalInFile = "local_infile"
	// PerformanceSchema is the name for 'performance_schema' system variable.
	PerformanceSchema = "performance_schema"
	// Flush is the name for 'flush' system variable.
	Flush = "flush"
	// SlaveAllowBatching is the name for 'slave_allow_batching' system variable.
	SlaveAllowBatching = "slave_allow_batching"
	// MyISAMUseMmap is the name for 'myisam_use_mmap' system variable.
	MyISAMUseMmap = "myisam_use_mmap"
	// InnodbFilePerTable is the name for 'innodb_file_per_table' system variable.
	InnodbFilePerTable = "innodb_file_per_table"
	// InnodbLogCompressedPages is the name for 'innodb_log_compressed_pages' system variable.
	InnodbLogCompressedPages = "innodb_log_compressed_pages"
	// InnodbPrintAllDeadlocks is the name for 'innodb_print_all_deadlocks' system variable.
	InnodbPrintAllDeadlocks = "innodb_print_all_deadlocks"
	// InnodbStrictMode is the name for 'innodb_strict_mode' system variable.
	InnodbStrictMode = "innodb_strict_mode"
	// InnodbCmpPerIndexEnabled is the name for 'innodb_cmp_per_index_enabled' system variable.
	InnodbCmpPerIndexEnabled = "innodb_cmp_per_index_enabled"
	// InnodbBufferPoolDumpAtShutdown is the name for 'innodb_buffer_pool_dump_at_shutdown' system variable.
	InnodbBufferPoolDumpAtShutdown = "innodb_buffer_pool_dump_at_shutdown"
	// InnodbAdaptiveHashIndex is the name for 'innodb_adaptive_hash_index' system variable.
	InnodbAdaptiveHashIndex = "innodb_adaptive_hash_index"
	// InnodbFtEnableStopword is the name for 'innodb_ft_enable_stopword' system variable.
	InnodbFtEnableStopword = "innodb_ft_enable_stopword"
	// InnodbSupportXA is the name for 'innodb_support_xa' system variable.
	InnodbSupportXA = "innodb_support_xa"
	// InnodbOptimizeFullTextOnly is the name for 'innodb_optimize_fulltext_only' system variable.
	InnodbOptimizeFullTextOnly = "innodb_optimize_fulltext_only"
	// InnodbStatusOutputLocks is the name for 'innodb_status_output_locks' system variable.
	InnodbStatusOutputLocks = "innodb_status_output_locks"
	// InnodbBufferPoolDumpNow is the name for 'innodb_buffer_pool_dump_now' system variable.
	InnodbBufferPoolDumpNow = "innodb_buffer_pool_dump_now"
	// InnodbBufferPoolLoadNow is the name for 'innodb_buffer_pool_load_now' system variable.
	InnodbBufferPoolLoadNow = "innodb_buffer_pool_load_now"
	// InnodbStatsOnMetadata is the name for 'innodb_stats_on_metadata' system variable.
	InnodbStatsOnMetadata = "innodb_stats_on_metadata"
	// InnodbDisableSortFileCache is the name for 'innodb_disable_sort_file_cache' system variable.
	InnodbDisableSortFileCache = "innodb_disable_sort_file_cache"
	// InnodbStatsAutoRecalc is the name for 'innodb_stats_auto_recalc' system variable.
	InnodbStatsAutoRecalc = "innodb_stats_auto_recalc"
	// InnodbBufferPoolLoadAbort is the name for 'innodb_buffer_pool_load_abort' system variable.
	InnodbBufferPoolLoadAbort = "innodb_buffer_pool_load_abort"
	// InnodbStatsPersistent is the name for 'innodb_stats_persistent' system variable.
	InnodbStatsPersistent = "innodb_stats_persistent"
	// InnodbRandomReadAhead is the name for 'innodb_random_read_ahead' system variable.
	InnodbRandomReadAhead = "innodb_random_read_ahead"
	// InnodbAdaptiveFlushing is the name for 'innodb_adaptive_flushing' system variable.
	InnodbAdaptiveFlushing = "innodb_adaptive_flushing"
	// InnodbTableLocks is the name for 'innodb_table_locks' system variable.
	InnodbTableLocks = "innodb_table_locks"
	// InnodbStatusOutput is the name for 'innodb_status_output' system variable.
	InnodbStatusOutput = "innodb_status_output"
	// NetBufferLength is the name for 'net_buffer_length' system variable.
	NetBufferLength = "net_buffer_length"
	// QueryCacheSize is the name of 'query_cache_size' system variable.
	QueryCacheSize = "query_cache_size"
	// TxReadOnly is the name of 'tx_read_only' system variable.
	TxReadOnly = "tx_read_only"
	// TransactionReadOnly is the name of 'transaction_read_only' system variable.
	TransactionReadOnly = "transaction_read_only"
	// CharacterSetServer is the name of 'character_set_server' system variable.
	CharacterSetServer = "character_set_server"
	// AutoIncrementIncrement is the name of 'auto_increment_increment' system variable.
	AutoIncrementIncrement = "auto_increment_increment"
	// AutoIncrementOffset is the name of 'auto_increment_offset' system variable.
	AutoIncrementOffset = "auto_increment_offset"
	// InitConnect is the name of 'init_connect' system variable.
	InitConnect = "init_connect"
	// CollationServer is the name of 'collation_server' variable.
	CollationServer = "collation_server"
	// NetWriteTimeout is the name of 'net_write_timeout' variable.
	NetWriteTimeout = "net_write_timeout"
	// ThreadPoolSize is the name of 'thread_pool_size' variable.
	ThreadPoolSize = "thread_pool_size"
	// WindowingUseHighPrecision is the name of 'windowing_use_high_precision' system variable.
	WindowingUseHighPrecision = "windowing_use_high_precision"
	// OptimizerSwitch is the name of 'optimizer_switch' system variable.
	OptimizerSwitch = "optimizer_switch"
	// SystemTimeZone is the name of 'system_time_zone' system variable.
	SystemTimeZone = "system_time_zone"
)

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(name string, value string) error
}
