// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hanadb

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/FerretDB/FerretDB/internal/handlers/commonerrors"
	"github.com/FerretDB/FerretDB/internal/handlers/hana/hjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// CreateWhereClause creates the WHERE-clause of the SQL statement.
func CreateWhereClause(filter *types.Document) (string, error) {
	var sql string

	for i, key := range filter.Keys() {

		if i == 0 {
			sql += " WHERE "
		} else {
			sql += " AND "
		}

		value, err := filter.Get(key)
		if err != nil {
			return "", err
		}

		// Stands for key-value SQL
		var kvSQL string
		kvSQL, err = wherePair(key, value)

		if err != nil {
			return "", err
		}

		sql += kvSQL
	}

	return sql, nil
}

// wherePair takes a {field: value} and converts it to SQL
func wherePair(key string, value any) (string, error) {
	var kvSQL string
	var err error

	if strings.HasPrefix(key, "$") { // {$: value}
		return logicExpression(key, value)
	}

	switch value := value.(type) {
	case types.Document:
		if strings.HasPrefix(value.Keys()[0], "$") { // {field: {$: value}}
			return fieldExpression(key, value)
		}
	}

	// vSQL: ValueSQL
	var vSQL string
	var sign string
	vSQL, sign, err = whereValue(value)

	if err != nil {
		return "", err
	}

	// kSQL: KeySQL
	var kSQL string
	kSQL, err = whereKey(key)
	if err != nil {
		return "", err
	}

	kvSQL = kSQL + sign + vSQL

	if isNor {
		kvSQL = "(" + kvSQL + " AND " + kSQL + " IS SET)"
	}

	return kvSQL, nil
}

// whereKey prepares the key (field) for SQL
func whereKey(key string) (string, error) {
	var kSQL string

	if strings.Contains(key, ".") {
		splitKey := strings.Split(key, ".")
		var isInt bool
		for i, k := range splitKey {

			if kInt, convErr := strconv.Atoi(k); convErr == nil {
				if isInt {
					return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrNotImplemented, "not yet supporting indexing on an array inside of an array")
				}
				if kInt < 0 {
					return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, "negative array index is not allowed")
				}
				kIntSQL := "[" + "%d" + "]"
				kSQL += fmt.Sprintf(kIntSQL, (kInt + 1))
				isInt = true
				continue
			}

			if i != 0 {
				kSQL += "."
			}

			kSQL += "\"" + k + "\""

			isInt = false

		}
	} else {
		kSQL = "\"" + key + "\""
	}

	return kSQL, nil
}

// whereValue prepares the value for SQL
func whereValue(value any) (vSQL string, sign string, err error) {
	var args []any
	switch value := value.(type) {
	case int32, int64:
		vSQL = "%d"
		args = append(args, value)
	case float64:
		vSQL = "%f"
		args = append(args, value)
	case string:
		vSQL = "'%s'"
		args = append(args, value)
	case bool:
		vSQL = "to_json_boolean(%t)"
		args = append(args, value)
	case nil:
		vSQL = "NULL"
		sign = " IS "
		return
	case types.Regex:
		vSQL, err = regex(value)
		if err != nil {
			return
		}
		sign = " LIKE "
		return
	case types.ObjectID:
		var bOBJ []byte
		bOBJ, err = hjson.MarshalSingleValue(value)
		if err != nil {
			return
		}

		oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
		vSQL = "%s"
		args = append(args, string(oid))

	case types.Document:
		vSQL = "%s"
		var docValue string
		docValue, err = whereDocument(value)
		args = append(args, docValue)
	default:
		return "", "", commonerrors.NewCommandErrorMsg(commonerrors.ErrNotImplemented, fmt.Sprintf("value %T not supported in filter", value))

	}
	sign = " = "
	vSQL = fmt.Sprintf(vSQL, args...)

	return
}

// whereDocument prepares a document for fx. value = {document}.
func whereDocument(doc types.Document) (string, error) {
	docSQL := "{"

	var err error
	var value any
	var args []any
	for i, key := range doc.Keys() {

		if i != 0 {
			docSQL += ", "
		}

		docSQL += "\"" + key + "\": "

		value, err = doc.Get(key)

		if err != nil {
			return "", err
		}

		switch value := value.(type) {
		case int32, int64:
			docSQL += "%d"
			args = append(args, value)
		case float64:
			docSQL += "%f"
			args = append(args, value)
		case string:

			docSQL += "'%s'"
			args = append(args, value)
		case bool:

			docSQL += "to_json_boolean(%t)"

			args = append(args, value)
		case nil:
			docSQL += " NULL "
		case types.ObjectID:
			docSQL += "%s"
			var bOBJ []byte
			bOBJ, err = hjson.MarshalSingleValue(value)
			oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
			oid = bytes.Replace(oid, []byte{39}, []byte{34}, 2)
			args = append(args, string(oid))
		case *types.Array:
			var sqlArray string

			sqlArray, err = PrepareArrayForSQL(value)

			docSQL += sqlArray

		case types.Document:

			docSQL += "%s"

			var docValue string
			docValue, err = whereDocument(value)
			if err != nil {
				return "", err
			}

			args = append(args, docValue)

		default:
			return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, fmt.Sprintf("the document used in filter contains a datatype not yet supported: %T", value))
		}
	}

	docSQL = fmt.Sprintf(docSQL, args...) + "}"

	return docSQL, nil
}

// PrepareArrayForSQL prepares an array which is inside of a document for SQL
func PrepareArrayForSQL(a *types.Array) (string, error) {
	var value any
	var args []any
	var err error

	sqlArray := "["
	for i := 0; i < a.Len(); i++ {
		if i != 0 {
			sqlArray += ", "
		}

		value, err = a.Get(i)
		if err != nil {
			return "", err
		}

		switch value := value.(type) {
		case string, int32, int64, float64, types.ObjectID, nil, bool:
			var sql string
			sql, _, err = whereValue(value)
			sqlArray += sql
		case *types.Array:
			var sql string
			sql, err = PrepareArrayForSQL(value)
			if err != nil {
				return "", err
			}
			sqlArray += "%s"
			args = append(args, sql)

		case types.Document:

			sqlArray += "%s"

			var docValue string
			docValue, err = whereDocument(value)
			if err != nil {
				return "", err
			}

			args = append(args, docValue)

		default:
			return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrNotImplemented, fmt.Sprintf("The array used in filter contains a datatype not yet supported: %T", value))
		}
	}

	sqlArray += "]"
	sqlArray = fmt.Sprintf(sqlArray, args...)

	return sqlArray, nil
}

var (
	isNor      bool
	norCounter int
)

// logicExpression converts expressions like $AND and $OR to the equivalent expressions in SQL.
func logicExpression(key string, value any) (string, error) {
	logicExprMap := map[string]string{
		"$and": " AND ",
		"$or":  " OR ",
		"$nor": " AND NOT (",
	}

	lowerKey := strings.ToLower(key)

	var logicExpr string
	var ok bool
	if logicExpr, ok = logicExprMap[lowerKey]; !ok {
		err := commonerrors.NewCommandErrorMsgWithArgument(commonerrors.ErrNotImplemented, "support for %s is not implemented yet", key)
		if strings.EqualFold(key, "$not") {
			err = commonerrors.NewCommandErrorMsgWithArgument(commonerrors.ErrBadValue, "unknown top level: %s. If you are trying to negate an entire expression, use $nor", key)
		}
		return "", err
	}

	var localIsNor bool
	if strings.EqualFold(key, "$nor") {
		localIsNor = true
		isNor = true
		norCounter++
	}

	kvSQL := "("
	var err error
	switch value := value.(type) {
	case *types.Array:
		if value.Len() < 2 && !isNor {
			err = fmt.Errorf("need minimum two expressions")
			return "", err
		}
		var expr any
		for i := 0; i < value.Len(); i++ {

			expr, err = value.Get(i)
			if err != nil {
				return "", err
			}

			switch expr := expr.(type) {
			case types.Document:

				if i == 0 && localIsNor {
					kvSQL += " NOT ("
				}
				if i != 0 {
					kvSQL += logicExpr
				}

				var value any
				var exprSQL string
				for i, k := range expr.Keys() {

					if i != 0 {
						kvSQL += " AND "
					}

					value, err = expr.Get(k)
					if err != nil {
						return "", nil
					}
					exprSQL, err = wherePair(k, value)
					if err != nil {
						return "", nil
					}

					kvSQL += exprSQL

				}

			default:
				return "", lazyerrors.Errorf("Found in array of logicExpression no document but instead the datatype: %T", value)
			}
			if localIsNor {
				kvSQL += ")"
			}
		}

	default:
		return "", commonerrors.NewCommandErrorMsgWithArgument(commonerrors.ErrBadValue, "%s must be an array", lowerKey)

	}

	kvSQL += ")"

	if localIsNor {
		norCounter--
		if norCounter == 0 {
			isNor = false
		}
	}
	return kvSQL, nil
}

// fieldExpression converts expressions like $gt or $elemMatch to the equivalent expression in SQL.
// Used for {field: {$: value}}.
func fieldExpression(key string, value any) (string, error) {
	fieldExprMap := map[string]string{
		"$gt":        " > ",
		"$gte":       " >= ",
		"$lt":        " < ",
		"$lte":       " <= ",
		"$eq":        " = ",
		"$ne":        " <> ",
		"$exists":    " IS ",
		"$size":      "CARDINALITY",
		"$all":       "all",
		"$elemmatch": "elemMatch",
		"$not":       " NOT ",
		"$regex":     " LIKE ",
	}

	kSQL, err := whereKey(key)
	if err != nil {
		return "", err
	}

	var kvSQL string
	switch value := value.(type) {
	case types.Document:

		var exprValue any
		var vSQL string
		for i, k := range value.Keys() {

			if i != 0 {
				kvSQL += " AND "
			}
			kvSQL += kSQL

			lowerK := strings.ToLower(k)

			fieldExpr, ok := fieldExprMap[lowerK]
			if !ok {
				return "", commonerrors.NewCommandErrorMsgWithArgument(commonerrors.ErrNotImplemented, "support for %s is not implemented yet", k)
			}

			exprValue, err = value.Get(k)
			if err != nil {
				return "", err
			}
			var sign string
			if lowerK == "$exists" {
				switch exprValue := exprValue.(type) {
				case bool:
					if exprValue {
						vSQL = "SET"
					} else {
						vSQL = "UNSET"
					}
				default:
					return "", lazyerrors.Errorf("$exists only works with boolean")
				}
			} else if lowerK == "$size" {
				kvSQL = fieldExpr + "(" + kvSQL + ")"
				vSQL, fieldExpr, err = whereValue(exprValue)
				if err != nil {
					return "", err
				}
			} else if lowerK == "$all" || lowerK == "$elemmatch" {
				kvSQL, err = filterArray(kvSQL, fieldExpr, exprValue)
				if err != nil {
					return "", err
				}
				continue
			} else if lowerK == "$not" {
				var fieldSQL string
				expr := value.Map()[k]
				fieldSQL, err = fieldExpression(key, expr)
				fieldSQL = "(" + fieldExpr + fieldSQL + " OR " + kSQL + " IS UNSET) "
				if err != nil {

					return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, "wrong use of $not")
				}

				kvSQL = fieldSQL
				return kvSQL, nil
			} else if lowerK == "$ne" {
				kvSQL = "(" + kvSQL
				vSQL, sign, err = whereValue(exprValue)
				if err != nil {
					return "", err
				}
				if strings.EqualFold(sign, " IS ") {
					fieldExpr = " IS NOT "
				}

				vSQL += " OR " + kSQL + " IS UNSET)"
			} else if lowerK == "$regex" {
				vSQL, err = regex(exprValue)
			} else {
				vSQL, sign, err = whereValue(exprValue)
				if err != nil {
					return "", err
				}

				if strings.EqualFold(sign, " IS ") {
					fieldExpr = sign
				}
			}

			kvSQL += fieldExpr + vSQL
			if isNor {
				kvSQL = "(" + kvSQL + " AND " + kSQL + " IS SET)"
			}

		}

	default:

		return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, fmt.Sprintf("In use of field expression a document was expected. Got instead: %T", value))
	}

	return kvSQL, nil
}

// filterArray implements $all and $elemMatch using the FOR ANY
func filterArray(field string, arrayOperator string, filters any) (string, error) {
	var kvSQL string
	var err error

	switch filters := filters.(type) {
	case types.Document:
		if strings.EqualFold(arrayOperator, "all") {

			return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, "$all needs an array")
		}
		i := 0
		for f, v := range filters.Map() {

			if i != 0 {
				kvSQL += " AND "
			}

			var doc *types.Document

			doc, err := types.NewDocument([]any{f, v}...)
			if err != nil {
				return "", err
			}
			var sql string
			if strings.Contains(doc.Keys()[0], "$") {
				sql, err = wherePair("element", doc)

				if strings.EqualFold(doc.Keys()[0], "$not") {
					sqlSlice := strings.Split(sql, "OR")
					sql = strings.Replace(sqlSlice[0], "(", "", 1)
				}
				if strings.Contains(sql, " IS SET") {
					sqlSlice := strings.Split(sql, " AND ")
					sql = strings.Replace(sqlSlice[0], "(", "", 1)
				}
			} else {
				var value any
				element := "element." + doc.Keys()[0]
				value, err = doc.Get(doc.Keys()[0])
				if err != nil {
					return "", err
				}

				sql, err = wherePair(element, value)
				if _, ok := value.(*types.Document); ok {
					if _, getErr := value.(*types.Document).Get("$not"); getErr == nil {
						replaceIndex := strings.LastIndex(sql, "UNSET")
						sql = sql[:replaceIndex] + strings.Replace(sql[replaceIndex:], "UNSET", "NULL", 1)
					}
				}
				if strings.Contains(sql, " IS SET") {
					sqlSlice := strings.Split(sql, " AND ")
					sql = strings.Replace(sqlSlice[0], "(", "", 1)
				}
			}

			if err != nil {
				return "", err
			}

			if i == 0 {
				kvSQL += "FOR ANY \"element\" IN " + field + " SATISFIES "
			}
			kvSQL += sql
			i++
		}

		kvSQL += " END "

	case *types.Array:
		if strings.EqualFold(arrayOperator, "elemmatch") {
			return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, "$elemMatch needs an object")
		}
		var value string
		var v any
		for i := 0; i < filters.Len(); i++ {

			if i != 0 {
				kvSQL += " AND "
			}
			v, err = filters.Get(i)
			if err != nil {
				return "", err
			}
			value, _, err = whereValue(v)
			if err != nil {
				return "", err
			}
			kvSQL += "FOR ANY \"element\" IN " + field + " SATISFIES \"element\" = " + value + " END "
		}
	default:
		return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, fmt.Sprintf("If $all: Expected array. If $elemMatch: Expected document. Got instead: %T", filters))
	}

	return kvSQL, nil
}

// regex converts $regex to the SQL equivalent regular expressions
func regex(value any) (string, error) {
	if regex, ok := value.(types.Regex); ok {
		value = regex.Pattern
		if regex.Options != "" {

			return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrNotImplemented, "The use of $options with regular expressions is not supported")
		}
	}

	var vSQL string
	var escape bool
	switch value := value.(type) {
	case string:
		if strings.Contains(value, "(?i)") || strings.Contains(value, "(?-i)") {

			return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrNotImplemented, "The use of (?i) and (?-i) with regular expressions is not supported")
		}

		var dot bool
		for i, s := range value {
			if i == 0 {
				if s == '^' {
					continue
				}
				if s == '.' {
					dot = true
					continue
				}
				if s == '%' || s == '_' {
					vSQL += "%" + "^" + string(s)
					escape = true
					continue
				}
				vSQL += "%" + string(s)
				continue
			}

			if dot && s != '*' && i == 1 {
				vSQL += "%_"
				if s != '.' {
					dot = false
				} else {
					continue
				}
			}

			if i == len(value)-1 {
				if dot && s != '*' {
					vSQL += "_"
					if s == '.' {
						vSQL += "_%"
						continue
					} else {
						dot = false
					}
				}
				if s == '$' {
					continue
				}
				if s == '*' && dot {
					vSQL += "%%"
					continue
				}
				if s == '.' {
					vSQL += "_%"
					continue
				}
				if s == '%' || s == '_' {
					vSQL += "^" + string(s) + "%"
					escape = true
					continue
				}
				vSQL += string(s) + "%"
				continue
			}

			if dot && s != '*' {
				vSQL += "_"
				if s == '.' {
					continue
				} else {
					dot = false
				}
			}

			if s == '.' {
				dot = true
				continue
			} else if s == '*' && dot {
				vSQL += "%"
				dot = false
				continue
			} else if s == '%' || s == '_' {
				vSQL += "^" + string(s)
				escape = true
				continue
			}

			vSQL += string(s)

		}
	default:

		return "", commonerrors.NewCommandErrorMsg(commonerrors.ErrBadValue, fmt.Sprintf("Expected either a JavaScript regular expression objects (i.e. /pattern/) or string containing a pattern. Got instead type %T", value))
	}

	vSQL = "'" + vSQL + "'"
	if escape {
		vSQL += " ESCAPE '^' "
	}

	return vSQL, nil
}
