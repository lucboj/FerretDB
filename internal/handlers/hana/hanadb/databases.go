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
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// validateDatabaseNameRe validates FerretDB database / SAP HANA schema names.
var validateDatabaseNameRe = regexp.MustCompile("^[-_a-z][-_a-z0-9]{0,62}$")

// Reserved prefix for database and collection names.
const reservedPrefix = "_ferretdb_"

// Databases returns a sorted list of FerretDB databases / PostgreSQL schemas.
func (hdb *Pool) Databases(ctx context.Context) ([]string, error) {
	sql := "SELECT SCHEMA_NAME FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_OWNER NOT LIKE '%_SYS_%' AND NOT SCHEMA_OWNER = 'SYS';"
	rows, err := hdb.QueryContext(ctx, sql)

	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	defer rows.Close()

	res := make([]string, 0, 2)

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, lazyerrors.Error(err)
		}

		res = append(res, name)
	}

	if err = rows.Err(); err != nil {
		return nil, lazyerrors.Error(err)
	}

	return res, nil
}

// CreateDatabaseIfNotExists ensures that given database exists.
//
// If the database doesn't exist, it creates it.
//
// It returns true if the database was created.
func (hdb *Pool) CreateDatabaseIfNotExists(ctx context.Context, db string) (bool, error) {
	if !validateDatabaseNameRe.MatchString(db) ||
		strings.HasPrefix(db, reservedPrefix) {
		return false, ErrInvalidDatabaseName
	}

	dbExists, err := hdb.DatabaseExists(ctx, db)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if dbExists {
		return true, nil
	}

	sql := fmt.Sprintf("CREATE SCHEMA \"%s\"", db)
	_, err = hdb.ExecContext(ctx, sql)

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	return false, nil
}

// DatabaseExists checks if the given database already exists.
//
// It returns true if yes and false if no.
func (hdb *Pool) DatabaseExists(ctx context.Context, db string) (bool, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = '%s'", db)

	var count int
	err := hdb.QueryRowContext(ctx, sql).Scan(&count)

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if count > 0 {
		return true, nil
	}

	return false, nil
}

// TablesSize returns the sum of sizes of all tables in the given database in bytes.
//
// The calculated sizes will not be correct as SAP HANA is an in-memory database.
// It will only show size of what is currently in memory.
func (hdb *Pool) TablesSize(ctx context.Context, db string) (int64, error) {
	var sizeOnDisk int64

	collections, err := hdb.Collections(ctx, db)
	if err != nil {
		return 0, err
	}

	for _, name := range collections {
		var tableSize any
		err = hdb.QueryRowContext(ctx, "SELECT TABLE_SIZE FROM \"PUBLIC\".\"M_TABLES\" "+
			"WHERE SCHEMA_NAME = $1 AND TABLE_NAME = $2 AND TABLE_TYPE = 'COLLECTION';",
			db, name).Scan(&tableSize)

		if err != nil {
			return 0, err
		}

		switch tableSize := tableSize.(type) {
		case int64: // collection is in memory and a size can be calculated
			sizeOnDisk += tableSize
		case nil: // collection is not in memory and no size can be calculated
			continue
		default:
			return 0, lazyerrors.Errorf("Got wrong type for tableSize. Got: %T", tableSize)
		}
	}

	return sizeOnDisk, nil
}
