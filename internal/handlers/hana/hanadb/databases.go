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
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// validateDatabaseNameRe validates FerretDB database / SAP HANA schema names.
var validateDatabaseNameRe = regexp.MustCompile("^[-_a-z][-_a-z0-9]{0,62}$")

// Databases returns a sorted list of FerretDB databases / SAP HANA schemas.
func Databases(ctx context.Context, tx *sql.Tx) ([]string, error) {
	sql := "SELECT SCHEMA_NAME FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_OWNER NOT LIKE '%_SYS_%' AND NOT SCHEMA_OWNER = 'SYS';"
	rows, err := tx.QueryContext(ctx, sql)

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
func CreateDatabaseIfNotExists(ctx context.Context, tx *sql.Tx, db string) error {
	if !validateDatabaseNameRe.MatchString(db) ||
		strings.HasPrefix(db, reservedPrefix) {
		return ErrInvalidDatabaseName
	}

	dbExists, err := DatabaseExists(ctx, tx, db)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if dbExists {
		return nil
	}

	sql := fmt.Sprintf("CREATE SCHEMA \"%s\"", db)
	_, err = tx.ExecContext(ctx, sql)

	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

// DatabaseExists checks if the given database already exists.
//
// It returns true if yes and false if no.
func DatabaseExists(ctx context.Context, tx *sql.Tx, db string) (bool, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = '%s'", db)

	var count int
	err := tx.QueryRowContext(ctx, sql).Scan(&count)

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if count > 0 {
		return true, nil
	}

	return false, nil
}

// collectionsSize returns the sum of sizes of all collections in the given database in bytes.
//
// The calculated sizes will not be correct as SAP HANA is an in-memory database.
// It will only show size of what is currently in memory.
func CollectionsSize(ctx context.Context, tx *sql.Tx, db string) (int64, error) {
	var sizeOnDisk int64

	collections, err := Collections(ctx, tx, db)
	if err != nil {
		return 0, err
	}

	for _, name := range collections {
		var collectionSize any
		err = tx.QueryRowContext(ctx, "SELECT TABLE_SIZE FROM \"PUBLIC\".\"M_TABLES\" "+
			"WHERE SCHEMA_NAME = $1 AND TABLE_NAME = $2 AND TABLE_TYPE = 'COLLECTION';",
			db, name).Scan(&collectionSize)

		if err != nil {
			return 0, err
		}

		switch collectionSize := collectionSize.(type) {
		case int64: // collection is in memory and a size can be calculated
			sizeOnDisk += collectionSize
		case nil: // collection is not in memory and no size can be calculated
			continue
		default:
			return 0, lazyerrors.Errorf("Got wrong type for collectionSize. Got: %T", collectionSize)
		}
	}

	return sizeOnDisk, nil
}
