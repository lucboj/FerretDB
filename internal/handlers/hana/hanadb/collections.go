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

	"golang.org/x/exp/slices"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// validateCollectionNameRe validates collection names.
var validateCollectionNameRe = regexp.MustCompile("^[a-zA-Z_-][a-zA-Z0-9_-]{0,119}$")

// Collections returns a sorted list of FerretDB collection names.
//
// It returns (possibly wrapped) ErrSchemaNotExist if FerretDB database / SAP HANA schema does not exist.
func Collections(ctx context.Context, tx *sql.Tx, db string) ([]string, error) {
	dbExists, err := DatabaseExists(ctx, tx, db)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if !dbExists {
		return []string{}, nil
	}

	sql := "SELECT TABLE_NAME FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_TYPE = 'COLLECTION';"
	rows, err := tx.QueryContext(ctx, sql, db)

	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	defer rows.Close()

	var collections []string

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, lazyerrors.Error(err)
		}

		collections = append(collections, name)
	}

	if err = rows.Err(); err != nil {
		return nil, lazyerrors.Error(err)
	}

	slices.Sort(collections)

	return collections, nil
}

// collectionExists returns true if FerretDB collection exists.
func collectionExists(ctx context.Context, tx *sql.Tx, db, collection string) (bool, error) {
	sql := fmt.Sprintf(
		"SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" "+
			"WHERE SCHEMA_NAME = '%s' AND table_name = '%s' AND TABLE_TYPE = 'COLLECTION'",
		db,
		collection,
	)

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

// CreateCollection creates a new FerretDB collection with the given name in the given database.
// If the database does not exist, it will be created.
//
// It returns possibly wrapped error:
//   - ErrInvalidDatabaseName - if the given database name doesn't conform to restrictions.
//   - ErrInvalidCollectionName - if the given collection name doesn't conform to restrictions.
//   - ErrAlreadyExist - if a FerretDB collection with the given name already exists.
func CreateCollection(ctx context.Context, tx *sql.Tx, db, collection string) error {
	if !validateCollectionNameRe.MatchString(collection) ||
		strings.HasPrefix(collection, reservedPrefix) {
		return ErrInvalidCollectionName
	}
	collection, created, err := newMetadata(tx, db, collection).ensure(ctx)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !created {
		return ErrAlreadyExist
	}

	if err = createHANACollectionIfNotExists(ctx, tx, db, collection); err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

// createHANACollectionIfNotExists creates the given SAP HANA collection in the given schema if the collection doesn't exist.
// If the collection already exists, it does nothing.
//
// If a SAP HANA conflict occurs it returns errTransactionConflict, and the caller could retry the transaction.
func createHANACollectionIfNotExists(ctx context.Context, tx *sql.Tx, schema, collection string) error {
	var err error

	sql := fmt.Sprintf("CREATE COLLECTION \"%s\".\"%s\"", schema, collection)

	if _, err = tx.ExecContext(ctx, sql); err == nil {
		return nil
	}

	if err != nil {
		if strings.Contains(err.Error(), "288: cannot use duplicate table name") {
			return ErrAlreadyExist
		}
	}

	return lazyerrors.Error(err)
}
