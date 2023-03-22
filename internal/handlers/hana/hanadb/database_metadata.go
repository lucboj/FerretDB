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
	"errors"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

const (
	// Reserved prefix for database and collection names.
	reservedPrefix = "_ferretdb_"

	// Database metadata table name.
	dbMetadataTableName = reservedPrefix + "database_metadata"
)

// metadata is a type to structure methods that work with metadata storing and getting.
//
// Metadata consists of collections and indexes settings.
type metadata struct {
	hdb        *Pool
	db         string
	collection string
}

// newMetadata returns a new instance of metadata for the given transaction, database and collection names.
func newMetadata(hdb *Pool, db, collection string) *metadata {
	return &metadata{
		hdb:        hdb,
		db:         db,
		collection: collection,
	}
}

// ensure returns SAP HANA collection name for the given FerretDB database and collection names.
// If such metadata don't exist, it creates them, including the creation of the SAP HANA schema if needed.
// If metadata were created, it returns true as the second return value. If metadata already existed, it returns false.
//
// It makes a document with _id and table fields and stores it in the dbMetadataTableName table.
// The given FerretDB collection name is stored in the _id field,
// the corresponding SAP HANA collection name is stored in the table field.
// For _id field it creates unique index.
//
// It returns a possibly wrapped error:
//   - ErrInvalidDatabaseName - if the given database name doesn't conform to restrictions.
//   - *transactionConflictError - if a PostgreSQL conflict occurs (the caller could retry the transaction).
func (m *metadata) ensure(ctx context.Context) (tableName string, created bool, err error) {
	tableName, err = m.getTableName(ctx)

	switch {
	case err == nil:
		// metadata already exist
		return

	case errors.Is(err, ErrTableNotExist):
		// metadata don't exist, do nothing
	default:
		return "", false, lazyerrors.Error(err)
	}

	err = CreateDatabaseIfNotExists(ctx, m.hdb, m.db)

	switch {
	case err == nil:
		// do nothing
	case errors.Is(err, ErrInvalidDatabaseName):
		return
	default:
		return "", false, lazyerrors.Error(err)
	}

	if err = createPGTableIfNotExists(ctx, m.hdb, m.db, dbMetadataTableName); err != nil {
		return "", false, lazyerrors.Error(err)
	}

	// Index to ensure that collection name is unique
	// if err = createPGIndexIfNotExists(ctx, m.tx, m.db, dbMetadataTableName, dbMetadataIndexName, true); err != nil {
	// 	return "", false, lazyerrors.Error(err)
	// }

	// Need?
	tableName = formatCollectionName(m.collection)
	metadata := must.NotFail(types.NewDocument(
		"_id", m.collection,
		"table", tableName,
		"indexes", must.NotFail(types.NewArray()),
	))

	err = insert(ctx, m.tx, insertParams{
		schema: m.db,
		table:  dbMetadataTableName,
		doc:    metadata,
	})

	switch {
	case err == nil:
		return tableName, true, nil
	case errors.Is(err, ErrUniqueViolation):
		// If metadata were created by another transaction we consider it transaction conflict error
		// to mark that transaction should be retried.
		return "", false, lazyerrors.Error(newTransactionConflictError(err))
	default:
		return "", false, lazyerrors.Error(err)
	}
}

// getTableName returns PostgreSQL table name for the given FerretDB database and collection.
//
// If such metadata don't exist, it returns ErrTableNotExist.
func (m *metadata) getTableName(ctx context.Context) (string, error) {
	doc, err := m.get(ctx, false)
	if err != nil {
		return "", lazyerrors.Error(err)
	}

	table := must.NotFail(doc.Get("table"))

	return table.(string), nil
}

// get returns metadata stored in the metadata table.
//
// If such metadata don't exist, it returns ErrTableNotExist.
func (m *metadata) get(ctx context.Context, forUpdate bool) (*types.Document, error) {
	metadataExist, err := tableExists(ctx, m.tx, m.db, dbMetadataTableName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if !metadataExist {
		return nil, ErrTableNotExist
	}

	iterParams := &iteratorParams{
		schema:    m.db,
		table:     dbMetadataTableName,
		filter:    must.NotFail(types.NewDocument("_id", m.collection)),
		forUpdate: forUpdate,
	}

	iter, err := buildIterator(ctx, m.tx, iterParams)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	defer iter.Close()

	// call iterator only once as only one document is expected.
	_, doc, err := iter.Next()

	switch {
	case err == nil:
		return doc, nil
	case errors.Is(err, iterator.ErrIteratorDone):
		// no metadata found for the given collection name
		return nil, ErrTableNotExist
	default:
		return nil, lazyerrors.Error(err)
	}
}

// set sets metadata for the given database and collection.
//
// To avoid data race, set should be called only after getMetadata with forUpdate = true is called,
// so that the metadata table is locked correctly.
func (m *metadata) set(ctx context.Context, doc *types.Document) error {
	if _, err := setById(ctx, m.tx, m.db, dbMetadataTableName, "", m.collection, doc); err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

// remove removes metadata.
//
// If such metadata don't exist, it doesn't return an error.
func (m *metadata) remove(ctx context.Context) error {
	_, err := deleteByIDs(ctx, m.tx, execDeleteParams{
		schema: m.db,
		table:  dbMetadataTableName,
	}, []any{m.collection},
	)

	if err == nil {
		return nil
	}

	return lazyerrors.Error(err)
}
