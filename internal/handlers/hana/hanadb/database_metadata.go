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
	"errors"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

const (
	// Reserved prefix for database and collection names.
	reservedPrefix = "_ferretdb_"

	// Database metadata collection name.
	dbMetadataCollectionName = reservedPrefix + "database_metadata"
)

// metadata is a type to structure methods that work with metadata storing and getting.
//
// Metadata consists of collections and indexes settings.
type metadata struct {
	tx         *sql.Tx
	db         string
	collection string
}

// newMetadata returns a new instance of metadata for the given transaction, database and collection names.
func newMetadata(tx *sql.Tx, db, collection string) *metadata {
	return &metadata{
		tx:         tx,
		db:         db,
		collection: collection,
	}
}

// ensure returns SAP HANA collection name for the given FerretDB database and collection names.
// If such metadata don't exist, it creates them, including the creation of the SAP HANA schema if needed.
// If metadata were created, it returns true as the second return value. If metadata already existed, it returns false.
//
// It makes a document with _id and collection fields and stores it in the dbMetadataCollectionName collection.
// The given FerretDB collection name is stored in the _id field,
// the corresponding SAP HANA collection name is stored in the collection field.
// For _id field it creates unique index.
//
// It returns a possibly wrapped error:
//   - ErrInvalidDatabaseName - if the given database name doesn't conform to restrictions.
//   - *transactionConflictError - if a SAP HANA conflict occurs (the caller could retry the transaction).
func (m *metadata) ensure(ctx context.Context) (collectionName string, created bool, err error) {
	collectionName, err = m.getCollectionName(ctx)

	switch {
	case err == nil:
		// metadata already exist
		return

	case errors.Is(err, ErrCollectionNotExist):
		// metadata don't exist, do nothing
	default:
		return "", false, lazyerrors.Error(err)
	}

	err = CreateDatabaseIfNotExists(ctx, m.tx, m.db)

	switch {
	case err == nil:
		// do nothing
	case errors.Is(err, ErrInvalidDatabaseName):
		return
	default:
		return "", false, lazyerrors.Error(err)
	}

	if err = createHANACollectionIfNotExists(ctx, m.tx, m.db, dbMetadataCollectionName); err != nil {
		return "", false, lazyerrors.Error(err)
	}

	// For now SAP HANA seems to be able to store collections with same length of name and same characters,
	// so for now m.collection will be collection name
	metadata := must.NotFail(types.NewDocument(
		"_id", m.collection,
		"collection", collectionName,
		"indexes", must.NotFail(types.NewArray()),
	))

	exists, err := collectionExists(ctx, m.tx, m.db, m.collection)

	if exists {
		// If metadata were created by another transaction we consider it transaction conflict error
		// to mark that transaction should be retried.
		return "", false, lazyerrors.Error(newTransactionConflictError(err))
	}

	err = insert(ctx, m.tx, insertParams{
		db:         m.db,
		collection: dbMetadataCollectionName,
		doc:        metadata,
	})

	switch {
	case err == nil:
		return collectionName, true, nil
	default:
		return "", false, lazyerrors.Error(err)
	}
}

// getCollectionName returns SAP HANA collection name for the given FerretDB database and collection.
//
// If such metadata don't exist, it returns ErrCollectionNotExist.
func (m *metadata) getCollectionName(ctx context.Context) (string, error) {
	doc, err := m.get(ctx)
	if err != nil {
		return "", lazyerrors.Error(err)
	}

	collection := must.NotFail(doc.Get("collection"))

	return collection.(string), nil
}

// get returns metadata stored in the metadata collection.
//
// If such metadata don't exist, it returns ErrCollectionNotExist.
func (m *metadata) get(ctx context.Context) (*types.Document, error) {
	metadataExist, err := collectionExists(ctx, m.tx, m.db, dbMetadataCollectionName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if !metadataExist {
		return nil, ErrCollectionNotExist
	}

	iterParams := &iteratorParams{
		db:         m.db,
		collection: dbMetadataCollectionName,
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
		return nil, ErrCollectionNotExist
	default:
		return nil, lazyerrors.Error(err)
	}
}
