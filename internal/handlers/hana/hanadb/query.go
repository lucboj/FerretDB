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
	"fmt"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// FetchedDocs is a struct that contains a list of documents and an error.
// It is used in the fetched channel returned by QueryDocuments.
type FetchedDocs struct {
	Err  error
	Docs []*types.Document
}

// QueryParams represents options/parameters used for SQL query.
type QueryParams struct {
	// Query filter for possible pushdown; may be ignored in part or entirely.
	DB         string
	Collection string
}

// QueryDocuments returns an queryIterator to fetch documents for given SQLParams.
// If the collection doesn't exist, it returns an empty iterator and no error.
// If an error occurs, it returns nil and that error, possibly wrapped.
//
// Transaction is not closed by this function. Use iterator.WithClose if needed.
func QueryDocuments(ctx context.Context, tx *sql.Tx, qp *QueryParams) (types.DocumentsIterator, error) {
	collection, err := newMetadata(tx, qp.DB, qp.Collection).getCollectionName(ctx)

	switch {
	case err == nil:
		// do nothing
	case errors.Is(err, ErrCollectionNotExist):
		return newIterator(ctx, nil, new(iteratorParams)), nil
	default:
		return nil, lazyerrors.Error(err)
	}

	iter, err := buildIterator(ctx, tx, &iteratorParams{
		db:         qp.DB,
		collection: collection,
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return iter, nil
}

// iteratorParams contains parameters for building an iterator.
type iteratorParams struct {
	unmarshal  func(b []byte) (*types.Document, error) // if set, iterator uses unmarshal to convert row to *types.Document.
	db         string
	collection string
}

// buildIterator returns an iterator to fetch documents for given iteratorParams.
func buildIterator(ctx context.Context, tx *sql.Tx, p *iteratorParams) (types.DocumentsIterator, error) {
	query := fmt.Sprintf("SELECT * FROM \"%s\".\"%s\"", p.db, p.collection)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return newIterator(ctx, rows, p), nil
}
