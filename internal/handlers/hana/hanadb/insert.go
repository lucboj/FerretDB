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

	"github.com/FerretDB/FerretDB/internal/handlers/pg/pjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

// insertParams describes the parameters for inserting a document into a collection.
type insertParams struct {
	doc        *types.Document // document to insert
	db         string          // SAP HANA schema name
	collection string          // SAP HANA collection name
}

// insert marshals and inserts a document with the given params.
//
// It returns possibly wrapped error:
//   - ErrUniqueViolation - if the pgerrcode.UniqueViolation error is caught (e.g. due to unique index constraint).
func insert(ctx context.Context, tx *sql.Tx, p insertParams) error {
	sql := fmt.Sprintf("INSERT INTO \"%s\".\"%s\" VALUES ($1)", p.db, p.collection)

	_, err := tx.ExecContext(ctx, sql, must.NotFail(pjson.Marshal(p.doc)))
	if err == nil {
		return nil
	}

	return lazyerrors.Error(err)

}
