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

package hana

import (
	"context"
	"errors"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/handlers/common"
	"github.com/FerretDB/FerretDB/internal/handlers/commonerrors"
	"github.com/FerretDB/FerretDB/internal/handlers/hana/hanadb"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/wire"
)

// MsgUpdate implements HandlerInterface.
func (h *Handler) MsgUpdate(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	dbPool, err := h.DBPool(ctx)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err := common.Unimplemented(document, "let"); err != nil {
		return nil, err
	}

	common.Ignored(document, h.L, "ordered", "writeConcern", "bypassDocumentValidation")

	var sp hanadb.SQLParam

	if sp.DB, err = common.GetRequiredParam[string](document, "$db"); err != nil {
		return nil, err
	}

	collectionParam, err := document.Get(document.Command())
	if err != nil {
		return nil, err
	}

	var ok bool
	if sp.Collection, ok = collectionParam.(string); !ok {
		return nil, common.NewCommandErrorMsgWithArgument(
			common.ErrBadValue,
			fmt.Sprintf("collection name has invalid type %s", common.AliasFromType(collectionParam)),
			document.Command(),
		)
	}

	var updates *types.Array
	if updates, err = common.GetOptionalParam(document, "updates", updates); err != nil {
		return nil, err
	}

	err = dbPool.CreateCollectionIfNotExists(ctx, sp.DB, sp.Collection)
	if err != nil {
		if errors.Is(err, hanadb.ErrInvalidCollectionName) ||
			errors.Is(err, hanadb.ErrInvalidDatabaseName) {
			msg := fmt.Sprintf("Invalid namespace: %s.%s", sp.DB, sp.Collection)
			return nil, common.NewCommandErrorMsg(common.ErrInvalidNamespace, msg)
		}
		return nil, err
	}

	var matched, modified int32
	var upserted types.Array

	for i := 0; i < updates.Len(); i++ {
		update, err := common.AssertType[*types.Document](must.NotFail(updates.Get(i)))
		if err != nil {
			return nil, err
		}

		unimplementedFields := []string{
			"c",
			"collation",
			"arrayFilters",
			"hint",
		}
		if err := common.Unimplemented(update, unimplementedFields...); err != nil {
			return nil, err
		}

		var q, u *types.Document
		var upsert bool
		var multi bool
		if q, err = common.GetOptionalParam(update, "q", q); err != nil {
			return nil, err
		}
		if u, err = common.GetOptionalParam(update, "u", u); err != nil {
			// TODO check if u is an array of aggregation pipeline stages
			return nil, err
		}

		// get comment from options.Update().SetComment() method
		if sp.Comment, err = common.GetOptionalParam(document, "comment", sp.Comment); err != nil {
			return nil, err
		}

		// get comment from query, e.g. db.collection.UpdateOne({"_id":"string", "$comment: "test"},{$set:{"v":"foo""}})
		if sp.Comment, err = common.GetOptionalParam(q, "$comment", sp.Comment); err != nil {
			return nil, err
		}

		if u != nil {
			if err = common.ValidateUpdateOperators(u); err != nil {
				return nil, err
			}
		}

		if upsert, err = common.GetOptionalParam(update, "upsert", upsert); err != nil {
			return nil, err
		}

		if multi, err = common.GetOptionalParam(update, "multi", multi); err != nil {
			return nil, err
		}

		sp.Filter = q

		resDocs, err := fetchAndFilterDocs(ctx, dbPool, &sp)
		if err != nil {
			return nil, err
		}

		if len(resDocs) == 0 {
			if !upsert {
				// nothing to do, continue to the next update operation
				continue
			}

			doc := q.DeepCopy()
			if _, err = common.UpdateDocument(doc, u); err != nil {
				return nil, err
			}
			if !doc.Has("_id") {
				doc.Set("_id", types.NewObjectID())
			}

			upserted.Append(must.NotFail(types.NewDocument(
				"index", int32(0), // TODO
				"_id", must.NotFail(doc.Get("_id")),
			)))

			if err = insertDocument(ctx, dbPool, &sp, doc); err != nil {
				return nil, err
			}

			matched++
			continue
		}

		if len(resDocs) > 1 && !multi {
			resDocs = resDocs[:1]
		}

		matched += int32(len(resDocs))

		for _, doc := range resDocs {
			changed, err := common.UpdateDocument(doc, u)
			if err != nil {
				return nil, err
			}

			if !changed {
				continue
			}

			rowsChanged, err := updateDocument(ctx, dbPool, &sp, doc)
			if err != nil {
				return nil, err
			}
			modified += int32(rowsChanged)
		}
	}

	res := must.NotFail(types.NewDocument(
		"n", matched,
	))

	if upserted.Len() != 0 {
		res.Set("upserted", &upserted)
	}

	res.Set("nModified", modified)
	res.Set("ok", float64(1))

	var reply wire.OpMsg
	must.NoError(reply.SetSections(wire.OpMsgSection{
		Documents: []*types.Document{res},
	}))

	return &reply, nil
}

// updateDocument updates documents by _id.
func updateDocument(ctx context.Context, dbPool *hanadb.Pool, sp *hanadb.SQLParam, doc *types.Document) (int64, error) {
	id := must.NotFail(doc.Get("_id"))

	res, err := dbPool.SetDocumentByID(ctx, sp, id, doc)
	if err == nil {
		return res, nil
	}

	return 0, commonerrors.CheckError(err)
}
