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
	"database/sql"
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

// MsgCreate implements HandlerInterface.
func (h *Handler) MsgCreate(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	dbPool, err := h.DBPool(ctx)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	unimplementedFields := []string{
		"capped",
		"timeseries",
		"expireAfterSeconds",
		"size",
		"max",
		"validationLevel",
		"validationAction",
		"viewOn",
		"pipeline",
		"collation",
	}
	if err = common.Unimplemented(document, unimplementedFields...); err != nil {
		return nil, err
	}

	ignoredFields := []string{
		"autoIndexId",
		"storageEngine",
		"indexOptionDefaults",
		"writeConcern",
		"comment",
	}
	common.Ignored(document, h.L, ignoredFields...)

	command := document.Command()

	db, err := common.GetRequiredParam[string](document, "$db")
	if err != nil {
		return nil, err
	}

	collection, err := common.GetRequiredParam[string](document, command)
	if err != nil {
		return nil, err
	}

	err = dbPool.InTransactionRetry(ctx, func(tx *sql.Tx) error {
		err = hanadb.CreateCollection(ctx, tx, db, collection)

		switch {
		case err == nil:
			return nil

		case errors.Is(err, hanadb.ErrAlreadyExist):
			msg := fmt.Sprintf("Collection %s.%s already exists.", db, collection)
			return commonerrors.NewCommandErrorMsg(commonerrors.ErrNamespaceExists, msg)

		case errors.Is(err, hanadb.ErrInvalidDatabaseName):
			msg := fmt.Sprintf("Invalid namespace: %s.%s", db, collection)
			return commonerrors.NewCommandErrorMsg(commonerrors.ErrInvalidNamespace, msg)

		case errors.Is(err, hanadb.ErrInvalidCollectionName):
			msg := fmt.Sprintf("Invalid collection name: '%s.%s'", db, collection)
			return commonerrors.NewCommandErrorMsg(commonerrors.ErrInvalidNamespace, msg)

		default:
			return lazyerrors.Error(err)
		}
	})
	if err != nil {
		return nil, err
	}

	var reply wire.OpMsg
	must.NoError(reply.SetSections(wire.OpMsgSection{
		Documents: []*types.Document{must.NotFail(types.NewDocument(
			"ok", float64(1),
		))},
	}))

	return &reply, nil
}
