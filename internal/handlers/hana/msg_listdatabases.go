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

	"github.com/FerretDB/FerretDB/internal/handlers/common"
	"github.com/FerretDB/FerretDB/internal/handlers/hana/hanadb"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/wire"
)

// MsgListDatabases implements HandlerInterface.
func (h *Handler) MsgListDatabases(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	dbPool, err := h.DBPool(ctx)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var filter *types.Document
	if filter, err = common.GetOptionalParam(document, "filter", filter); err != nil {
		return nil, err
	}

	common.Ignored(document, h.L, "comment", "authorizedDatabases")

	nameOnly, err := common.GetBoolOptionalParam(document, "nameOnly")
	if err != nil {
		return nil, err
	}

	var totalSize int64
	var databases *types.Array
	err = dbPool.InTransaction(ctx, func(tx *sql.Tx) error {
		var databaseNames []string
		var err error
		databaseNames, err = hanadb.Databases(ctx, tx)
		if err != nil {
			return err
		}

		databases = types.MakeArray(len(databaseNames))
		for _, databaseName := range databaseNames {
			var sizeOnDisk int64
			sizeOnDisk, err = hanadb.CollectionsSize(ctx, tx, databaseName)
			if err != nil {
				return err
			}

			totalSize += sizeOnDisk

			d := must.NotFail(types.NewDocument(
				"name", databaseName,
				"sizeOnDisk", sizeOnDisk,
				"empty", sizeOnDisk == 0,
			))

			matches, err := common.FilterDocument(d, filter)
			if err != nil {
				return err
			}

			if !matches {
				continue
			}

			if nameOnly {
				d = must.NotFail(types.NewDocument(
					"name", databaseName,
				))
			}

			databases.Append(d)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	var reply wire.OpMsg

	switch {
	case nameOnly:
		must.NoError(reply.SetSections(wire.OpMsgSection{
			Documents: []*types.Document{must.NotFail(types.NewDocument(
				"databases", databases,
				"ok", float64(1),
			))},
		}))
	default:
		must.NoError(reply.SetSections(wire.OpMsgSection{
			Documents: []*types.Document{must.NotFail(types.NewDocument(
				"databases", databases,
				"totalSize", totalSize,
				"totalSizeMb", totalSize/1024/1024,
				"ok", float64(1),
			))},
		}))
	}

	return &reply, nil
}
