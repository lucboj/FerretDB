package hanadb

import (
	"context"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/handlers/hana/hjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

type SQLParam struct {
	DB         string
	Collection string
	Comment    string
	// Query filter for possible pushdown; may be ignored in part or entirely.
	Filter *types.Document
}

func (hdb *Pool) GetDocuments(ctx context.Context, SQLParam *SQLParam) ([]*types.Document, error) {

	query := fmt.Sprintf("SELECT * FROM \"%s\".\"%s\"", SQLParam.DB, SQLParam.Collection)

	rows, err := hdb.QueryContext(ctx, query)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var res []*types.Document
	var d []byte

	for rows.Next() {
		if err := rows.Scan(&d); err != nil {
			return nil, lazyerrors.Error(err)
		}
		doc, err := hjson.Unmarshal(d)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		res = append(res, doc)
	}

	return res, nil
}
