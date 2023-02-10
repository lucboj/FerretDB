package hanadb

import (
	"context"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

func (hdb *Pool) DeleteDocumentsByID(ctx context.Context, sp *SQLParam, ids []any) (int64, error) {

	sql := fmt.Sprintf("DELETE FROM \"%s\".\"%s\"", sp.DB, sp.Collection)

	whereSQL := "WHERE \"_id\" IN ("

	for i, id := range ids {
		idsVals, _, err := whereValue(id)
		if err != nil {
			return 0, err
		}
		if i != 0 {
			whereSQL += ", "
		}
		whereSQL += idsVals

	}

	sql += whereSQL + ")"

	tag, err := hdb.ExecContext(ctx, sql)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected()
}

func (hdb *Pool) DeleteOneDocumentByID(ctx context.Context, sp *SQLParam, id any) error {
	whereSQL, err := CreateWhereClause(must.NotFail(types.NewDocument("_id", id)))
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("DELETE FROM \"%s\".\"%s\"", sp.DB, sp.Collection)
	sql += whereSQL

	_, err = hdb.ExecContext(ctx, sql)

	return err
}
