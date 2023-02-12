package hanadb

import (
	"context"
	"errors"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/handlers/commonerrors"
	"github.com/FerretDB/FerretDB/internal/handlers/pg/pgdb"
	"github.com/FerretDB/FerretDB/internal/types"
)

// TODO: When JSON Documentstore supports replacing document with JSON string using PARSE_JSON change function to use UPDATE statement
func (hdb *Pool) SetDocumentByID(ctx context.Context, sp *SQLParam, id any, doc *types.Document) (int64, error) {

	//sql := fmt.Sprintf("UPDATE \"%s\".\"%s\" SET \"%s\"=$1 WHERE \"_id\" = '63e4e43a35baf79b46f2a300'", sp.DB, sp.Collection, sp.Collection)

	err := hdb.DeleteOneDocumentByID(ctx, sp, id)
	if err != nil {
		return 0, err
	}

	err = hdb.InsertDocument(ctx, sp.DB, sp.Collection, doc)
	if err == nil {
		return 1, nil
	}

	if errors.Is(err, pgdb.ErrInvalidCollectionName) || errors.Is(err, pgdb.ErrInvalidDatabaseName) {
		msg := fmt.Sprintf("Invalid namespace: %s.%s", sp.DB, sp.Collection)
		return 0, commonerrors.NewCommandErrorMsg(commonerrors.ErrInvalidNamespace, msg)
	}

	return 0, commonerrors.CheckError(err)
}
