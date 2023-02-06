package hanadb

import (
	"context"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/handlers/hana/hjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// InsertDocument inserts a document into FerretDB database and collection.
// If database or collection does not exist, it will be created.
// If the document is not valid, it returns *types.ValidationError.
func (hdb *Pool) InsertDocument(ctx context.Context, db, collection string, doc *types.Document) error {
	if err := doc.ValidateData(); err != nil {
		return err
	}

	if err := hdb.CreateCollectionIfNotExists(ctx, db, collection); err != nil {
		return lazyerrors.Error(err)
	}

	b, err := hjson.Marshal(doc)
	if err != nil {
		return lazyerrors.Error(err)
	}

	sql := fmt.Sprintf("INSERT INTO \"%s\".\"%s\" VALUES ($1)", db, collection)

	if _, err = hdb.ExecContext(ctx, sql, b); err != nil {
		return err
	}

	// _, err = hdb.Driver.UseDatabase(db).Insert(ctx, collection, []driver.Document{b})

	return err
}
