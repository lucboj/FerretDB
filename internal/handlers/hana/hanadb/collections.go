package hanadb

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// validateCollectionNameRe validates collection names.
var validateCollectionNameRe = regexp.MustCompile("^[a-zA-Z_-][a-zA-Z0-9_-]{0,119}$")

// Reserved prefix for database and collection names.
var reservedPrefix = "_ferretdb_"

// CreateCollectionIfNotExists ensures that given FerretDB database and collection exist.
// If the database does not exist, it will be created.
//
// It returns possibly wrapped error:
//   - ErrInvalidDatabaseName - if the given database name doesn't conform to restrictions.
//   - ErrInvalidCollectionName - if the given collection name doesn't conform to restrictions.
//   - *transactionConflictError - if a PostgreSQL conflict occurs (the caller could retry the transaction).
func (hdb *Pool) CreateCollectionIfNotExists(ctx context.Context, db, collection string) error {
	if !validateCollectionNameRe.MatchString(collection) ||
		strings.HasPrefix(collection, reservedPrefix) {
		return ErrInvalidCollectionName
	}

	var err error
	exists, err := hdb.collectionExists(ctx, db, collection)
	if err != nil {
		return err
	}
	if exists {
		// table already exists.
		return nil
	}

	err = hdb.createCollection(ctx, db, collection)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return err
}

func (hdb *Pool) collectionExists(ctx context.Context, db, collection string) (bool, error) {

	sql := fmt.Sprintf("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = '%s' AND table_name = '%s' AND TABLE_TYPE = 'COLLECTION'", db, collection)

	var count int
	err := hdb.QueryRowContext(ctx, sql).Scan(&count)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if count > 0 {
		return true, nil
	}

	return false, nil
}

// TODO: catch and use error for collection already existing
func (hdb *Pool) createCollection(ctx context.Context, db, collection string) error {
	sql := fmt.Sprintf("CREATE COLLECTION \"%s\".\"%s\"")

	_, err := hdb.ExecContext(ctx, sql)

	return err
}
