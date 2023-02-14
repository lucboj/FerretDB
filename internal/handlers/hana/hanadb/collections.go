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
func (hdb *Pool) CreateCollectionIfNotExists(ctx context.Context, db, collection string) error {
	if !validateCollectionNameRe.MatchString(collection) ||
		strings.HasPrefix(collection, reservedPrefix) {
		return ErrInvalidCollectionName
	}

	var err error

	_, err = hdb.CreateDatabaseIfNotExists(ctx, db)
	if err != nil {
		return err
	}

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
	sql := fmt.Sprintf("CREATE COLLECTION \"%s\".\"%s\"", db, collection)

	_, err := hdb.ExecContext(ctx, sql)

	return err
}

func (hdb *Pool) DropCollection(ctx context.Context, db, collection string) error {
	schemaExists, err := hdb.databaseExists(ctx, db)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !schemaExists {
		return ErrSchemaNotExist
	}

	collectionExists, err := hdb.collectionExists(ctx, db, collection)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !collectionExists {
		return ErrTableNotExist
	}

	sql := fmt.Sprintf("DROP COLLECTION \"%s\".\"%s\"", db, collection)

	_, err = hdb.ExecContext(ctx, sql)

	return err
}

func (hdb *Pool) Collections(ctx context.Context, db string) ([]string, error) {
	if _, err := hdb.CreateDatabaseIfNotExists(ctx, db); err != nil && err != ErrAlreadyExist {
		return nil, lazyerrors.Errorf("Handler.msgStorage: %w", err)
	}

	sql := "SELECT TABLE_NAME FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_TYPE = 'COLLECTION';"
	rows, err := hdb.QueryContext(ctx, sql, db)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	defer rows.Close()

	var collections []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, lazyerrors.Error(err)
		}

		collections = append(collections, name)
	}
	if err = rows.Err(); err != nil {
		return nil, lazyerrors.Error(err)
	}

	return collections, nil
}
