package hanadb

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// validateDatabaseNameRe validates FerretDB database / PostgreSQL schema names.
var validateDatabaseNameRe = regexp.MustCompile("^[-_a-z][-_a-z0-9]{0,62}$")

// Databases returns a sorted list of FerretDB databases / PostgreSQL schemas.
func (hdb *Pool) Databases(ctx context.Context) ([]string, error) {
	sql := "SELECT SCHEMA_NAME FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_OWNER NOT LIKE '%_SYS_%' AND NOT SCHEMA_OWNER = 'SYS';"
	rows, err := hdb.QueryContext(ctx, sql)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	defer rows.Close()

	res := make([]string, 0, 2)
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, lazyerrors.Error(err)
		}

		res = append(res, name)
	}
	if err = rows.Err(); err != nil {
		return nil, lazyerrors.Error(err)
	}

	return res, nil
}

// createDatabaseIfNotExists ensures that given database exists.
// If the database doesn't exist, it creates it.
// It returns true if the database was created.
func (hdb *Pool) CreateDatabaseIfNotExists(ctx context.Context, db string) (bool, error) {
	dbExists, err := hdb.databaseExists(ctx, db)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if dbExists {
		return true, nil
	}

	if !validateDatabaseNameRe.MatchString(db) ||
		strings.HasPrefix(db, reservedPrefix) {
		return false, ErrInvalidDatabaseName
	}

	sql := fmt.Sprintf("CREATE SCHEMA \"%s\"", db)
	_, err = hdb.ExecContext(ctx, sql)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	return false, nil
}

func (hdb *Pool) databaseExists(ctx context.Context, db string) (bool, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = '%s'", db)

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

// DropDatabase drops FerretDB database.
//
// It returns ErrSchemaNotExist if schema does not exist.
func (hdb *Pool) DropDatabase(ctx context.Context, db string) error {

	dbExists, err := hdb.databaseExists(ctx, db)
	if err != nil {
		return err
	}

	if !dbExists {
		return ErrSchemaNotExist
	}

	sql := fmt.Sprintf("DROP SCHEMA \"%s\" CASCADE", db)

	_, err = hdb.ExecContext(ctx, sql)

	return err
}

// // DatabaseSize returns the size of the current database in bytes.
// func DatabaseSize(ctx context.Context, tx pgx.Tx) (int64, error) {
// 	var size int64

// 	err := tx.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&size)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return size, nil
// }

// TablesSize returns the sum of sizes of all tables in the given database in bytes.
func (hdb *Pool) TablesSize(ctx context.Context, db string) (int64, error) {
	var sizeOnDisk int64

	collections, err := hdb.Collections(ctx, db)
	if err != nil {
		return 0, err
	}

	for _, name := range collections {
		var tableSize any
		err = hdb.QueryRowContext(ctx, "SELECT TABLE_SIZE FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_NAME = $2 AND TABLE_TYPE = 'COLLECTION';", db, name).Scan(&tableSize)
		if err != nil {
			return 0, err
		}
		switch tableSize := tableSize.(type) {
		case int64: // collection is in memory and a size can be calculated
			sizeOnDisk += tableSize
		case nil: // collection is not in memory and no size can be calculated
			continue
		default:
			return 0, lazyerrors.Errorf("Got wrong type for tableSize. Got: %T", tableSize)
		}
	}

	return sizeOnDisk, nil
}
