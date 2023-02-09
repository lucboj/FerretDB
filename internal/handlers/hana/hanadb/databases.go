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

// // Databases returns a sorted list of FerretDB databases / PostgreSQL schemas.
// func Databases(ctx context.Context, tx pgx.Tx) ([]string, error) {
// 	sql := "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
// 	rows, err := tx.Query(ctx, sql)
// 	if err != nil {
// 		return nil, lazyerrors.Error(err)
// 	}
// 	defer rows.Close()

// 	res := make([]string, 0, 2)
// 	for rows.Next() {
// 		var name string
// 		if err = rows.Scan(&name); err != nil {
// 			return nil, lazyerrors.Error(err)
// 		}

// 		if strings.HasPrefix(name, "pg_") || name == "information_schema" {
// 			continue
// 		}

// 		res = append(res, name)
// 	}
// 	if err = rows.Err(); err != nil {
// 		return nil, lazyerrors.Error(err)
// 	}

// 	return res, nil
// }

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

// // TablesSize returns the sum of sizes of all tables in the given database in bytes.
// func (pgPool *Pool) TablesSize(ctx context.Context, tx pgx.Tx, db string) (int64, error) {
// 	tables, err := tablesFiltered(ctx, tx, db)
// 	if err != nil {
// 		return 0, err
// 	}

// 	// iterate over result to collect sizes
// 	var sizeOnDisk int64

// 	for _, name := range tables {
// 		var tableSize int64
// 		fullName := pgx.Identifier{db, name}.Sanitize()
// 		// If the table was deleted after we got the list of tables, pg_total_relation_size will return null.
// 		// We use COALESCE to scan this null value as 0 in this case.
// 		// Even though we run the query in a transaction, the current isolation level doesn't guarantee
// 		// that the table is not deleted (see https://www.postgresql.org/docs/14/transaction-iso.html).
// 		// PgPool (not a transaction) is used on purpose here. In this case, transaction doesn't lock
// 		// relations, and it's possible that the table/schema is deleted between the moment we get the list of tables
// 		// and the moment we get the size of the table. In this case, we might receive an error from the database,
// 		// and transaction will be interrupted. Such errors are not critical, we can just ignore them, and
// 		// we don't need to interrupt the whole transaction.
// 		err = pgPool.QueryRow(ctx, "SELECT COALESCE(pg_total_relation_size($1), 0)", fullName).Scan(&tableSize)
// 		if err == nil {
// 			sizeOnDisk += tableSize
// 			continue
// 		}

// 		var pgErr *pgconn.PgError
// 		if errors.As(err, &pgErr) {
// 			switch pgErr.Code {
// 			case pgerrcode.UndefinedTable, pgerrcode.InvalidSchemaName:
// 				// Table or schema was deleted after we got the list of tables, just ignore it
// 				continue
// 			}
// 		}

// 		return 0, err
// 	}

// 	return sizeOnDisk, nil
// }
