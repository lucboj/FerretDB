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

package hanadb

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/FerretDB/FerretDB/internal/util/ctxutil"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// transactionConflictError is returned when one of the queries in the transaction returned an error because
// of an unexpected conflict. The caller could retry such a transaction.
type transactionConflictError struct {
	err error // underlying error
}

// newTransactionConflictError creates a new *transactionConflictError with the given underlying error.
func newTransactionConflictError(err error) error {
	return &transactionConflictError{err: err}
}

// Error implements the error interface.
func (e *transactionConflictError) Error() string {
	return "transactionConflictError: " + e.err.Error()
}

// InTransaction wraps the given function f in a transaction.
// If f returns an error, the transaction is rolled back.
// Errors are wrapped with lazyerrors.Error,
// so the caller needs to use errors.Is to check the error,
// for example, errors.Is(err, ErrSchemaNotExist).
func (hdb *Pool) InTransaction(ctx context.Context, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	if tx, err = hdb.DB.BeginTx(ctx, nil); err != nil {
		err = lazyerrors.Error(err)
		return
	}

	var committed bool

	defer func() {
		// It is not enough to check `err == nil` there,
		// because in tests `f` could contain testify/require.XXX or `testing.TB.FailNow()` calls
		// that call `runtime.Goexit()`, leaving `err` unset in `err = f(tx)` below.
		// This situation would hang a test.
		//
		// As a bonus, checking a separate variable also handles any panics in `f`.
		if committed {
			return
		}

		if rerr := tx.Rollback(); rerr != nil {
			hdb.l.Error("failed to perform rollback", zap.String("error", rerr.Error()))

			// in case of `runtime.Goexit()` or `panic(nil)`; see above
			if err == nil {
				err = rerr
			}
		}
	}()

	if err = f(tx); err != nil {
		err = lazyerrors.Error(err)
		return
	}

	if err = tx.Commit(); err != nil {
		err = lazyerrors.Error(err)
		return
	}

	committed = true
	return
}

// InTransactionRetry wraps the given function f in a transaction.
// If f returns (possibly wrapped) *transactionConflictError, the transaction is retried multiple times with delays.
// If the transaction still fails after that, the last error is returned.
func (hdb *Pool) InTransactionRetry(ctx context.Context, f func(*sql.Tx) error) error {
	// TODO use exponential backoff with jitter instead
	// https://github.com/FerretDB/FerretDB/issues/1720
	const (
		attemptsMax   = 30
		retryDelayMin = 100 * time.Millisecond
		retryDelayMax = 200 * time.Millisecond
	)

	var attempts int

	for {
		err := hdb.InTransaction(ctx, f)
		var tcErr *transactionConflictError

		switch {
		case err == nil:
			return nil

		case errors.As(err, &tcErr):
			attempts++
			if attempts >= attemptsMax {
				return lazyerrors.Errorf("giving up after %d attempts: %w", attempts, err)
			}

			deltaMS := rand.Int63n((retryDelayMax - retryDelayMin).Milliseconds())
			delay := retryDelayMin + time.Duration(deltaMS)*time.Millisecond

			hdb.l.Warn("attempt failed, retrying", zap.Error(err), zap.Int("attempt", attempts), zap.Duration("delay", delay))

			ctxutil.Sleep(ctx, delay)

		default:
			return lazyerrors.Errorf("non-retriable error: %w", err)
		}
	}
}
