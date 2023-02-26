package hanadb

import (
	_ "SAP/go-hdb/driver"
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"
)

type Pool struct {
	*sql.DB
}

func NewPool(ctx context.Context, url string, logger *zap.Logger) (*Pool, error) {
	pool, err := sql.Open("hdb", url)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to HANA instance with given connection string")
	}

	// Check connection
	err = pool.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection to HANA instance was not established")
	}

	res := &Pool{
		DB: pool,
	}

	return res, nil
}
