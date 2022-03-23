package repository

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

type PostgresConnection struct {
	Conn *pgxpool.Pool
}

func GetPostgresConnection(ctx context.Context, postgresURL string) (*PostgresConnection, error) {
	conn, err := pgxpool.Connect(ctx, postgresURL)
	log.Infof("DB URL: %s", postgresURL)
	if err != nil {
		return nil, fmt.Errorf("error connection to DB %v", err)
	}
	log.Println("successfully connected")

	return &PostgresConnection{Conn: conn}, nil
}
