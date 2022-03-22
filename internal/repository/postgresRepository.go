package repository

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

type PostgresConnection struct {
	Conn *pgxpool.Pool
}

func GetPostgresConnection(postgresURL string) *PostgresConnection {
	conn, err := pgxpool.Connect(context.Background(), postgresURL)
	log.Infof("DB URL: %s", postgresURL)
	if err != nil {
		log.Fatalf("Error connection to DB %v", err)
	}
	log.Println("successfully connected")

	return &PostgresConnection{Conn: conn}
}
