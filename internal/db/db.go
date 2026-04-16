package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sekolahpintar/statistik-engine/internal/config"
)

func New(cfg *config.Config) (*sqlx.DB, error) {
	database, err := sqlx.Open("postgres", cfg.DB.DSN())
	if err != nil {
		return nil, err
	}

	database.SetMaxOpenConns(30)
	database.SetMaxIdleConns(10)
	database.SetConnMaxLifetime(5 * time.Minute)
	database.SetConnMaxIdleTime(2 * time.Minute)

	if err := database.Ping(); err != nil {
		return nil, err
	}

	return database, nil
}
