// Pacote db abre o pool de conexões com o Postgres.
package db

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect usa DATABASE_URL (postgres://usuário:senha@host:porta/banco).
// O pool é compartilhado pelo HTTP e pelos workers.
// Ping confirma que o banco aceita conexão antes de o servidor subir.
func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL não definida")
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("abrir pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping no postgres: %w", err)
	}

	return pool, nil
}
