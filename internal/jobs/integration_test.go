package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testDatabaseURL = "postgres://graceful:graceful@127.0.0.1:5433/graceful_test?sslmode=disable"

// setupTestRepository abre o pool do banco de testes.
// Recusa qualquer banco que não seja graceful_test.
func setupTestRepository(t *testing.T) (*Repository, *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDatabaseURL)
	if err != nil {
		t.Fatalf("abrir pool de testes: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("conectar ao PostgreSQL de testes: %v", err)
	}

	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		t.Fatalf("identificar banco de testes: %v", err)
	}
	if database != "graceful_test" {
		t.Fatalf("banco %q não permitido: testes exigem graceful_test", database)
	}

	return NewRepository(pool), pool
}

// insertProcessingJob cria um job já reservado na tentativa informada.
func insertProcessingJob(t *testing.T, pool *pgxpool.Pool, attempt int) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (payload, status, attempts, max_attempts, lease_until)
		VALUES ('{"id": 901}'::jsonb, 'processing', $1, 3, NOW() + INTERVAL '5 minutes')
		RETURNING id
	`, attempt).Scan(&id)
	if err != nil {
		t.Fatalf("criar fixture processing: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM jobs WHERE id = $1", id); err != nil {
			t.Errorf("remover job de teste %d: %v", id, err)
		}
	})

	return id
}

func TestCompleteFencing(t *testing.T) {
	repo, pool := setupTestRepository(t)
	id := insertProcessingJob(t, pool, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A tentativa 1 pertence a um worker antigo. Ela não pode concluir a tentativa 2.
	err := repo.Complete(ctx, id, 1)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("tentativa antiga: esperado ErrLeaseLost; recebido %v", err)
	}

	var status string
	var attempts int
	if err := pool.QueryRow(ctx,
		"SELECT status, attempts FROM jobs WHERE id = $1", id,
	).Scan(&status, &attempts); err != nil {
		t.Fatalf("consultar job após tentativa antiga: %v", err)
	}
	if status != "processing" || attempts != 2 {
		t.Fatalf("tentativa antiga alterou job: status=%q attempts=%d", status, attempts)
	}

	if err := repo.Complete(ctx, id, 2); err != nil {
		t.Fatalf("tentativa atual deveria concluir: %v", err)
	}

	var leaseCleared bool
	if err := pool.QueryRow(ctx,
		"SELECT status, lease_until IS NULL FROM jobs WHERE id = $1", id,
	).Scan(&status, &leaseCleared); err != nil {
		t.Fatalf("consultar job concluído: %v", err)
	}
	if status != "completed" || !leaseCleared {
		t.Fatalf("resultado inválido: status=%q lease_limpo=%v", status, leaseCleared)
	}

	// Uma segunda conclusão da mesma tentativa não pode parecer sucesso.
	if err := repo.Complete(ctx, id, 2); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("conclusão duplicada: esperado ErrLeaseLost; recebido %v", err)
	}
}

func TestFailFencing(t *testing.T) {
	repo, pool := setupTestRepository(t)
	id := insertProcessingJob(t, pool, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A tentativa 1 não pode gravar falha em cima da tentativa 2 ativa.
	err := repo.Fail(ctx, id, "erro de worker antigo", 1)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("falha de tentativa antiga: esperado ErrLeaseLost; recebido %v", err)
	}

	var status string
	var errorIsNull bool
	if err := pool.QueryRow(ctx,
		"SELECT status, last_error IS NULL FROM jobs WHERE id = $1", id,
	).Scan(&status, &errorIsNull); err != nil {
		t.Fatalf("consultar job após falha antiga: %v", err)
	}
	if status != "processing" || !errorIsNull {
		t.Fatalf("tentativa antiga alterou job: status=%q erro_nulo=%v", status, errorIsNull)
	}

	// A tentativa 2 devolve o job para a fila com espera de retry.
	if err := repo.Fail(ctx, id, "falha temporária", 2); err != nil {
		t.Fatalf("tentativa atual deveria registrar falha: %v", err)
	}

	var lastError string
	var leaseCleared, retryScheduled bool
	if err := pool.QueryRow(ctx, `
		SELECT status, last_error, lease_until IS NULL, available_at > NOW()
		FROM jobs WHERE id = $1
	`, id).Scan(&status, &lastError, &leaseCleared, &retryScheduled); err != nil {
		t.Fatalf("consultar job reagendado: %v", err)
	}
	if status != "pending" || lastError != "falha temporária" || !leaseCleared || !retryScheduled {
		t.Fatalf("retry incorreto: status=%q erro=%q lease_limpo=%v retry_futuro=%v",
			status, lastError, leaseCleared, retryScheduled)
	}
}

// TestRenewLeaseSuccess testa se o lease do job é renovado com sucesso.
// O job deve estar na tentativa 2 e ter um lease válido.
// O lease deve ser renovado com sucesso.
// O lease deve ser maior que o original.
// O job deve estar na tentativa 2 e ter um lease válido.
// O lease deve ser renovado com sucesso.
// O lease deve ser maior que o original.
// O job deve estar na tentativa 2 e ter um lease válido.
// O lease deve ser renovado com sucesso.
// O lease deve ser maior que o original.
// O job deve estar na tentativa 2 e ter um lease válido.
func TestRenewLeaseSuccess(t *testing.T) {
	repo, pool := setupTestRepository(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Criar um job na tentativa 2 e com um lease válido.
	var id int64
	var originalLease time.Time
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (
		    payload,
		    status,
		    attempts,
		    lease_until
		)
		VALUES (
		    '{"document_id": 999}',
		    'processing',
		    2,
		    NOW() + INTERVAL '5 seconds'
		)
		RETURNING id, lease_until
	`).Scan(&id, &originalLease)
	if err != nil {
		t.Fatalf("criar job processing: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM jobs WHERE id = $1", id); err != nil {
			t.Errorf("remover job de teste %d: %v", id, err)
		}
	})

	// Renovar o lease do job.
	if err := repo.RenewLease(ctx, id, 2); err != nil {
		t.Fatalf("renovar lease: %v", err)
	}

	// Verificar se o lease foi renovado.
	var leaseUntil time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_until FROM jobs WHERE id = $1", id).Scan(&leaseUntil); err != nil {
		t.Fatalf("consultar lease_until: %v", err)
	}
	if !leaseUntil.After(originalLease) {
		t.Fatalf("lease_until não aumentou: original=%v atual=%v", originalLease, leaseUntil)
	}
}
