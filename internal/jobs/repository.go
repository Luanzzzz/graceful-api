package jobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNoJob indica que Claim não encontrou linha elegível.
// O worker trata isso como "esperar e tentar de novo", não como falha.
var ErrNoJob = errors.New("nenhum job disponível")
var ErrLeaseLost = errors.New("reserva do job perdida ou expirada")

// Repository executa o SQL da fila persistente.
type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Enqueue insere um job pending.
// idempotencyKey nil grava NULL. Valor repetido fere o UNIQUE da coluna.
func (r *Repository) Enqueue(
	ctx context.Context,
	payload []byte,
	idempotencyKey *string,
) (int64, error) {
	const sql = `
		INSERT INTO jobs (payload, idempotency_key)
		VALUES ($1, $2)
		RETURNING id
	`

	var id int64
	err := r.pool.QueryRow(ctx, sql, payload, idempotencyKey).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("inserir job: %w", err)
	}

	return id, nil
}

// claimSQL reserva um único job e já faz COMMIT lógico da mudança
// dentro da transação de Claim.
//
// Elegível:
//   - pending com available_at no passado (pronto para rodar, inclusive retry);
//   - processing cujo lease_until já venceu (worker anterior sumiu).
//
// FOR UPDATE trava a linha até o COMMIT.
// SKIP LOCKED faz o outro worker pular a linha travada e pegar outro job.
// LIMIT 1 + ORDER BY available_at, id pega o mais antigo.
//
// A reserva marca processing, soma 1 em attempts e renova o lease por 30s.
const claimSQL = `
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE attempts < max_attempts
      AND (
          (status = 'pending' AND available_at <= NOW())
          OR
          (status = 'processing' AND lease_until <= NOW())
      )
    ORDER BY available_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs AS j
SET
    status = 'processing',
    attempts = j.attempts + 1,
    lease_until = NOW() + INTERVAL '30 seconds',
    updated_at = NOW()
FROM candidate
WHERE j.id = candidate.id
RETURNING
    j.id,
    j.payload,
    j.status,
    j.attempts,
    j.lease_until,
    j.idempotency_key,
    j.available_at,
    j.max_attempts,
    j.last_error,
    j.created_at,
    j.updated_at
`

// Claim abre uma transação, reserva no máximo um job e confirma.
// Sem candidato, devolve ErrNoJob. O lock da linha acaba no COMMIT.
func (r *Repository) Claim(ctx context.Context) (*Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciar transação: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, claimSQL)
	if err != nil {
		return nil, fmt.Errorf("reservar job: %w", err)
	}

	job, err := scanJob(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if job == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit vazio: %w", err)
		}
		return nil, ErrNoJob
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	return job, nil
}

// Complete encerra o job com sucesso e solta o lease.

func (r *Repository) Complete(
	ctx context.Context,
	jobID int64,
	attempt int,
) error {
	const sql = `
        UPDATE jobs
        SET
            status = 'completed',
            lease_until = NULL,
            updated_at = NOW()
        WHERE id = $1
          AND status = 'processing'
          AND attempts = $2
          AND lease_until > NOW()
    `

	tag, err := r.pool.Exec(ctx, sql, jobID, attempt)

	if err != nil {
		return fmt.Errorf("concluir job %d: %w", jobID, err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf(
			"job %d, tentativa %d: %w",
			jobID,
			attempt,
			ErrLeaseLost,
		)
	}

	return nil
}

// Fail registra o erro.
// Se ainda há tentativas, volta para pending e empurra available_at
// com espera exponencial (2^attempts segundos, no máximo 2^6).
// Se attempts já chegou em max_attempts, o status fica failed.
func (r *Repository) Fail(ctx context.Context, jobID int64, lastError string, attempt int) error {
	const sql = `
		UPDATE jobs AS j
		SET
		    status = CASE
		        WHEN j.attempts >= j.max_attempts THEN 'failed'
		        ELSE 'pending'
		    END,
		    last_error = $3,
		    lease_until = NULL,
		    available_at = CASE
		        WHEN j.attempts >= j.max_attempts THEN j.available_at
		        ELSE NOW() + (POWER(2, LEAST(j.attempts, 6)) * INTERVAL '1 second')
		    END,
		    updated_at = NOW()
		WHERE j.id = $1
		  AND j.status = 'processing'
		  AND j.attempts = $2
		  AND j.lease_until > NOW()
	`

	tag, err := r.pool.Exec(ctx, sql, jobID, attempt, lastError)
	if err != nil {
		return fmt.Errorf("falhar job: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf(
			"job %d, tentativa %d: %w",
			jobID,
			attempt,
			ErrLeaseLost,
		)
	}

	return nil
}

// scanJob lê a única linha do RETURNING.
// nil, nil significa que o UPDATE não afetou ninguém.
func scanJob(rows pgx.Rows) (*Job, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	var job Job
	var status string

	err := rows.Scan(
		&job.ID,
		&job.Payload,
		&status,
		&job.Attempts,
		&job.LeaseUntil,
		&job.IdempotencyKey,
		&job.AvailableAt,
		&job.MaxAttempts,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("ler job: %w", err)
	}

	job.Status = Status(status)
	return &job, nil
}
