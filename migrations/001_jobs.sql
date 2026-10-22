-- Tabela da fila persistente.
-- IF NOT EXISTS cria o schema em banco novo.
-- Os ALTER abaixo completam a tabela se ela já existia sem essas colunas.
CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,

    -- Dados da tarefa. O HTTP grava {"id": N}.
    payload JSONB NOT NULL,

    -- pending: esperando worker.
    -- processing: reservado, com lease.
    -- completed: terminou.
    -- failed: esgotou max_attempts.
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'processing',
            'completed', 'failed'
        )),

    -- Quantas vezes um worker já reservou este job.
    attempts INTEGER NOT NULL DEFAULT 0,

    -- Até quando a reserva atual vale. NULL quando ninguém está com o job.
    -- Worker que sumir pode ter o job retomado depois deste instante.
    lease_until TIMESTAMPTZ,

    -- Evita inserir a mesma requisição duas vezes. NULL é permitido mais de uma vez.
    idempotency_key TEXT UNIQUE,

    -- Só entra na fila de novo quando este instante já passou (retry com espera).
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Limite de reservas. Claim exige attempts < max_attempts.
    max_attempts INTEGER NOT NULL DEFAULT 3,

    -- Texto do último erro, quando Fail é chamado.
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS available_at
    TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS max_attempts
    INTEGER NOT NULL DEFAULT 3;

ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS last_error TEXT;
