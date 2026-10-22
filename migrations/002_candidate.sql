-- Reserva manual de um job, a mesma regra de repository.go (claimSQL).
-- Duas sessões abertas ao mesmo tempo pegam ids diferentes:
-- FOR UPDATE segura a linha até o COMMIT e SKIP LOCKED faz a outra pular essa linha.
BEGIN;

WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE attempts < max_attempts
      AND (
          -- Pronto para executar, inclusive retry cuja espera já acabou.
          (status = 'pending' AND available_at <= NOW())
          OR
          -- Reserva antiga abandonada: o lease venceu e ninguém concluiu.
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
RETURNING j.id, j.payload, j.status,
          j.attempts, j.lease_until;

COMMIT;
