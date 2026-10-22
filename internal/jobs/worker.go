package jobs

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

const (
	// Pausa entre tentativas quando não há job ou a reserva falhou.
	defaultPollInterval = 500 * time.Millisecond
	// Tempo simulado de processamento de cada job.
	defaultWorkDuration = 3 * time.Second
)

// RunWorker fica em loop reservando jobs no Postgres até ctx ser cancelado.
// wg.Done avisa o main de que este worker saiu.
func RunWorker(
	ctx context.Context,
	workerID int,
	repo *Repository,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		if ctx.Err() != nil {
			log.Printf("Worker %d encerrou: %v", workerID, ctx.Err())
			return
		}

		job, err := repo.Claim(ctx)
		if errors.Is(err, ErrNoJob) {
			if !sleepOrDone(ctx, defaultPollInterval) {
				return
			}
			continue
		}
		if err != nil {
			log.Printf("Worker %d erro ao reservar job: %v", workerID, err)
			if !sleepOrDone(ctx, defaultPollInterval) {
				return
			}
			continue
		}

		processRepositoryJob(ctx, workerID, repo, job)
	}
}

// processRepositoryJob espera 3 segundos e marca o job como concluído.
// Se o processo estiver encerrando, marca falha para outro worker poder retomar
// quando o lease vencer (ou na hora, se Fail devolver o job para pending).
func processRepositoryJob(
	ctx context.Context,
	workerID int,
	repo *Repository,
	job *Job,
) {
	taskID, ok := job.TaskID()
	label := taskID
	if !ok {
		label = int(job.ID)
	}

	log.Printf("Worker %d iniciou job %d (tarefa %d)", workerID, job.ID, label)

	select {
	case <-time.After(defaultWorkDuration):
		if err := repo.Complete(ctx, job.ID, job.Attempts); err != nil {
			log.Printf("Worker %d erro ao concluir job %d: %v", workerID, job.ID, err)
			return
		}
		log.Printf("Worker %d concluiu job %d (tarefa %d)", workerID, job.ID, label)

	case <-ctx.Done():
		// Contexto novo: o ctx do worker já foi cancelado e não serviria para o UPDATE.
		failCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := repo.Fail(failCtx, job.ID, ctx.Err().Error(), job.Attempts); err != nil {
			log.Printf("Worker %d erro ao marcar falha do job %d: %v", workerID, job.ID, err)
		}
		log.Printf("Worker %d interrompeu job %d", workerID, job.ID)
	}
}

// MemoryWorker consome a fila em memória de queue.go.
// Não é usado pelo main atual.
func MemoryWorker(
	ctx context.Context,
	id int,
	jobs <-chan int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		log.Printf("Worker %d iniciou tarefa %d", id, job)

		select {
		case <-time.After(defaultWorkDuration):
			log.Printf("Worker %d concluiu tarefa %d", id, job)

		case <-ctx.Done():
			log.Printf("Worker %d interrompeu tarefa %d", id, job)
			return
		}
	}

	log.Printf("Worker %d encerrou normalmente", id)
}

// sleepOrDone espera d ou retorna false se ctx for cancelado antes.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
