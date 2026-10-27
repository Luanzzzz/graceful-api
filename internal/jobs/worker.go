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

	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()

	// cria um novo contexto para o heartbeat
	// que é cancelado quando o job é cancelado
	heartbeatCtx, cancelHeartbeat := context.WithCancel(jobCtx)
	defer cancelHeartbeat()

	// cria um canal para o erro do heartbeat
	// que é usado para enviar o erro do heartbeat
	heartbeatErr := make(chan error, 1)
	heartbeatDone := make(chan struct{})

	// heartbeat goroutine
	// manda o heartbeat para o job a cada 10 segundos
	// se o job for cancelado, para a goroutine
	// se a goroutine for cancelada, para o heartbeat
	go func() {
		defer close(heartbeatDone)

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return

			//chamar o repo.RenewLease
			//Se RenewLease() retornar nil, registre que o lease foi renovado.
			//Se retornar um erro, registre o erro e encerre a goroutine com return.
			//Mantenha defer close(heartbeatDone) e defer ticker.Stop().
			case <-ticker.C:
				if err := repo.RenewLease(heartbeatCtx, job.ID, job.Attempts); err !=
					nil {
					log.Printf(
						"Worker %d erro ao renovar lease do job %d: %v",
						workerID,
						job.ID,
						err,
					)
					//enviar o erro pelo canal heartbeatErr
					//chamar cancelJob para interromper a goroutine
					heartbeatErr <- err
					cancelJob()

					return
				}
				log.Printf(
					"Worker %d renovou lease do job %d",
					workerID,
					job.ID,
				)

			}
		}
	}()

	select {
	case <-time.After(defaultWorkDuration):
		if err := repo.Complete(ctx, job.ID, job.Attempts); err != nil {
			log.Printf("Worker %d erro ao concluir job %d: %v", workerID, job.ID, err)
			return
		}
		log.Printf("Worker %d concluiu job %d (tarefa %d)", workerID, job.ID, label)

	case <-jobCtx.Done():
		// Contexto novo: jobCtx pode ter sido cancelado pelo worker ou pelo heartbeat,
		// e nesse caso o contexto original não serve para o UPDATE.
		failCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		select {
		case err := <-heartbeatErr:
			//Registar o erro e encerrar o processamento
			log.Printf("Worker %d erro do heartbeat: %v", workerID, err)
			return
		default:
			//Não existe erro pendente no canal.
		}

		if err := repo.Fail(failCtx, job.ID, jobCtx.Err().Error(), job.Attempts); err != nil {
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
