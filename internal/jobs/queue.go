package jobs

import (
	"errors"
	"sync"
)

// Fila em memória da primeira versão do projeto.
// O fluxo atual grava e reserva jobs no Postgres (repository.go).
// Esta fila ficou só como referência da implementação anterior.
var (
	ErrClosed = errors.New("fila em encerramento")
	ErrFull   = errors.New("fila cheia")
)

// Queue é um canal com capacidade fixa.
// closed impede Submit depois do encerramento.
type Queue struct {
	mu     sync.Mutex
	jobs   chan int
	closed bool
}

func NewQueue(size int) *Queue {
	return &Queue{
		jobs: make(chan int, size),
	}
}

// Submit enfileira id.
// Se o canal já está cheio, devolve ErrFull sem bloquear.
// Se Close já foi chamado, devolve ErrClosed.
func (q *Queue) Submit(id int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}

	select {
	case q.jobs <- id:
		return nil
	default:
		return ErrFull
	}
}

// Close marca a fila e fecha o canal.
// Quem está lendo o canal recebe os ids que já estavam lá e depois termina.
// O mutex evita fechar o canal duas vezes.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
}

func (q *Queue) Jobs() <-chan int {
	return q.jobs
}
