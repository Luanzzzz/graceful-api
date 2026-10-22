// Pacote server expõe o HTTP que enfileira tarefas na tabela jobs.
package server

import (
	"fmt"
	"net/http"
	"strconv"

	"graceful-api/internal/jobs"
)

// New monta o roteador.
// POST /jobs?id=N grava um job com payload {"id": N}.
// O header Idempotency-Key, se vier, é gravado para impedir duplicata.
func New(repo *jobs.Repository) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}

		id, err := strconv.Atoi(r.URL.Query().Get("id"))
		if err != nil || id <= 0 {
			http.Error(w, "id deve ser inteiro positivo", http.StatusBadRequest)
			return
		}

		payload, err := jobs.PayloadFromTaskID(id)
		if err != nil {
			http.Error(w, "payload inválido", http.StatusInternalServerError)
			return
		}

		// Ponteiro nulo vira NULL no Postgres; string vazia não é chave.
		var idempotencyKey *string
		if key := r.Header.Get("Idempotency-Key"); key != "" {
			idempotencyKey = &key
		}

		jobID, err := repo.Enqueue(r.Context(), payload, idempotencyKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Tarefa %d enfileirada (job %d)\n", id, jobID)
	})

	return mux
}
