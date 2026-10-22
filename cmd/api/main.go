// Ponto de entrada da API.
// Sobe o Postgres, dois workers que reservam jobs no banco e o HTTP.
// No Ctrl+C (ou SIGTERM) para de aceitar requisições e espera os workers saírem.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"graceful-api/internal/config"
	"graceful-api/internal/db"
	"graceful-api/internal/jobs"
	"graceful-api/internal/server"
)

func main() {
	// Lê .env só para variáveis que ainda não existem no ambiente.
	config.LoadDotEnv(".env")

	// signalCtx é cancelado quando o processo recebe interrupção ou SIGTERM.
	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	// Pool de conexões. Sem ping bem-sucedido o processo nem sobe.
	pool, err := db.Connect(context.Background())
	if err != nil {
		log.Fatalf("Postgres: %v", err)
	}
	defer pool.Close()
	log.Println("Postgres conectado")

	repo := jobs.NewRepository(pool)

	// workerCtx é independente do sinal: o cancelamento dos workers
	// acontece depois, no encerramento, para eles devolverem o job atual.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	var wg sync.WaitGroup

	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go jobs.RunWorker(workerCtx, i, repo, &wg)
	}

	// Fecha quando os dois workers terminarem o RunWorker.
	workersDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	httpServer := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: server.New(repo),
	}

	serverErr := make(chan error, 1)

	go func() {
		log.Println("Servidor HTTP iniciado na porta 8080")

		err := httpServer.ListenAndServe()

		// ErrServerClosed é o resultado normal de Shutdown, não uma falha.
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}

		serverErr <- err
	}()

	// Espera um sinal de parada ou o HTTP cair sozinho.
	select {
	case <-signalCtx.Done():
		log.Println("Sinal de encerramento recebido")

	case err := <-serverErr:
		log.Printf("Servidor HTTP parou: %v", err)
	}

	// Pede para os workers pararem de buscar jobs novos.
	cancelWorkers()

	// Prazo total para o HTTP terminar as requisições em andamento
	// e para os workers saírem do loop.
	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancelShutdown()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown HTTP não concluiu: %v", err)

		if err := httpServer.Close(); err != nil {
			log.Printf("Erro no fechamento HTTP: %v", err)
		}
	}

	select {
	case <-workersDone:
		log.Println("Todos os workers foram encerrados")

	case <-shutdownCtx.Done():
		log.Println("Tempo esgotado aguardando workers")
	}
}
