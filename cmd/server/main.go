package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/theHerta27/ModelGate/internal/api"
	"github.com/theHerta27/ModelGate/internal/config"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/service"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	chatProvider, err := provider.NewFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("create provider: %w", err)
	}
	chatService := service.NewChatService(chatProvider)
	handler := api.NewHandler(chatService)
	router := api.NewRouter(handler)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownSignal, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("ModelGate listening on %s with provider %s", cfg.HTTPAddr, cfg.Provider)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-shutdownSignal.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	return nil
}
