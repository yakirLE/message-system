package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yakirLE/message-system/message-sender/internal/app"
	"github.com/yakirLE/message-system/message-sender/internal/config"
	"go.uber.org/zap"
)

func main() {
	cfg := config.New()
	app := app.New(cfg)

	//starting app
	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := app.Start(startCtx); err != nil {
		log.Fatalf("failed to start application %v", zap.Error(err))
	}
	cancel()

	//graceful shutdown
	sigc := make(chan os.Signal, 1)
	defer close(sigc)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	sig := <-sigc
	log.Println("shutting down due to signal", zap.Stringer("signal", sig))

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Stop(stopCtx); err != nil {
		log.Fatalf("failed to stop app %v", zap.Error(err))
	}
}
