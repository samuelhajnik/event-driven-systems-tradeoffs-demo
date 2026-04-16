package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	apihandler "event-driven-systems-tradeoffs-demo/internal/api"
	"event-driven-systems-tradeoffs-demo/internal/config"
	"event-driven-systems-tradeoffs-demo/internal/ingest"
	"event-driven-systems-tradeoffs-demo/internal/stats"
)

func main() {
	cfg := config.Load()
	counter := &stats.Counters{}
	producer := ingest.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()

	handler := apihandler.NewHandler(producer, counter)
	server := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("api listening on :%s topic=%s brokers=%v", cfg.HTTPPort, cfg.KafkaTopic, cfg.KafkaBrokers)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}
