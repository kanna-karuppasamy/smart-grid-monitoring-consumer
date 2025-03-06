package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/config"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/influxdb"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/kafka"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/processor"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize InfluxDB client
	influxClient, err := influxdb.NewClient(cfg.InfluxDB)
	if err != nil {
		log.Fatalf("Failed to create InfluxDB client: %v", err)
	}
	// Don't use defer for closing here, we'll explicitly close after consumers are stopped

	// Initialize processor
	proc := processor.NewProcessor(influxClient, cfg.Processor)

	// Create context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())

	// Handle termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize consumers
	var wg sync.WaitGroup
	consumers := make([]*kafka.Consumer, cfg.Kafka.ConsumerCount)

	log.Printf("Starting %d Kafka consumers...", cfg.Kafka.ConsumerCount)

	// Start consumers
	for i := 0; i < cfg.Kafka.ConsumerCount; i++ {
		consumer, err := kafka.NewConsumer(
			fmt.Sprintf("consumer-%d", i),
			cfg.Kafka,
			proc.ProcessMessages,
		)
		if err != nil {
			log.Fatalf("Failed to create consumer %d: %v", i, err)
		}

		consumers[i] = consumer

		wg.Add(1)
		go func(c *kafka.Consumer, id int) {
			defer wg.Done()
			log.Printf("Starting consumer %d", id)
			if err := c.Consume(ctx); err != nil {
				log.Printf("Consumer %d error: %v", id, err)
			}
			log.Printf("Consumer %d stopped", id)
		}(consumer, i)
	}

	// Wait for termination signal
	<-sigChan
	log.Println("Received termination signal. Shutting down...")

	// Cancel context to stop consumers
	cancel()

	// Set a deadline for clean shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Create a channel to signal when all consumers are done
	done := make(chan struct{})

	go func() {
		// Wait for all consumers to finish
		wg.Wait()
		close(done)
	}()

	// Wait for either all consumers to stop or the timeout
	select {
	case <-done:
		log.Println("All consumers stopped successfully")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timed out, forcing exit")
	}

	// Now it's safe to close the InfluxDB client
	log.Println("Closing InfluxDB client...")
	influxClient.Close()

	log.Println("Shutdown complete.")
}
