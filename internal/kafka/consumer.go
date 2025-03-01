package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/config"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/models"

	"github.com/Shopify/sarama"
)

// MessageProcessor is a function that processes batches of transactions
type MessageProcessor func([]models.Transaction) error

// Consumer represents a Kafka consumer
type Consumer struct {
	id         string
	config     config.KafkaConfig
	consumer   sarama.ConsumerGroup
	processor  MessageProcessor
	msgBuffer  []models.Transaction
	bufferLock sync.Mutex
	lastFlush  time.Time
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(id string, config config.KafkaConfig, processor MessageProcessor) (*Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Optimize for throughput
	saramaConfig.Consumer.Fetch.Min = 1
	saramaConfig.Consumer.Fetch.Default = 1024 * 1024 // 1MB
	saramaConfig.Consumer.MaxWaitTime = 250 * time.Millisecond

	client, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		id:        id,
		config:    config,
		consumer:  client,
		processor: processor,
		msgBuffer: make([]models.Transaction, 0, config.BatchSize),
		lastFlush: time.Now(),
	}, nil
}

// Consume starts consuming messages from Kafka
func (c *Consumer) Consume(ctx context.Context) error {
	// Setup error handling
	errorChan := make(chan error)
	go func() {
		for err := range c.consumer.Errors() {
			log.Printf("Consumer %s error: %v", c.id, err)
			errorChan <- err
		}
	}()

	// Setup message handling
	handler := &consumerGroupHandler{
		consumer: c,
		ctx:      ctx,
	}

	// Setup periodic flushing
	flushTicker := time.NewTicker(c.config.BatchTimeout)
	defer flushTicker.Stop()

	go func() {
		for {
			select {
			case <-flushTicker.C:
				c.flushBuffer()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Consume
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errorChan:
			return err
		default:
			if err := c.consumer.Consume(ctx, []string{c.config.Topic}, handler); err != nil {
				if err != context.Canceled {
					return err
				}
				return nil
			}
		}
	}
}

// addMessage adds a message to the buffer and flushes if needed
func (c *Consumer) addMessage(transaction models.Transaction) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()

	c.msgBuffer = append(c.msgBuffer, transaction)

	// Flush if buffer is full
	if len(c.msgBuffer) >= c.config.BatchSize {
		c.flushBufferLocked()
	}
}

// flushBuffer flushes the message buffer
func (c *Consumer) flushBuffer() {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()

	c.flushBufferLocked()
}

// flushBufferLocked flushes the message buffer while holding the lock
func (c *Consumer) flushBufferLocked() {
	if len(c.msgBuffer) == 0 {
		return
	}

	// Create a copy of the buffer
	messages := make([]models.Transaction, len(c.msgBuffer))
	copy(messages, c.msgBuffer)

	// Clear the buffer
	c.msgBuffer = c.msgBuffer[:0]
	c.lastFlush = time.Now()

	// Process messages in a separate goroutine
	go func(msgs []models.Transaction) {
		if err := c.processor(msgs); err != nil {
			log.Printf("Error processing messages: %v", err)
		}
	}(messages)
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	consumer *Consumer
	ctx      context.Context
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if h.ctx.Err() != nil {
			return h.ctx.Err()
		}

		var transaction models.Transaction
		if err := json.Unmarshal(message.Value, &transaction); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		h.consumer.addMessage(transaction)
		session.MarkMessage(message, "")
	}
	return nil
}
