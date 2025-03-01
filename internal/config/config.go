package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration
type Config struct {
	Kafka     KafkaConfig
	InfluxDB  InfluxDBConfig
	Processor ProcessorConfig
}

// KafkaConfig holds Kafka-related configuration
type KafkaConfig struct {
	Brokers       []string
	Topic         string
	GroupID       string
	ConsumerCount int
	BatchSize     int
	BatchTimeout  time.Duration
}

// InfluxDBConfig holds InfluxDB-related configuration
type InfluxDBConfig struct {
	URL          string
	Org          string
	Token        string
	Bucket       string
	BatchSize    int
	BatchTimeout time.Duration
}

// ProcessorConfig holds processor-related configuration
type ProcessorConfig struct {
	WorkerCount        int
	QueueSize          int
	EnableAggregations bool
}

// Load loads configuration from environment variables with sensible defaults
func Load() (*Config, error) {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:       getEnvStringSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:         getEnv("KAFKA_TOPIC", "smart-grid-readings"),
			GroupID:       getEnv("KAFKA_GROUP_ID", "smart-grid-monitoring-consumer"),
			ConsumerCount: getEnvInt("KAFKA_CONSUMER_COUNT", 12), // Match partition count
			BatchSize:     getEnvInt("KAFKA_BATCH_SIZE", 10000),
			BatchTimeout:  getEnvDuration("KAFKA_BATCH_TIMEOUT", 1*time.Second),
		},
		InfluxDB: InfluxDBConfig{
			URL:          getEnv("INFLUXDB_URL", "http://localhost:8086"),
			Org:          getEnv("INFLUXDB_ORG", "Solo"),
			Token:        getEnv("INFLUX_TOKEN", "BO4GWv3eHzqpbfQzmRbuPeWcU-3sytzrYNzIgckAiV8XD8i22ndOQlI7IjRt2z2asH25jtgvil4M7E7alPvs6Q=="),
			Bucket:       getEnv("INFLUXDB_BUCKET", "smart-grid-monitor"),
			BatchSize:    getEnvInt("INFLUXDB_BATCH_SIZE", 5000),
			BatchTimeout: getEnvDuration("INFLUXDB_BATCH_TIMEOUT", 500*time.Millisecond),
		},
		Processor: ProcessorConfig{
			WorkerCount:        getEnvInt("PROCESSOR_WORKER_COUNT", 4),
			QueueSize:          getEnvInt("PROCESSOR_QUEUE_SIZE", 1000000),
			EnableAggregations: getEnvBool("PROCESSOR_ENABLE_AGGREGATIONS", true),
		},
	}, nil
}

// Helper functions to get environment variables with defaults
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvStringSlice(key string, defaultValue []string) []string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.Split(value, ",")
	}
	return defaultValue
}
