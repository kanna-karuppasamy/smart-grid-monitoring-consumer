# Smart Grid Monitoring - Kafka Consumer

This application consumes smart meter transaction data from Kafka and writes it to InfluxDB for time-series analysis and monitoring.

## Features

- High-throughput Kafka consumer optimized for 1M+ transactions per second
- Support for 32 Kafka partitions with parallel processing
- Efficient batched writes to InfluxDB
- Real-time aggregations to support dashboard use cases:
  - Live energy consumption per second
  - Power usage trends (last 24 hours)
  - List of smart meters with faults or offline status
  - Map of high-energy consumption regions
- Configurable via environment variables

## Architecture

The application follows a multi-stage pipeline architecture:
1. **Kafka Consumption**: Consumes messages from Kafka partitions
2. **Processing Queue**: Buffers messages for processing
3. **Processing Workers**: Process messages in parallel
4. **Aggregation Engine**: Computes real-time aggregations
5. **InfluxDB Writer**: Writes data to InfluxDB in optimized batches

## InfluxDB Schema

The application uses the following InfluxDB schema:

- **Measurement: `energy_consumption`**
  - Tags: `meter_id`, `region`, `status`, `building_type`
  - Fields: `consumption_kwh`, `latitude`, `longitude`, `peak_load`
  - Timestamp: From transaction data

- **Measurement: `energy_consumption_aggregate`**
  - Fields: `total_kwh`, `reading_count`, `max_kwh`, `min_kwh`, `avg_kwh`
  - Timestamp: Aggregation bucket timestamp (1-second resolution)

- **Measurement: `meter_status`**
  - Tags: `status`
  - Fields: `count`
  - Timestamp: Aggregation timestamp

- **Measurement: `region_consumption`**
  - Tags: `region`
  - Fields: `total_kwh`, `meter_count`, `average_kwh`, `max_consumption`
  - Timestamp: Aggregation timestamp

## Setup Instructions

### Prerequisites

- Go 1.16 or higher
- Running Kafka cluster with the `smart-grid-transactions` topic (32 partitions)
- Running InfluxDB v1.x instance

### InfluxDB Setup

Before running the consumer, set up InfluxDB:

```bash
# Create database
CREATE DATABASE smart_grid_monitoring

# Create retention policies
CREATE RETENTION POLICY "raw_data" ON "smart_grid_monitoring" DURATION 30d REPLICATION 1 DEFAULT
CREATE RETENTION POLICY "aggregated_data" ON "smart_grid_monitoring" DURATION 365d REPLICATION 1
```

### Configuration

The application is configured via environment variables:

```bash
# Kafka Configuration
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC=smart-grid-transactions
export KAFKA_GROUP_ID=smart-grid-monitoring-consumer
export KAFKA_CONSUMER_COUNT=32
export KAFKA_BATCH_SIZE=10000
export KAFKA_BATCH_TIMEOUT=1s

# InfluxDB Configuration
export INFLUXDB_URL=http://localhost:8086
export INFLUXDB_DATABASE=smart_grid_monitoring
export INFLUXDB_RETENTION_POLICY=raw_data
export INFLUXDB_BATCH_SIZE=5000
export INFLUXDB_BATCH_TIMEOUT=500ms
export INFLUXDB_MAX_RETRIES=3
export INFLUXDB_RETRY_INTERVAL=1s

# Processor Configuration
export PROCESSOR_WORKER_COUNT=16
export PROCESSOR_QUEUE_SIZE=1000000
export PROCESSOR_ENABLE_AGGREGATIONS=true
```

### Building and Running

```bash
# Get dependencies
go mod tidy

# Build
go build -o consumer ./cmd/consumer/

# Run
./consumer
```

## Performance Tuning

To achieve 1M+ TPS throughput:

1. **Kafka Consumer Settings**:
   - Adjust `KAFKA_BATCH_SIZE` for optimal batch sizes
   - Set `KAFKA_CONSUMER_COUNT` to match partition count (32)

2. **InfluxDB Settings**:
   - Adjust `INFLUXDB_BATCH_SIZE` based on InfluxDB hardware (5,000-50,000)
   - Tune `INFLUXDB_BATCH_TIMEOUT` to balance latency vs. throughput

3. **Processor Settings**:
   - Set `PROCESSOR_WORKER_COUNT` to match available CPU cores
   - Adjust `PROCESSOR_QUEUE_SIZE` based on memory constraints

## Monitoring

The application logs key metrics and events to stdout:
- Consumer start/stop events
- Processing errors
- Batch processing statistics
- Queue overflow warnings

## Troubleshooting

Common issues:

1. **Poor throughput**:
   - Check Kafka partition count (should match consumer count)
   - Verify InfluxDB hardware (SSD, sufficient RAM)
   - Tune batch sizes and timeouts

2. **High memory usage**:
   - Reduce queue size
   - Reduce batch sizes
   - Check for memory leaks in aggregators

3. **Data loss**:
   - Increase InfluxDB retry settings
   - Check disk space on InfluxDB server
   - Verify Kafka retention settings