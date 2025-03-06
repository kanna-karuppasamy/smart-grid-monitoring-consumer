package influxdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/config"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/models"
)

// Client represents an InfluxDB v2 client
type Client struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
	config   config.InfluxDBConfig
	mu       sync.RWMutex
	closed   bool
}

// NewClient initializes the InfluxDB v2 client and verifies connectivity
func NewClient(cfg config.InfluxDBConfig) (*Client, error) {
	// Create the client
	client := influxdb2.NewClient(cfg.URL, cfg.Token)
	writeAPI := client.WriteAPI(cfg.Org, cfg.Bucket)

	// Add a health check to verify credentials
	_, err := client.Health(context.Background())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to InfluxDB: %v", err)
	}

	fmt.Println("Loaded Client Successfully - Connection Verified")
	return &Client{
		client:   client,
		writeAPI: writeAPI,
		config:   cfg,
		closed:   false,
	}, nil
}

// WriteTransactions writes transactions to InfluxDB v2
func (c *Client) WriteTransactions(transactions []models.Transaction) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return fmt.Errorf("client is closed")
	}

	for _, tx := range transactions {
		point := write.NewPoint(
			"energy_consumption",
			map[string]string{
				"meter_id":      tx.MeterID,
				"region":        tx.Region,
				"status":        tx.Status,
				"building_type": tx.BuildingType,
			},
			map[string]interface{}{
				"consumption_kwh": tx.ConsumptionKWh,
				"latitude":        tx.Latitude,
				"longitude":       tx.Longitude,
				"peak_load":       tx.PeakLoad,
			},
			tx.Timestamp, // Using transaction timestamp instead of current time
		)

		// Write the point
		c.writeAPI.WritePoint(point)
	}
	c.mu.RUnlock()

	return nil
}

// WriteStatusCounts writes aggregated status counts to InfluxDB
func (c *Client) WriteStatusCounts(counts []models.StatusCount, timestamp time.Time) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return fmt.Errorf("client is closed")
	}

	for _, count := range counts {
		point := write.NewPoint(
			"meter_status_counts",
			map[string]string{
				"status": count.Status,
			},
			map[string]interface{}{
				"count": count.Count,
			},
			timestamp,
		)

		c.writeAPI.WritePoint(point)
	}
	c.mu.RUnlock()

	return nil
}

// WriteRegionConsumption writes aggregated region consumption to InfluxDB
func (c *Client) WriteRegionConsumption(consumption []models.RegionConsumption, timestamp time.Time) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return fmt.Errorf("client is closed")
	}

	for _, rc := range consumption {
		point := write.NewPoint(
			"region_consumption",
			map[string]string{
				"region": rc.Region,
			},
			map[string]interface{}{
				"total_kwh":       rc.TotalKWh,
				"meter_count":     rc.MeterCount,
				"average_kwh":     rc.AverageKWh,
				"max_consumption": rc.MaxConsumption,
			},
			timestamp,
		)

		c.writeAPI.WritePoint(point)
	}
	c.mu.RUnlock()

	return nil
}

// WriteTimeSeriesPoints writes time series data points to InfluxDB
func (c *Client) WriteTimeSeriesPoints(points []models.TimeSeriesPoint) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return fmt.Errorf("client is closed")
	}

	for _, p := range points {
		point := write.NewPoint(
			"consumption_timeseries",
			map[string]string{}, // No tags for this measurement
			map[string]interface{}{
				"total_kwh":     p.TotalKWh,
				"reading_count": p.ReadingCount,
				"max_kwh":       p.MaxKWh,
				"min_kwh":       p.MinKWh,
				"avg_kwh":       p.AvgKWh,
			},
			p.Timestamp,
		)

		c.writeAPI.WritePoint(point)
	}
	c.mu.RUnlock()

	return nil
}

// Close gracefully closes the InfluxDB client
func (c *Client) Close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		// Flush any pending writes before closing
		c.writeAPI.Flush()
		c.client.Close()
	}
	c.mu.Unlock()
}
