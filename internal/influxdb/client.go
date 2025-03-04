package influxdb

import (
	"context"
	"fmt"
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
}

// NewClient initializes the InfluxDB v2 client and verifies connectivity
func NewClient(cfg config.InfluxDBConfig) (*Client, error) {
	fmt.Println(cfg.Token)
	fmt.Println(cfg.URL)

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
	}, nil
}

// WriteTransactions writes transactions to InfluxDB v2
func (c *Client) WriteTransactions(transactions []models.Transaction) error {
	fmt.Println("WriteTransactions")
	fmt.Println(transactions[0].ConsumptionKWh)
	fmt.Println(transactions[10].ConsumptionKWh)
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

	return nil
}

// WriteStatusCounts writes aggregated status counts to InfluxDB
func (c *Client) WriteStatusCounts(counts []models.StatusCount, timestamp time.Time) error {
	fmt.Println("WriteStatusCounts")
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

	return nil
}

// WriteRegionConsumption writes aggregated region consumption to InfluxDB
func (c *Client) WriteRegionConsumption(consumption []models.RegionConsumption, timestamp time.Time) error {
	fmt.Println("WriteRegionConsumption")
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

	return nil
}

// WriteTimeSeriesPoints writes time series data points to InfluxDB
func (c *Client) WriteTimeSeriesPoints(points []models.TimeSeriesPoint) error {
	fmt.Println("WriteTimeSeriesPoints")
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

	return nil
}

// Close closes the InfluxDB client
func (c *Client) Close() {
	c.client.Close()
}
