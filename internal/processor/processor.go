package processor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/config"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/influxdb"
	"github.com/kanna-karuppasamy/smart-grid-monitoring-consumer/internal/models"
)

// Processor processes incoming transactions
type Processor struct {
	influxClient         *influxdb.Client
	config               config.ProcessorConfig
	queue                chan []models.Transaction
	wg                   sync.WaitGroup
	statusAggregator     *statusAggregator
	regionAggregator     *regionAggregator
	timeSeriesAggregator *timeSeriesAggregator
}

// NewProcessor creates a new processor
func NewProcessor(influxClient *influxdb.Client, config config.ProcessorConfig) *Processor {
	p := &Processor{
		influxClient: influxClient,
		config:       config,
		queue:        make(chan []models.Transaction, config.QueueSize),
	}

	// Initialize aggregators if enabled
	if config.EnableAggregations {
		p.statusAggregator = newStatusAggregator(influxClient)
		p.regionAggregator = newRegionAggregator(influxClient)
		p.timeSeriesAggregator = newTimeSeriesAggregator(influxClient)
	}
	fmt.Println("NewProcessor")
	// Start workers
	p.wg.Add(config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		go p.worker(i)
	}

	return p
}

// ProcessMessages processes a batch of transactions
func (p *Processor) ProcessMessages(transactions []models.Transaction) error {
	// Create a copy of the transactions to avoid concurrent modification
	txCopy := make([]models.Transaction, len(transactions))
	copy(txCopy, transactions)

	fmt.Println("ProcessMessages")
	// Add to queue
	select {
	case p.queue <- txCopy:
		return nil
	default:
		// Queue is full, log and drop messages
		log.Printf("Warning: Processing queue is full, dropping %d messages", len(transactions))
		return nil
	}
}

// worker processes transactions from the queue
func (p *Processor) worker(id int) {

	fmt.Println("worker")
	defer p.wg.Done()

	for batch := range p.queue {
		// Process raw data
		if err := p.influxClient.WriteTransactions(batch); err != nil {
			log.Printf("Worker %d: Error writing transactions: %v", id, err)
			continue
		}

		// Process aggregations if enabled
		if p.config.EnableAggregations {
			// Update aggregators
			p.statusAggregator.update(batch)
			p.regionAggregator.update(batch)
			p.timeSeriesAggregator.update(batch)
		}
	}
}

// Stop stops the processor
func (p *Processor) Stop() {
	close(p.queue)
	p.wg.Wait()

	// Final flush for aggregators
	if p.config.EnableAggregations {
		p.statusAggregator.flush()
		p.regionAggregator.flush()
		p.timeSeriesAggregator.flush()
	}
}

// statusAggregator aggregates meter status information
type statusAggregator struct {
	client     *influxdb.Client
	counts     map[string]int
	mutex      sync.Mutex
	lastUpdate time.Time
}

func newStatusAggregator(client *influxdb.Client) *statusAggregator {

	fmt.Println("newStatusAggregator")
	a := &statusAggregator{
		client:     client,
		counts:     make(map[string]int),
		lastUpdate: time.Now(),
	}

	// Start periodic flusher
	go a.periodicFlush()

	return a
}

func (a *statusAggregator) update(transactions []models.Transaction) {

	fmt.Println("update")
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Update counts
	for _, tx := range transactions {
		a.counts[tx.Status]++
	}

	// Flush if enough time has passed
	if time.Since(a.lastUpdate) > 10*time.Second {
		a.flushLocked()
	}
}

func (a *statusAggregator) flush() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.flushLocked()
}

func (a *statusAggregator) flushLocked() {

	fmt.Println("flushLocked")
	if len(a.counts) == 0 {
		return
	}

	// Convert to slice
	counts := make([]models.StatusCount, 0, len(a.counts))
	for status, count := range a.counts {
		counts = append(counts, models.StatusCount{
			Status: status,
			Count:  count,
		})
	}

	// Write to InfluxDB
	if err := a.client.WriteStatusCounts(counts, time.Now()); err != nil {
		log.Printf("Error writing status counts: %v", err)
		return
	}

	// Reset counts
	a.counts = make(map[string]int)
	a.lastUpdate = time.Now()
}

func (a *statusAggregator) periodicFlush() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		a.flush()
	}
}

// regionAggregator aggregates consumption by region
type regionAggregator struct {
	client     *influxdb.Client
	regions    map[string]regionStats
	mutex      sync.Mutex
	lastUpdate time.Time
}

type regionStats struct {
	totalKWh       float64
	meterCount     int
	maxConsumption float64
	meterIDs       map[string]bool // Track unique meters
}

func newRegionAggregator(client *influxdb.Client) *regionAggregator {
	a := &regionAggregator{
		client:     client,
		regions:    make(map[string]regionStats),
		lastUpdate: time.Now(),
	}

	// Start periodic flusher
	go a.periodicFlush()

	return a
}

func (a *regionAggregator) update(transactions []models.Transaction) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Update stats
	for _, tx := range transactions {
		stats, exists := a.regions[tx.Region]
		if !exists {
			stats = regionStats{
				meterIDs: make(map[string]bool),
			}
		}

		stats.totalKWh += tx.ConsumptionKWh
		stats.meterIDs[tx.MeterID] = true
		stats.meterCount = len(stats.meterIDs)

		if tx.ConsumptionKWh > stats.maxConsumption {
			stats.maxConsumption = tx.ConsumptionKWh
		}

		a.regions[tx.Region] = stats
	}

	// Flush if enough time has passed
	if time.Since(a.lastUpdate) > 15*time.Second {
		a.flushLocked()
	}
}

func (a *regionAggregator) flush() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.flushLocked()
}

func (a *regionAggregator) flushLocked() {
	if len(a.regions) == 0 {
		return
	}

	// Convert to slice
	consumption := make([]models.RegionConsumption, 0, len(a.regions))
	for region, stats := range a.regions {
		consumption = append(consumption, models.RegionConsumption{
			Region:         region,
			TotalKWh:       stats.totalKWh,
			MeterCount:     stats.meterCount,
			AverageKWh:     stats.totalKWh / float64(stats.meterCount),
			MaxConsumption: stats.maxConsumption,
		})
	}

	// Write to InfluxDB
	if err := a.client.WriteRegionConsumption(consumption, time.Now()); err != nil {
		log.Printf("Error writing region consumption: %v", err)
		return
	}

	// Reset regions
	a.regions = make(map[string]regionStats)
	a.lastUpdate = time.Now()
}

func (a *regionAggregator) periodicFlush() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		a.flush()
	}
}

// timeSeriesAggregator aggregates time series data
type timeSeriesAggregator struct {
	client         *influxdb.Client
	buckets        map[time.Time]*timeSeriesBucket
	mutex          sync.Mutex
	lastFlush      time.Time
	bucketDuration time.Duration
}

type timeSeriesBucket struct {
	totalKWh     float64
	readingCount int
	maxKWh       float64
	minKWh       float64
	timestamp    time.Time
}

func newTimeSeriesAggregator(client *influxdb.Client) *timeSeriesAggregator {
	a := &timeSeriesAggregator{
		client:         client,
		buckets:        make(map[time.Time]*timeSeriesBucket),
		lastFlush:      time.Now(),
		bucketDuration: 1 * time.Second, // 1-second buckets
	}

	// Start periodic flusher
	go a.periodicFlush()

	return a
}

func (a *timeSeriesAggregator) update(transactions []models.Transaction) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	for _, tx := range transactions {
		// Truncate timestamp to bucket duration
		bucketTime := tx.Timestamp.Truncate(a.bucketDuration)

		bucket, exists := a.buckets[bucketTime]
		if !exists {
			bucket = &timeSeriesBucket{
				timestamp: bucketTime,
				minKWh:    tx.ConsumptionKWh, // Initialize min with first value
			}
			a.buckets[bucketTime] = bucket
		}

		bucket.totalKWh += tx.ConsumptionKWh
		bucket.readingCount++

		if tx.ConsumptionKWh > bucket.maxKWh {
			bucket.maxKWh = tx.ConsumptionKWh
		}

		if tx.ConsumptionKWh < bucket.minKWh {
			bucket.minKWh = tx.ConsumptionKWh
		}
	}

	// Flush old buckets periodically
	if time.Since(a.lastFlush) > 5*time.Second {
		a.flushOldBucketsLocked()
	}
}

func (a *timeSeriesAggregator) flushOldBucketsLocked() {
	threshold := time.Now().Add(-10 * time.Second)
	points := make([]models.TimeSeriesPoint, 0)

	for timestamp, bucket := range a.buckets {
		if timestamp.Before(threshold) {
			points = append(points, models.TimeSeriesPoint{
				Timestamp:    bucket.timestamp,
				TotalKWh:     bucket.totalKWh,
				ReadingCount: bucket.readingCount,
				MaxKWh:       bucket.maxKWh,
				MinKWh:       bucket.minKWh,
				AvgKWh:       bucket.totalKWh / float64(bucket.readingCount),
			})
			delete(a.buckets, timestamp)
		}
	}

	if len(points) > 0 {
		// Write to InfluxDB
		if err := a.client.WriteTimeSeriesPoints(points); err != nil {
			log.Printf("Error writing time series points: %v", err)
		}
	}

	a.lastFlush = time.Now()
}

func (a *timeSeriesAggregator) flush() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Flush all buckets
	points := make([]models.TimeSeriesPoint, 0, len(a.buckets))

	for timestamp, bucket := range a.buckets {
		points = append(points, models.TimeSeriesPoint{
			Timestamp:    bucket.timestamp,
			TotalKWh:     bucket.totalKWh,
			ReadingCount: bucket.readingCount,
			MaxKWh:       bucket.maxKWh,
			MinKWh:       bucket.minKWh,
			AvgKWh:       bucket.totalKWh / float64(bucket.readingCount),
		})
		delete(a.buckets, timestamp)
	}

	if len(points) > 0 {
		// Write to InfluxDB
		if err := a.client.WriteTimeSeriesPoints(points); err != nil {
			log.Printf("Error writing time series points: %v", err)
		}
	}
}

func (a *timeSeriesAggregator) periodicFlush() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		a.flush()
	}
}
