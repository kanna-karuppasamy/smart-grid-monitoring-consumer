package models

import (
	"time"
)

// Transaction represents a smart meter reading transaction
type Transaction struct {
	ID             string    `json:"id"`
	MeterID        string    `json:"meterId"`
	Timestamp      time.Time `json:"timestamp"`
	ConsumptionKWh float64   `json:"consumptionKWh"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	Region         string    `json:"region"`
	Status         string    `json:"status"`
	BuildingType   string    `json:"buildingType"`
	PeakLoad       bool      `json:"peakLoad"`
}

// StatusCount represents aggregated count of meters by status
type StatusCount struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// RegionConsumption represents aggregated consumption by region
type RegionConsumption struct {
	Region         string  `json:"region"`
	TotalKWh       float64 `json:"total_kwh"`
	MeterCount     int     `json:"meter_count"`
	AverageKWh     float64 `json:"average_kwh"`
	MaxConsumption float64 `json:"max_consumption"`
}

// TimeSeriesPoint represents a single aggregated time series data point
type TimeSeriesPoint struct {
	Timestamp    time.Time `json:"timestamp"`
	TotalKWh     float64   `json:"total_kwh"`
	ReadingCount int       `json:"reading_count"`
	MaxKWh       float64   `json:"max_kwh"`
	MinKWh       float64   `json:"min_kwh"`
	AvgKWh       float64   `json:"avg_kwh"`
}
