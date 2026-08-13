-- TimescaleDB Schema for TelemetryCore
-- Initialize the telemetry database with hypertables for time-series data

-- Enable TimescaleDB extension if not already enabled
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Main telemetry readings table (hypertable for time-series)
CREATE TABLE IF NOT EXISTS telemetry_readings (
    id BIGSERIAL,
    device_id TEXT NOT NULL,
    timestamp BIGINT NOT NULL,                    -- Unix epoch in milliseconds
    temperature FLOAT8,
    voltage FLOAT8,
    acceleration_x FLOAT8,
    acceleration_y FLOAT8,
    acceleration_z FLOAT8,
    gyro_x FLOAT8,
    gyro_y FLOAT8,
    gyro_z FLOAT8,
    metadata JSONB DEFAULT '{}'::jsonb,          -- Store key-value tags as JSON
    inserted_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

-- Convert to hypertable with time partitioning on timestamp (converted to timestamp)
-- Use 1-day chunks for efficient queries
SELECT create_hypertable(
    'telemetry_readings',
    'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => '1 day'::interval
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_telemetry_device_id_time 
  ON telemetry_readings (device_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_inserted_at 
  ON telemetry_readings (inserted_at DESC);

-- Alerts table for anomaly detection results
CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,                     -- e.g., 'temperature_high', 'voltage_low'
    severity TEXT DEFAULT 'warning',              -- 'info', 'warning', 'critical'
    message TEXT,
    affected_reading_id BIGINT REFERENCES telemetry_readings(id) ON DELETE SET NULL,
    timestamp TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP
);

-- Index for alert queries
CREATE INDEX IF NOT EXISTS idx_alerts_device_id_timestamp 
  ON alerts (device_id, timestamp DESC);

-- Device metadata table
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    device_type TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Grant permissions (adjust user if needed)
-- Uncomment and modify to set proper permissions:
-- GRANT SELECT, INSERT ON telemetry_readings TO telemetry_app;
-- GRANT SELECT, INSERT ON alerts TO telemetry_app;
-- GRANT SELECT ON devices TO telemetry_app;
