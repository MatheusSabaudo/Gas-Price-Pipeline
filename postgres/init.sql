CREATE TABLE IF NOT EXISTS gas_prices (
    id SERIAL PRIMARY KEY,
    kafka_key VARCHAR(100),
    timestamp TIMESTAMP,
    date DATE,
    time TIME,
    hour INTEGER,
    day_of_week VARCHAR(20),
    month INTEGER,
    year INTEGER,
    region_id VARCHAR(10),
    region_name VARCHAR(50),
    region_zone VARCHAR(20),
    station_id VARCHAR(20),
    station_name VARCHAR(50),
    station_brand VARCHAR(50),
    fuel_type_id VARCHAR(10),
    fuel_type_name VARCHAR(50),
    price DECIMAL(5,3),
    price_unit VARCHAR(20),
    currency VARCHAR(3),
    is_weekend INTEGER,
    is_rush_hour INTEGER,
    is_anomaly INTEGER,
    anomaly_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gas_prices_timestamp ON gas_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_gas_prices_region ON gas_prices(region_id);
CREATE INDEX IF NOT EXISTS idx_gas_prices_fuel ON gas_prices(fuel_type_id);
CREATE INDEX IF NOT EXISTS idx_gas_prices_anomaly ON gas_prices(is_anomaly);