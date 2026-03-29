CREATE TABLE IF NOT EXISTS stg_satellite_position (
    satellite_id TEXT,
    latitude TEXT,
    longitude TEXT,
    altitude_km TEXT,
    timestamp TEXT,
    UNIQUE (satellite_id, timestamp)
);

CREATE TABLE IF NOT EXISTS stg_telemetry_battery (
    satellite_id TEXT,
    voltage TEXT,
    temperature_c TEXT,
    timestamp TEXT,
    UNIQUE (satellite_id, timestamp)
);

CREATE TABLE IF NOT EXISTS dim_satellites (
    id SERIAL PRIMARY KEY,
    satellite_name VARCHAR(100) UNIQUE
);

INSERT INTO dim_satellites (satellite_name) 
VALUES 
    ('SENTINEL-1A'), 
    ('SENTINEL-1B'), 
    ('CUBESAT-X'), 
    ('CUBESAT-Y')
ON CONFLICT (satellite_name) DO NOTHING;
