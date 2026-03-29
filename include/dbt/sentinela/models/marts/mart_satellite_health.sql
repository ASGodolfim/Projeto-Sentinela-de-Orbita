WITH telemetry AS (
    SELECT * FROM {{ ref('stg_telemetry_clean') }}
),
position AS (
    SELECT * FROM {{ ref('stg_position_clean') }}
),
dim_satellites AS (
    SELECT * FROM {{ source('aerospace_raw', 'dim_satellites') }}
)

SELECT
    t.satellite_id,
    s.satellite_name,
    t.record_timestamp AS telemetry_time,
    p.record_timestamp AS position_time,
    t.voltage,
    t.temperature_c,
    p.latitude,
    p.longitude,
    p.altitude_km
FROM telemetry t
INNER JOIN dim_satellites s 
    ON t.satellite_id = s.satellite_name
LEFT JOIN position p 
    ON t.satellite_id = p.satellite_id 
    -- Sincronizamos os dois pings de sensores usando o mesmo relógio analítico para cruzar os eixos
    AND EXTRACT(EPOCH FROM t.record_timestamp) = EXTRACT(EPOCH FROM p.record_timestamp)
