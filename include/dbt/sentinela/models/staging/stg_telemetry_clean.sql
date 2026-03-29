WITH source AS (
    SELECT * FROM {{ source('aerospace_raw', 'stg_telemetry_battery') }}
)

SELECT
    satellite_id,
    CAST(NULLIF(NULLIF(TRIM(voltage), ''), 'NaN') AS DECIMAL(5,2)) AS voltage,
    CAST(NULLIF(NULLIF(TRIM(temperature_c), ''), 'NaN') AS DECIMAL(5,2)) AS temperature_c,
    CAST(NULLIF(TRIM(timestamp), '') AS TIMESTAMP) AS record_timestamp
FROM source
