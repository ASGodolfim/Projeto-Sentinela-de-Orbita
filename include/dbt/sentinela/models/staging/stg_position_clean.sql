WITH source AS (
    SELECT * FROM {{ source('aerospace_raw', 'stg_satellite_position') }}
)

SELECT
    satellite_id,
    CAST(NULLIF(TRIM(latitude), '') AS DECIMAL(10,6)) AS latitude,
    CAST(NULLIF(TRIM(longitude), '') AS DECIMAL(10,6)) AS longitude,
    CAST(REPLACE(NULLIF(TRIM(altitude_km), ''), ',', '.') AS DECIMAL(10,2)) AS altitude_km,
    CAST(NULLIF(TRIM(timestamp), '') AS TIMESTAMP) AS record_timestamp
FROM source
