WITH source AS (
    SELECT * FROM {{ source('aerospace_raw', 'stg_satellite_position') }}
)

SELECT
    satellite_id,
    CAST(latitude AS DECIMAL(10,6)) AS latitude,
    CAST(longitude AS DECIMAL(10,6)) AS longitude,
    -- Sanitização exigida pela premissa: removemos a vírgula textual do sensor e inserimos ponto matemático
    CAST(REPLACE(altitude_km, ',', '.') AS DECIMAL(10,2)) AS altitude_km,
    CAST(timestamp AS TIMESTAMP) AS record_timestamp
FROM source
