WITH source AS (
    SELECT * FROM {{ source('aerospace_raw', 'stg_telemetry_battery') }}
)

SELECT
    satellite_id,
    CASE 
        WHEN voltage = 'NaN' THEN NULL 
        ELSE CAST(voltage AS DECIMAL(5,2)) 
    END AS voltage,
    
    CASE 
        WHEN temperature_c = 'NaN' THEN NULL 
        ELSE CAST(temperature_c AS DECIMAL(5,2)) 
    END AS temperature_c,
    
    CAST(timestamp AS TIMESTAMP) AS record_timestamp
FROM source
