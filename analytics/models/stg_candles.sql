WITH source_data AS (

    SELECT * FROM {{ source('snowflake_raw_data', 'RAW_CANDLES') }}

),

-- Renaming and cleaning up the columns
cleaned_and_renamed AS (

    SELECT
        -- Flatten the OBJECT and cast to a proper timestamp
        window:start::timestamp as candle_start_time,
        window:end::timestamp as candle_end_time,

        "OPEN" AS open_price,
        "HIGH" AS high_price,
        "LOW" AS low_price,
        "CLOSE" AS close_price,
        "VOLUME" AS trading_volume

    FROM source_data

)


SELECT * FROM cleaned_and_renamed