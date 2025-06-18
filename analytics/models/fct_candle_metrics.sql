-- Final mart model.
-- It selects from clean staging model and adds new metrics.

WITH stg_candles AS (

    SELECT * FROM {{ ref('stg_candles') }}

),

-- This CTE adds a 7-period simple moving average for the closing price
with_metrics AS (

    SELECT
        *,
        avg(close_price) OVER (
            ORDER BY candle_start_time
            rows BETWEEN 6 PRECEDING AND current row
        ) AS close_price_7_period_sma

    FROM stg_candles

)

SELECT * FROM with_metrics