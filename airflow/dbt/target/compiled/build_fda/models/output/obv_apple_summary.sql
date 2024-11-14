WITH ordered_data AS (
    SELECT
        date,
        close,
        volume,
        symbol,
        LEAD(close) OVER (PARTITION BY symbol ORDER BY date) AS next_close
    FROM dev.raw_data.stock_data -- Referencing the raw data table in the dev database
    WHERE symbol = 'AAPL'  -- Filter for Apple symbol
),

obv_calculation AS (
    SELECT
        date,
        close,
        volume,
        symbol,
        CASE
            WHEN close > LAG(close) OVER (PARTITION BY symbol ORDER BY date) THEN volume
            WHEN close < LAG(close) OVER (PARTITION BY symbol ORDER BY date) THEN -volume
            ELSE 0
        END AS volume_change
    FROM ordered_data
),

cumulative_obv AS (
    SELECT
        date,
        symbol,
        SUM(volume_change) OVER (PARTITION BY symbol ORDER BY date) AS obv
    FROM obv_calculation
)

SELECT
    date,
    symbol,
    obv
FROM cumulative_obv
ORDER BY date