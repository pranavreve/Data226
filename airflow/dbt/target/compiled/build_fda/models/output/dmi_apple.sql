WITH apple_prices AS (
    SELECT
        date,
        close,
        symbol as series
    FROM dev.raw_data.stock_data
    WHERE series = 'AAPL'
),

-- Calculate the 20-day SMA and 20-day standard deviation
sma_stddev AS (
    SELECT
        date,
        close,
        AVG(close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS middle_band,
        STDDEV(close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS stddev_20
    FROM apple_prices
),

-- Calculate the upper and lower bands
bands AS (
    SELECT
        date,
        close,
        middle_band,
        (middle_band + (2 * stddev_20)) AS upper_band,
        (middle_band - (2 * stddev_20)) AS lower_band
    FROM sma_stddev
),

-- Calculate +DI and -DI based on daily price movements
directional_indicators AS (
    SELECT
        date,
        close,
        middle_band,
        upper_band,
        lower_band,
        LAG(close, 1) OVER (ORDER BY date) AS prev_close,
        -- +DI: Calculate positive movement
        CASE WHEN close > prev_close THEN close - prev_close ELSE 0 END AS plus_di,
        -- -DI: Calculate negative movement
        CASE WHEN close < prev_close THEN prev_close - close ELSE 0 END AS minus_di
    FROM bands
)

SELECT
    date,
    close,
    middle_band,
    upper_band,
    lower_band,
    -- Summing up DI over the 20-day period for smoother trend analysis
    SUM(plus_di) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS plus_di_twenty,
    SUM(minus_di) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS minus_di_twenty
FROM directional_indicators
ORDER BY date