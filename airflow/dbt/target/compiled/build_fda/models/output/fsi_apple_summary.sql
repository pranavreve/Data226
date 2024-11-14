WITH apple_interval_widths AS (
    SELECT
        ts,
        forecast,
        lower_bound,
        upper_bound,
        series,
        (upper_bound - lower_bound) AS interval_width
    FROM dev.adhoc.stock_data_forecast  -- Referencing the stock data forecast table
    WHERE series = 'AAPL'  -- Filter for Apple symbol
),

statistics AS (
    SELECT
        series,
        AVG(interval_width) AS avg_interval_width,
        STDDEV(interval_width) AS stddev_interval_width
    FROM apple_interval_widths
    GROUP BY series
)

SELECT
    a.ts,
    a.series,
    a.forecast,
    a.lower_bound,
    a.upper_bound,
    a.interval_width,
    s.avg_interval_width,
    s.stddev_interval_width,
    -- FSI Calculation
    1 - (s.stddev_interval_width / s.avg_interval_width) AS fsi
FROM apple_interval_widths a
JOIN statistics s
    ON a.series = s.series
ORDER BY a.ts