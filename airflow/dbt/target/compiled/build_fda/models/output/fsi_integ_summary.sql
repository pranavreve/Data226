WITH interval_widths AS (
    SELECT
        ts,
        forecast,
        lower_bound,
        upper_bound,
        REPLACE(series, '"', '') AS series,
        (upper_bound - lower_bound) AS interval_width
    FROM dev.adhoc.stock_data_forecast  -- Referencing the stock data forecast table
),

statistics AS (
    SELECT
        series,
        AVG(interval_width) AS avg_interval_width,
        STDDEV(interval_width) AS stddev_interval_width
    FROM interval_widths
    GROUP BY series
)

SELECT
    distinct(a.series),
    s.avg_interval_width,
    s.stddev_interval_width,
    -- FSI Calculation
    1 - (s.stddev_interval_width / s.avg_interval_width) AS fsi
FROM interval_widths a
JOIN statistics s
    ON a.series = s.series