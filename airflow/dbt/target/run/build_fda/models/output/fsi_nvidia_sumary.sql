
  
    

        create or replace transient table dev.analytics.fsi_nvidia_sumary
         as
        (WITH nvidia_interval_widths AS (
    SELECT
        ts,
        forecast,
        lower_bound,
        upper_bound,
        series,
        (upper_bound - lower_bound) AS interval_width
    FROM dev.adhoc.stock_data_forecast  -- Referencing the stock data forecast table
    WHERE series = 'NVDA'  -- Filter for Nvidia symbol
),

statistics AS (
    SELECT
        series,
        AVG(interval_width) AS avg_interval_width,
        STDDEV(interval_width) AS stddev_interval_width
    FROM nvidia_interval_widths
    GROUP BY series
)

SELECT
    n.ts,
    n.series,
    n.forecast,
    n.lower_bound,
    n.upper_bound,
    n.interval_width,
    s.avg_interval_width,
    s.stddev_interval_width,
    -- FSI Calculation
    1 - (s.stddev_interval_width / s.avg_interval_width) AS fsi
FROM nvidia_interval_widths n
JOIN statistics s
    ON n.series = s.series
ORDER BY n.ts
        );
      
  