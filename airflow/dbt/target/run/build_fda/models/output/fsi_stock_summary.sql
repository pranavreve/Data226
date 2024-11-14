
  
    

        create or replace transient table dev.analytics.fsi_stock_summary
         as
        (WITH interval_widths AS (
    SELECT
        ts as date,
        forecast,
        lower_bound,
        upper_bound,
        REPLACE(series, '"', '') AS symbol,
        (upper_bound - lower_bound) AS interval_width,
        'FORECASTED' as record_type
    FROM dev.adhoc.stock_data_forecast  -- Referencing the stock data forecast table
),

statistics AS (
    SELECT
        symbol,
        AVG(interval_width) AS avg_interval_width,
        STDDEV(interval_width) AS stddev_interval_width
    FROM interval_widths
    GROUP BY symbol
),

recent_data AS (
    SELECT
        date,
        close,
        symbol,
        'HISTORICAL' as record_type
    FROM dev.raw_data.stock_data
    WHERE date >= CURRENT_DATE() - INTERVAL '21 DAY'  -- Fetch data from the last 21 days (3 weeks)
      AND symbol IN ('AAPL', 'NVDA')                    -- Filter for specific symbols
)

SELECT
    a.record_type,
    a.symbol,
    a.date,
    a.forecast as close,
    s.avg_interval_width,
    s.stddev_interval_width,
    -- FSI Calculation
    1 - (s.stddev_interval_width / s.avg_interval_width) AS fsi
FROM interval_widths a
JOIN statistics s
    ON a.symbol = s.symbol

UNION ALL

SELECT
    r.record_type,
    r.symbol,
    r.date,
    r.close,
    NULL AS avg_interval_width,  -- NULL for non-forecasted data
    NULL AS stddev_interval_width,  -- NULL for non-forecasted data
    NULL AS fsi,  -- NULL for non-forecasted data
FROM recent_data r
ORDER BY date, symbol
        );
      
  