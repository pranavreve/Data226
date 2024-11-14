SELECT *
FROM dev.analytics.fsi_stock_summary
WHERE symbol IN ('AAPL', 'NVDA')
  AND record_type = 'FORECASTED'
  AND fsi IS NULL