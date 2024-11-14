select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT *
FROM dev.analytics.fsi_stock_summary
WHERE symbol IN ('AAPL', 'NVDA')
  AND record_type = 'FORECASTED'
  AND fsi IS NULL
      
    ) dbt_internal_test