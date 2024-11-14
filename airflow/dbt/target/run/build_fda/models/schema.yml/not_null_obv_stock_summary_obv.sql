select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select obv
from dev.analytics.obv_stock_summary
where obv is null



      
    ) dbt_internal_test