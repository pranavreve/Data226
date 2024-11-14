select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from dev.analytics.bollinger_stock_summary
where date is null



      
    ) dbt_internal_test