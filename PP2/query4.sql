with MonthTypeSales(sales, month, type) AS (
  select sum(weeklysales), extract(month from weekdate), stores.type
  from sales
         join stores on sales.store = stores.store
  group by extract(month from weekdate), stores.type
)
select month,
       type,
       sales                                          as total_sales_per_month,
       sales / (sum(sales) over (partition by month)) as contribution
from MonthTypeSales
order by month, type