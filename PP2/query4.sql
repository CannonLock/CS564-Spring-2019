with MonthTypeSales(sales, month, type) AS (
  select sum(weeklysales), extract(month from weekdate), stores.type
  from sales
         join stores on sales.store = stores.store
  group by extract(month from weekdate), stores.type
)
select month                                               as months,
       type,
       sales                                               as sum,
       sales / (sum(sales) over (partition by type)) * 100 as contribution
from MonthTypeSales
order by type, month;