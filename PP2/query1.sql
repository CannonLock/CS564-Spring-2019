with StoreSale(store, sales) as (
  select store, sum(WeeklySales)
  from sales
         join holidays on sales.weekdate = holidays.weekdate
  where holidays.isholiday
  group by store
)
select store, sales
from StoreSale
where sales in (select max(sales)
                from StoreSale
                union
                select min(sales)
                from StoreSale);