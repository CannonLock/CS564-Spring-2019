with StoreDeptSales(store, dept, sales) AS
       (
         select store, dept, sum(weeklysales)
         from sales
         group by store, dept
       ),
     DeptNormSales(dept, norm_sales) AS
       (
         select dept, sum(sales / size)
         from StoreDeptSales
                join stores on stores.store = StoreDeptSales.store
         group by dept
       ),
     Top10DeptNormSales as (
       select dept
       from DeptNormSales
       order by norm_sales desc
       limit 10
     ),
     Top10DeptNormSalesPerMonth(dept, month, year, monthlysales) AS (
       select sales.dept,
              extract(month from sales.weekdate),
              extract(year from sales.weekdate),
              sum(sales.weeklysales)
       from Top10DeptNormSales
              join sales on sales.dept = Top10DeptNormSales.dept
       group by sales.dept,
                extract(month from sales.weekdate),
                extract(year from sales.weekdate)
     )
select dept,
       year as yr,
       month as mo,
       monthlysales,
       cast(monthlysales * 100/ sum(monthlysales) over (partition by dept) as decimal(18, 2)) as contribution,
       cast(sum(monthlysales)
           over (partition by dept order by year, month rows unbounded preceding) as decimal(18, 2)) as cumulative_sales
from Top10DeptNormSalesPerMonth;