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
     Top10DeptNormSalesPerMonth(dept, month, year, weeklysales) AS (
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
       month,
       year,
       avg(weeklysales)
           over (partition by dept order by year, month rows 2 preceding)         as moving_average,
       sum(weeklysales)
           over (partition by dept order by year, month rows unbounded preceding) as cumulative_sales
from Top10DeptNormSalesPerMonth;