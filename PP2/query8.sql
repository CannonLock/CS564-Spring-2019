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
       )
select dept
from DeptNormSales
order by norm_sales desc
limit 10;