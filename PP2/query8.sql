with StoreDeptSales(store, dept, sales) AS
       (
         select store, dept, sum(weeklysales)
         from sales
         group by store, dept
       ),
     DeptNormSales(dept, normsales) AS
       (
         select dept, sum(sales / size)
         from StoreDeptSales
                join stores on stores.store = StoreDeptSales.store
         group by dept
       )
select dept, normsales
from DeptNormSales
order by normsales desc
limit 10;