with DepartSale(store, dept ,weeklysales) as
       (
         select store, dept, sum(weeklysales) as sum
         from sales
         group by (store, dept)
       ),
     StoreSale(store, weeklysales) as
       (
         select store, sum(weeklysales)
         from DepartSale
         group by store
       ),
     DepartSaleContribution(dept, contribution) AS (
       select dept, DepartSale.weeklysales / StoreSale.weeklysales
       from StoreSale
              join DepartSale on StoreSale.store = DepartSale.store
       where DepartSale.weeklysales > StoreSale.weeklysales * 0.05
     )
select dept, avg(contribution)
from DepartSaleContribution
group by dept
having count(dept) > 3;