with DepartSale(store,
       dept,
       weeklysales) as (select store, dept, sum(weeklysales) as sum from sales group by (store, dept)),
     StoreSale(store,
       weeklysales) as (select store, sum(weeklysales) from DepartSale group by store),
     DepartSaleContribution(store,
       dept,
       contribution) AS (select StoreSale.store, DepartSale.dept, DepartSale.weeklysales / StoreSale.weeklysales
                         from StoreSale
                                join DepartSale on StoreSale.store = DepartSale.store),
     DepartContributionAvg(dept,
       avg) as (select dept, avg(contribution) from DepartSaleContribution group by dept),
     DepartWant(dept) as (select dept
                          from DepartSaleContribution
                          where contribution > 0.05
                          group by dept
                          having count(dept) > 3)
select DepartWant.dept, avg
from DepartWant
       join DepartContributionAvg on DepartWant.dept = DepartContributionAvg.dept;