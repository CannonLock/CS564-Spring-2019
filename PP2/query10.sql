with type_sale as (select type,
                          extract(year from weekdate)    as year,
                          extract(quarter from weekdate) as quarters,
                          sum(weeklysales)               as sum
                   from sales
                          join stores on sales.store = stores.store
                   group by grouping sets ((type, year),(type, year, quarters)))
select A.year, A.quarters, A.sum as A_sale, B.sum as B_sale, C.sum as C_sale
from type_sale as A
       join (type_sale as B join type_sale as C on B.year = C.year and (B.quarters = C.quarters or (B.quarters isnull and c.quarters isnull) ))
         on A.year = B.year and (A.quarters = B.quarters or (A.quarters isnull and c.quarters isnull ))
where A.type = 'A'
  and B.type = 'B'
  and C.type = 'C'
order by year, quarters nulls last;
