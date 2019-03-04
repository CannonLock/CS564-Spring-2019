with type_sale as (select type,
                          extract(year from weekdate)    as year,
                          extract(quarter from weekdate) as quarters,
                          sum(weeklysales)               as sum
                  from sales
                  join stores on sales.store = stores.store
                  group by type, year, quarters),

  summary (year, quarter, A_sale, B_sale, C_sale) as( select A.year, A.quarters, A.sum, B.sum, C.sum
                                                      from type_sale as A
                                                      join (type_sale as B join type_sale as C on B.year = C.year and (B.quarters = C.quarters))
                                                      on A.year = B.year and (A.quarters = B.quarters)
                                                      where A.type = 'A'
                                                      and B.type = 'B'
                                                      and C.type = 'C')

select * from summary
union all
select year, NULL, sum(A_sale), sum(B_sale), sum(C_sale)
from summary
group by year
order by year, quarter nulls last;
