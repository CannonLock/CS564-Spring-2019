with SalesTemporalCorr(AttributeName, CorrValue) AS (
  with SalesTemporal AS (
    select weeklysales, temperature, fuelprice, cpi, unemploymentrate
    from temporaldata
           join sales on temporaldata.store = sales.store and
                         temporaldata.weekdate = sales.weekdate
    )
    select 'Temperature',
           corr(weeklysales, temperature)
    from SalesTemporal
    union
    select 'Fuel price',
           corr(weeklysales, fuelprice)
    from SalesTemporal
    union
    select 'CPI',
           corr(weeklysales, cpi)
    from SalesTemporal
    union
    select 'Unemployment rate',
           corr(weeklysales, unemploymentrate)
    from SalesTemporal
)
select AttributeName,
       case
         when CorrValue > 0 then '+'
         when CorrValue < 0 then '-'
         else '0' end
         as CorrSign,
       CorrValue
from SalesTemporalCorr;