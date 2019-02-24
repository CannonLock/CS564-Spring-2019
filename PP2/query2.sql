select distinct (store)
from temporaldata
where fuelprice < 4
  and unemploymentrate > 10;