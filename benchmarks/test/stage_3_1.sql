with x as (select *
           from (values (1, 2, 'shanghai'),
                        (3, 4, 'dalian'),
                        (5, 6, 'new york')) as X(a, b, c))
select *
from x
union all
select a + 10, b + 20, c
from x;

