select b, c, a, a * 2 as q
from (values (1, 2, 'shanghai'),
             (3, 4, 'dalian'),
             (5, 6, 'new york')) as X(a, b, c)
where a > 2;

select c, sum(a) as sum, count(*) as count, max(b) as max
from (values (1, 2, 'shanghai'),
             (3, 4, 'dalian'),
             (5, 6, 'new york'),
             (6, 87, 'new york'),
             (69, 1, 'dalian'),
             (15, 97, 'dalian')) as X(a, b, c)
group by c
order by count;
