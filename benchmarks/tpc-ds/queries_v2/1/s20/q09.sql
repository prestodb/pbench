-- q09.sql

select case when (select count(*) 
                  from store_sales 
                  where ss_quantity between 1 and 20) > 19162
            then (select avg(ss_ext_tax) 
                  from store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 21 and 40) > 31922
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 41 and 60) > 7040
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 61 and 80) > 17822
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 81 and 100) > 45423
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1
;
