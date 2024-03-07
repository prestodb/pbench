--#BGBLK 10

set current query optimization 5;

--set current schema blu2;

select count(distinct SS_ITEM_SK), ss_sold_date_sk from store_sales  WHERE ss_sold_date_sk<=2450816+2000  group by ss_sold_date_sk ;


--#EOBLK
