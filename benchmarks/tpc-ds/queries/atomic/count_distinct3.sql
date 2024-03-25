--#BGBLK 100

--set current query optimization 5;
--set current schema bdinsights;




SELECT COUNT(DISTINCT (cast(SS_STORE_SK as varchar) || cast(ss_item_sk as varchar))) -- ss_ticket_number || ss_item_sk) )
from store_sales 
where SS_EXT_LIST_PRICE<50;


--#EOBLK
