--#BGBLK 100

set current query optimization 5;
--set current schema bdinsights;




SELECT COUNT(DISTINCT (SS_STORE_SK || ss_item_sk)) -- ss_ticket_number || ss_item_sk) )
from store_sales 
where SS_EXT_LIST_PRICE<50;


--#EOBLK
