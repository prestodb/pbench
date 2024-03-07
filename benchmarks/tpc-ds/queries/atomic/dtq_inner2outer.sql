--#BGBLK 10

--degree 2: 9.3 seconds
--degree ANY: 15.4 seconds

-- db2 "select min(s1.SS_SOLD_DATE_SK), max(s1.SS_SOLD_DATE_SK) from blu2.store_sales s1"
----------- -----------
--    2450816     2452642



--set current schema bdinsights;

select sum(s1."SS_NET_PAID"), s1."SS_ADDR_SK"
   from store_SALES s1 , store_returns s2
  where s1.ss_item_sk = s2.sr_item_sk
  and s1."SS_SOLD_DATE_SK"<2450817
group by s1."SS_ADDR_SK"

/*<OPTGUIDELINES>
      <HSJOIN TQ_STRATEGY='INNER_TO_OUTER' TQ_TYPE='DIRECTED'>
	    <TBSCAN TABID='Q1' /><TBSCAN TABID='Q2' />
	  </HSJOIN>
</OPTGUIDELINES>*/

;



--  select max("SS_SOLD_DATE_SK") from  blu2.salesdtq;
    --  2452642
--  min("SS_SOLD_DATE_SK") from  blu2.salesdtq;
    --  2450816



--#EOBLK
