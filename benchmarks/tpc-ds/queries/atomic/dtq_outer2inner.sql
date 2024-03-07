--#BGBLK 30

-- degree 2: 8.6 seocnds
-- degree ANY 12.3 seconds

--set current schema bdinsights;

select sum(s1."SS_NET_PAID"), s1."SS_ADDR_SK"
   from store_sales s1 ,  store_returns s2
  where s1.ss_item_sk = s2.sr_item_sk
  and s1."SS_SOLD_DATE_SK"<2450817 
group by s1."SS_ADDR_SK"




/*<OPTGUIDELINES>
      <HSJOIN TQ_STRATEGY='OUTER_TO_INNER' TQ_TYPE='DIRECTED'>
	    <TBSCAN TABID='Q2' /><TBSCAN TABID='Q1' />
	  </HSJOIN>
</OPTGUIDELINES>*/

;



--  select max("SS_SOLD_DATE_SK") from  blu2.salesdtq;
    --  2452642
--  min("SS_SOLD_DATE_SK") from  blu2.salesdtq;
    --  2450816



--#EOBLK
