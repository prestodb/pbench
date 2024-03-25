--#BGBLK 10

-- degree 2: 15  seocnds
-- degree ANY: 80 seconds

-- set current schema bdinsights;

with ss as 
(select 
   SS_NET_PAID, SS_ADDR_SK
,SS_SOLD_DATE_SK, 
   ss_item_sk
   from store_sales 
--group by ss_item_sk, SS_SOLD_DATE_SK,
  -- SS_NET_PAID, SS_ADDR_SK
)
select count(*)
  --  sum(s1.SS_NET_PAID), s1.SS_ADDR_SK
   from ss s1 
 , ss s2
  where s1.ss_item_sk = s2.ss_item_sk
  and s1.SS_SOLD_DATE_SK<2451200 and s2.SS_SOLD_DATE_SK<2451200
-- group by s1.SS_ADDR_SK


/*<OPTGUIDELINES>
      <HSJOIN TQ_STRATEGY='OUTER_TO_INNER' TQ_TYPE='BROADCAST'>
            <TBSCAN TABID='Q2' /><TBSCAN TABID='Q1' />
          </HSJOIN>
</OPTGUIDELINES>*/

;



--  select max(SS_SOLD_DATE_SK) from  blu2.salesdtq;
    --  2452642
--  min(SS_SOLD_DATE_SK) from  blu2.salesdtq;
    --  2450816



--#EOBLK
