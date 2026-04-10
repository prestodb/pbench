-- q28.sql

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 72 and 72+10 
             or ss_coupon_amt between 15644 and 15644+1000
             or ss_wholesale_cost between 49 and 49+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 183 and 183+10
          or ss_coupon_amt between 9949 and 9949+1000
          or ss_wholesale_cost between 51 and 51+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 175 and 175+10
          or ss_coupon_amt between 1226 and 1226+1000
          or ss_wholesale_cost between 58 and 58+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 167 and 167+10
          or ss_coupon_amt between 9449 and 9449+1000
          or ss_wholesale_cost between 54 and 54+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 27 and 27+10
          or ss_coupon_amt between 3110 and 3110+1000
          or ss_wholesale_cost between 1 and 1+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 67 and 67+10
          or ss_coupon_amt between 8476 and 8476+1000
          or ss_wholesale_cost between 36 and 36+20)) B6
limit 100;
