-- q28.sql

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 114 and 114+10 
             or ss_coupon_amt between 9239 and 9239+1000
             or ss_wholesale_cost between 52 and 52+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 127 and 127+10
          or ss_coupon_amt between 3635 and 3635+1000
          or ss_wholesale_cost between 40 and 40+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 63 and 63+10
          or ss_coupon_amt between 8302 and 8302+1000
          or ss_wholesale_cost between 19 and 19+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 68 and 68+10
          or ss_coupon_amt between 1844 and 1844+1000
          or ss_wholesale_cost between 45 and 45+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 175 and 175+10
          or ss_coupon_amt between 16760 and 16760+1000
          or ss_wholesale_cost between 47 and 47+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 133 and 133+10
          or ss_coupon_amt between 1515 and 1515+1000
          or ss_wholesale_cost between 73 and 73+20)) B6
limit 100;
