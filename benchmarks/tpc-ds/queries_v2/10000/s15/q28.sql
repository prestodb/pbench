-- q28.sql

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 144 and 144+10 
             or ss_coupon_amt between 13839 and 13839+1000
             or ss_wholesale_cost between 63 and 63+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 106 and 106+10
          or ss_coupon_amt between 4460 and 4460+1000
          or ss_wholesale_cost between 11 and 11+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 7 and 7+10
          or ss_coupon_amt between 2965 and 2965+1000
          or ss_wholesale_cost between 35 and 35+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 114 and 114+10
          or ss_coupon_amt between 4258 and 4258+1000
          or ss_wholesale_cost between 45 and 45+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 148 and 148+10
          or ss_coupon_amt between 11867 and 11867+1000
          or ss_wholesale_cost between 19 and 19+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 41 and 41+10
          or ss_coupon_amt between 10738 and 10738+1000
          or ss_wholesale_cost between 78 and 78+20)) B6
limit 100;
