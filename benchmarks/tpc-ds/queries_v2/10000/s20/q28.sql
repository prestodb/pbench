-- q28.sql

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 183 and 183+10 
             or ss_coupon_amt between 3614 and 3614+1000
             or ss_wholesale_cost between 12 and 12+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 47 and 47+10
          or ss_coupon_amt between 4566 and 4566+1000
          or ss_wholesale_cost between 0 and 0+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 120 and 120+10
          or ss_coupon_amt between 15450 and 15450+1000
          or ss_wholesale_cost between 70 and 70+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 8 and 8+10
          or ss_coupon_amt between 16948 and 16948+1000
          or ss_wholesale_cost between 66 and 66+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 77 and 77+10
          or ss_coupon_amt between 8258 and 8258+1000
          or ss_wholesale_cost between 39 and 39+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 59 and 59+10
          or ss_coupon_amt between 7973 and 7973+1000
          or ss_wholesale_cost between 38 and 38+20)) B6
limit 100;
