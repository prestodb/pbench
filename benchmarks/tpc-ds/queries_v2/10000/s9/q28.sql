-- q28.sql

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 164 and 164+10 
             or ss_coupon_amt between 2520 and 2520+1000
             or ss_wholesale_cost between 56 and 56+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 145 and 145+10
          or ss_coupon_amt between 14763 and 14763+1000
          or ss_wholesale_cost between 53 and 53+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 33 and 33+10
          or ss_coupon_amt between 6466 and 6466+1000
          or ss_wholesale_cost between 29 and 29+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 170 and 170+10
          or ss_coupon_amt between 7949 and 7949+1000
          or ss_wholesale_cost between 13 and 13+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 126 and 126+10
          or ss_coupon_amt between 4538 and 4538+1000
          or ss_wholesale_cost between 78 and 78+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 138 and 138+10
          or ss_coupon_amt between 9369 and 9369+1000
          or ss_wholesale_cost between 54 and 54+20)) B6
limit 100;
