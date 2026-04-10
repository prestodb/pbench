-- q28.sql

select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 111 and 111+10 
             or ss_coupon_amt between 9591 and 9591+1000
             or ss_wholesale_cost between 40 and 40+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 104 and 104+10
          or ss_coupon_amt between 3322 and 3322+1000
          or ss_wholesale_cost between 39 and 39+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 91 and 91+10
          or ss_coupon_amt between 4333 and 4333+1000
          or ss_wholesale_cost between 50 and 50+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 190 and 190+10
          or ss_coupon_amt between 12554 and 12554+1000
          or ss_wholesale_cost between 65 and 65+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 78 and 78+10
          or ss_coupon_amt between 10810 and 10810+1000
          or ss_wholesale_cost between 56 and 56+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 49 and 49+10
          or ss_coupon_amt between 15885 and 15885+1000
          or ss_wholesale_cost between 1 and 1+20)) B6
limit 100;
