SET SESSION iceberg.compression_codec='NONE';
SET SESSION query_max_execution_time='12h';
SET SESSION query_max_run_time='12h';

USE iceberg.tpcds_sf1_parquet_iceberg;

INSERT INTO call_center
SELECT
    cast(cc_call_center_sk as INT),
    trim(cast(cc_call_center_id as VARCHAR(16))),
    cast(cc_rec_start_date as DATE),
    cast(cc_rec_end_date as DATE),
    cast(cc_closed_date_sk as INT),
    cast(cc_open_date_sk as INT),
    trim(cast(cc_name as VARCHAR(50))),
    trim(cast(cc_class as VARCHAR(50))),
    cast(cc_employees as INT),
    cast(cc_sq_ft as INT),
    trim(cast(cc_hours as VARCHAR(20))),
    trim(cast(cc_manager as VARCHAR(40))),
    cast(cc_mkt_id as INT),
    trim(cast(cc_mkt_class as VARCHAR(50))),
    trim(cast(cc_mkt_desc as VARCHAR(100))),
    trim(cast(cc_market_manager as VARCHAR(40))),
    cast(cc_division as INT),
    trim(cast(cc_division_name as VARCHAR(50))),
    cast(cc_company as INT),
    trim(cast(cc_company_name as VARCHAR(50))),
    trim(cast(cc_street_number as VARCHAR(10))),
    trim(cast(cc_street_name as VARCHAR(60))),
    trim(cast(cc_street_type as VARCHAR(15))),
    trim(cast(cc_suite_number as VARCHAR(10))),
    trim(cast(cc_city as VARCHAR(60))),
    trim(cast(cc_county as VARCHAR(30))),
    trim(cast(cc_state as VARCHAR(2))),
    trim(cast(cc_zip as VARCHAR(10))),
    trim(cast(cc_country as VARCHAR(20))),
    cast(cc_gmt_offset as DECIMAL(5,2)),
    cast(cc_tax_percentage as DECIMAL(5,2))
FROM tpcds.sf1.call_center;

INSERT INTO catalog_page
SELECT
    cast(cp_catalog_page_sk as INT),
    trim(cast(cp_catalog_page_id as VARCHAR(16))),
    cast(cp_start_date_sk as INT),
    cast(cp_end_date_sk as INT),
    trim(cast(cp_department as VARCHAR(50))),
    cast(cp_catalog_number as INT),
    cast(cp_catalog_page_number as INT),
    trim(cast(cp_description as VARCHAR(100))),
    trim(cast(cp_type as VARCHAR(100)))
FROM tpcds.sf1.catalog_page;

INSERT INTO catalog_returns
SELECT
    cast(cr_returned_date_sk as INT),
    cast(cr_returned_time_sk as INT),
    cast(cr_item_sk as INT),
    cast(cr_refunded_customer_sk as INT),
    cast(cr_refunded_cdemo_sk as INT),
    cast(cr_refunded_hdemo_sk as INT),
    cast(cr_refunded_addr_sk as INT),
    cast(cr_returning_customer_sk as INT),
    cast(cr_returning_cdemo_sk as INT),
    cast(cr_returning_hdemo_sk as INT),
    cast(cr_returning_addr_sk as INT),
    cast(cr_call_center_sk as INT),
    cast(cr_catalog_page_sk as INT),
    cast(cr_ship_mode_sk as INT),
    cast(cr_warehouse_sk as INT),
    cast(cr_reason_sk as INT),
    cast(cr_order_number as BIGINT),
    cast(cr_return_quantity as INT),
    cast(cr_return_amount as DECIMAL(7,2)),
    cast(cr_return_tax as DECIMAL(7,2)),
    cast(cr_return_amt_inc_tax as DECIMAL(7,2)),
    cast(cr_fee as DECIMAL(7,2)),
    cast(cr_return_ship_cost as DECIMAL(7,2)),
    cast(cr_refunded_cash as DECIMAL(7,2)),
    cast(cr_reversed_charge as DECIMAL(7,2)),
    cast(cr_store_credit as DECIMAL(7,2)),
    cast(cr_net_loss as DECIMAL(7,2))
FROM tpcds.sf1.catalog_returns;

INSERT INTO catalog_sales
SELECT
    cast(cs_sold_date_sk as INT),
    cast(cs_sold_time_sk as INT),
    cast(cs_ship_date_sk as INT),
    cast(cs_bill_customer_sk as INT),
    cast(cs_bill_cdemo_sk as INT),
    cast(cs_bill_hdemo_sk as INT),
    cast(cs_bill_addr_sk as INT),
    cast(cs_ship_customer_sk as INT),
    cast(cs_ship_cdemo_sk as INT),
    cast(cs_ship_hdemo_sk as INT),
    cast(cs_ship_addr_sk as INT),
    cast(cs_call_center_sk as INT),
    cast(cs_catalog_page_sk as INT),
    cast(cs_ship_mode_sk as INT),
    cast(cs_warehouse_sk as INT),
    cast(cs_item_sk as INT),
    cast(cs_promo_sk as INT),
    cast(cs_order_number as BIGINT),
    cast(cs_quantity as INT),
    cast(cs_wholesale_cost as DECIMAL(7,2)),
    cast(cs_list_price as DECIMAL(7,2)),
    cast(cs_sales_price as DECIMAL(7,2)),
    cast(cs_ext_discount_amt as DECIMAL(7,2)),
    cast(cs_ext_sales_price as DECIMAL(7,2)),
    cast(cs_ext_wholesale_cost as DECIMAL(7,2)),
    cast(cs_ext_list_price as DECIMAL(7,2)),
    cast(cs_ext_tax as DECIMAL(7,2)),
    cast(cs_coupon_amt as DECIMAL(7,2)),
    cast(cs_ext_ship_cost as DECIMAL(7,2)),
    cast(cs_net_paid as DECIMAL(7,2)),
    cast(cs_net_paid_inc_tax as DECIMAL(7,2)),
    cast(cs_net_paid_inc_ship as DECIMAL(7,2)),
    cast(cs_net_paid_inc_ship_tax as DECIMAL(7,2)),
    cast(cs_net_profit as DECIMAL(7,2))
FROM tpcds.sf1.catalog_sales;

INSERT INTO customer
SELECT
    cast(c_customer_sk as INT),
    trim(cast(c_customer_id as VARCHAR(16))),
    cast(c_current_cdemo_sk as INT),
    cast(c_current_hdemo_sk as INT),
    cast(c_current_addr_sk as INT),
    cast(c_first_shipto_date_sk as INT),
    cast(c_first_sales_date_sk as INT),
    trim(cast(c_salutation as VARCHAR(10))),
    trim(cast(c_first_name as VARCHAR(20))),
    trim(cast(c_last_name as VARCHAR(30))),
    trim(cast(c_preferred_cust_flag as VARCHAR(1))),
    cast(c_birth_day as INT),
    cast(c_birth_month as INT),
    cast(c_birth_year as INT),
    trim(cast(c_birth_country as VARCHAR(20))),
    trim(cast(c_login as VARCHAR(13))),
    trim(cast(c_email_address as VARCHAR(50))),
    cast(c_last_review_date_sk as INT)
FROM tpcds.sf1.customer;

INSERT INTO customer_address
SELECT
    cast(ca_address_sk as INT),
    trim(cast(ca_address_id as VARCHAR(16))),
    trim(cast(ca_street_number as VARCHAR(10))),
    trim(cast(ca_street_name as VARCHAR(60))),
    trim(cast(ca_street_type as VARCHAR(15))),
    trim(cast(ca_suite_number as VARCHAR(10))),
    trim(cast(ca_city as VARCHAR(60))),
    trim(cast(ca_county as VARCHAR(30))),
    trim(cast(ca_state as VARCHAR(2))),
    trim(cast(ca_zip as VARCHAR(10))),
    trim(cast(ca_country as VARCHAR(20))),
    cast(ca_gmt_offset as DECIMAL(5,2)),
    trim(cast(ca_location_type as VARCHAR(20)))
FROM tpcds.sf1.customer_address;

INSERT INTO customer_demographics
SELECT
    cast(cd_demo_sk as INT),
    trim(cast(cd_gender as VARCHAR(1))),
    trim(cast(cd_marital_status as VARCHAR(1))),
    trim(cast(cd_education_status as VARCHAR(20))),
    cast(cd_purchase_estimate as INT),
    trim(cast(cd_credit_rating as VARCHAR(10))),
    cast(cd_dep_count as INT),
    cast(cd_dep_employed_count as INT),
    cast(cd_dep_college_count as INT)
FROM tpcds.sf1.customer_demographics;

INSERT INTO date_dim
SELECT
    cast(d_date_sk as INT),
    trim(cast(d_date_id as VARCHAR(16))),
    cast(d_date as DATE),
    cast(d_month_seq as INT),
    cast(d_week_seq as INT),
    cast(d_quarter_seq as INT),
    cast(d_year as INT),
    cast(d_dow as INT),
    cast(d_moy as INT),
    cast(d_dom as INT),
    cast(d_qoy as INT),
    cast(d_fy_year as INT),
    cast(d_fy_quarter_seq as INT),
    cast(d_fy_week_seq as INT),
    trim(cast(d_day_name as VARCHAR(9))),
    trim(cast(d_quarter_name as VARCHAR(6))),
    trim(cast(d_holiday as VARCHAR(1))),
    trim(cast(d_weekend as VARCHAR(1))),
    trim(cast(d_following_holiday as VARCHAR(1))),
    cast(d_first_dom as INT),
    cast(d_last_dom as INT),
    cast(d_same_day_ly as INT),
    cast(d_same_day_lq as INT),
    trim(cast(d_current_day as VARCHAR(1))),
    trim(cast(d_current_week as VARCHAR(1))),
    trim(cast(d_current_month as VARCHAR(1))),
    trim(cast(d_current_quarter as VARCHAR(1))),
    trim(cast(d_current_year as VARCHAR(1)))
FROM tpcds.sf1.date_dim;

INSERT INTO household_demographics
SELECT
    cast(hd_demo_sk as INT),
    cast(hd_income_band_sk as INT),
    trim(cast(hd_buy_potential as VARCHAR(15))),
    cast(hd_dep_count as INT),
    cast(hd_vehicle_count as INT)
FROM tpcds.sf1.household_demographics;

INSERT INTO income_band
SELECT
    cast(ib_income_band_sk as INT),
    cast(ib_lower_bound as INT),
    cast(ib_upper_bound as INT)
FROM tpcds.sf1.income_band;

INSERT INTO inventory
SELECT
    cast(inv_date_sk as INT),
    cast(inv_item_sk as INT),
    cast(inv_warehouse_sk as INT),
    cast(inv_quantity_on_hand as INT)
FROM tpcds.sf1.inventory;

INSERT INTO item
SELECT
    cast(i_item_sk as INT),
    trim(cast(i_item_id as VARCHAR(16))),
    cast(i_rec_start_date as DATE),
    cast(i_rec_end_date as DATE),
    trim(cast(i_item_desc as VARCHAR(200))),
    cast(i_current_price as DECIMAL(7,2)),
    cast(i_wholesale_cost as DECIMAL(7,2)),
    cast(i_brand_id as INT),
    trim(cast(i_brand as VARCHAR(50))),
    cast(i_class_id as INT),
    trim(cast(i_class as VARCHAR(50))),
    cast(i_category_id as INT),
    trim(cast(i_category as VARCHAR(50))),
    cast(i_manufact_id as INT),
    trim(cast(i_manufact as VARCHAR(50))),
    trim(cast(i_size as VARCHAR(20))),
    trim(cast(i_formulation as VARCHAR(20))),
    trim(cast(i_color as VARCHAR(20))),
    trim(cast(i_units as VARCHAR(10))),
    trim(cast(i_container as VARCHAR(10))),
    cast(i_manager_id as INT),
    trim(cast(i_product_name as VARCHAR(50)))
FROM tpcds.sf1.item;

INSERT INTO promotion
SELECT
    cast(p_promo_sk as INT),
    trim(cast(p_promo_id as VARCHAR(16))),
    cast(p_start_date_sk as INT),
    cast(p_end_date_sk as INT),
    cast(p_item_sk as INT),
    cast(p_cost as DECIMAL(15,2)),
    cast(p_response_targe as INT),
    trim(cast(p_promo_name as VARCHAR(50))),
    trim(cast(p_channel_dmail as VARCHAR(1))),
    trim(cast(p_channel_email as VARCHAR(1))),
    trim(cast(p_channel_catalog as VARCHAR(1))),
    trim(cast(p_channel_tv as VARCHAR(1))),
    trim(cast(p_channel_radio as VARCHAR(1))),
    trim(cast(p_channel_press as VARCHAR(1))),
    trim(cast(p_channel_event as VARCHAR(1))),
    trim(cast(p_channel_demo as VARCHAR(1))),
    trim(cast(p_channel_details as VARCHAR(100))),
    trim(cast(p_purpose as VARCHAR(15))),
    trim(cast(p_discount_active as VARCHAR(1)))
FROM tpcds.sf1.promotion;

INSERT INTO reason
SELECT
    cast(r_reason_sk as INT),
    trim(cast(r_reason_id as VARCHAR(16))),
    trim(cast(r_reason_desc as VARCHAR(100)))
FROM tpcds.sf1.reason;

INSERT INTO ship_mode
SELECT
    cast(sm_ship_mode_sk as INT),
    trim(cast(sm_ship_mode_id as VARCHAR(16))),
    trim(cast(sm_type as VARCHAR(30))),
    trim(cast(sm_code as VARCHAR(10))),
    trim(cast(sm_carrier as VARCHAR(20))),
    trim(cast(sm_contract as VARCHAR(20)))
FROM tpcds.sf1.ship_mode;

INSERT INTO store
SELECT
    cast(s_store_sk as INT),
    trim(cast(s_store_id as VARCHAR(16))),
    cast(s_rec_start_date as DATE),
    cast(s_rec_end_date as DATE),
    cast(s_closed_date_sk as INT),
    trim(cast(s_store_name as VARCHAR(50))),
    cast(s_number_employees as INT),
    cast(s_floor_space as INT),
    trim(cast(s_hours as VARCHAR(20))),
    trim(cast(s_manager as VARCHAR(40))),
    cast(s_market_id as INT),
    trim(cast(s_geography_class as VARCHAR(100))),
    trim(cast(s_market_desc as VARCHAR(100))),
    trim(cast(s_market_manager as VARCHAR(40))),
    cast(s_division_id as INT),
    trim(cast(s_division_name as VARCHAR(50))),
    cast(s_company_id as INT),
    trim(cast(s_company_name as VARCHAR(50))),
    trim(cast(s_street_number as VARCHAR(10))),
    trim(cast(s_street_name as VARCHAR(60))),
    trim(cast(s_street_type as VARCHAR(15))),
    trim(cast(s_suite_number as VARCHAR(10))),
    trim(cast(s_city as VARCHAR(60))),
    trim(cast(s_county as VARCHAR(30))),
    trim(cast(s_state as VARCHAR(2))),
    trim(cast(s_zip as VARCHAR(10))),
    trim(cast(s_country as VARCHAR(20))),
    cast(s_gmt_offset as DECIMAL(5,2)),
    cast(s_tax_precentage as DECIMAL(5,2))
FROM tpcds.sf1.store;

INSERT INTO store_returns
SELECT
    cast(sr_returned_date_sk as INT),
    cast(sr_return_time_sk as INT),
    cast(sr_item_sk as INT),
    cast(sr_customer_sk as INT),
    cast(sr_cdemo_sk as INT),
    cast(sr_hdemo_sk as INT),
    cast(sr_addr_sk as INT),
    cast(sr_store_sk as INT),
    cast(sr_reason_sk as INT),
    cast(sr_ticket_number as BIGINT),
    cast(sr_return_quantity as INT),
    cast(sr_return_amt as DECIMAL(7,2)),
    cast(sr_return_tax as DECIMAL(7,2)),
    cast(sr_return_amt_inc_tax as DECIMAL(7,2)),
    cast(sr_fee as DECIMAL(7,2)),
    cast(sr_return_ship_cost as DECIMAL(7,2)),
    cast(sr_refunded_cash as DECIMAL(7,2)),
    cast(sr_reversed_charge as DECIMAL(7,2)),
    cast(sr_store_credit as DECIMAL(7,2)),
    cast(sr_net_loss as DECIMAL(7,2))
FROM tpcds.sf1.store_returns;

INSERT INTO store_sales
SELECT
    cast(ss_sold_date_sk as INT),
    cast(ss_sold_time_sk as INT),
    cast(ss_item_sk as INT),
    cast(ss_customer_sk as INT),
    cast(ss_cdemo_sk as INT),
    cast(ss_hdemo_sk as INT),
    cast(ss_addr_sk as INT),
    cast(ss_store_sk as INT),
    cast(ss_promo_sk as INT),
    cast(ss_ticket_number as BIGINT),
    cast(ss_quantity as INT),
    cast(ss_wholesale_cost as DECIMAL(7,2)),
    cast(ss_list_price as DECIMAL(7,2)),
    cast(ss_sales_price as DECIMAL(7,2)),
    cast(ss_ext_discount_amt as DECIMAL(7,2)),
    cast(ss_ext_sales_price as DECIMAL(7,2)),
    cast(ss_ext_wholesale_cost as DECIMAL(7,2)),
    cast(ss_ext_list_price as DECIMAL(7,2)),
    cast(ss_ext_tax as DECIMAL(7,2)),
    cast(ss_coupon_amt as DECIMAL(7,2)),
    cast(ss_net_paid as DECIMAL(7,2)),
    cast(ss_net_paid_inc_tax as DECIMAL(7,2)),
    cast(ss_net_profit as DECIMAL(7,2))
FROM tpcds.sf1.store_sales;

INSERT INTO time_dim
SELECT
    cast(t_time_sk as INT),
    trim(cast(t_time_id as VARCHAR(16))),
    cast(t_time as INT),
    cast(t_hour as INT),
    cast(t_minute as INT),
    cast(t_second as INT),
    trim(cast(t_am_pm as VARCHAR(2))),
    trim(cast(t_shift as VARCHAR(20))),
    trim(cast(t_sub_shift as VARCHAR(20))),
    trim(cast(t_meal_time as VARCHAR(20)))
FROM tpcds.sf1.time_dim;

INSERT INTO warehouse
SELECT
    cast(w_warehouse_sk as INT),
    trim(cast(w_warehouse_id as VARCHAR(16))),
    trim(cast(w_warehouse_name as VARCHAR(20))),
    cast(w_warehouse_sq_ft as INT),
    trim(cast(w_street_number as VARCHAR(10))),
    trim(cast(w_street_name as VARCHAR(60))),
    trim(cast(w_street_type as VARCHAR(15))),
    trim(cast(w_suite_number as VARCHAR(10))),
    trim(cast(w_city as VARCHAR(60))),
    trim(cast(w_county as VARCHAR(30))),
    trim(cast(w_state as VARCHAR(2))),
    trim(cast(w_zip as VARCHAR(10))),
    trim(cast(w_country as VARCHAR(20))),
    cast(w_gmt_offset as DECIMAL(5,2))
FROM tpcds.sf1.warehouse;

INSERT INTO web_page
SELECT
    cast(wp_web_page_sk as INT),
    trim(cast(wp_web_page_id as VARCHAR(16))),
    cast(wp_rec_start_date as DATE),
    cast(wp_rec_end_date as DATE),
    cast(wp_creation_date_sk as INT),
    cast(wp_access_date_sk as INT),
    trim(cast(wp_autogen_flag as VARCHAR(1))),
    cast(wp_customer_sk as INT),
    trim(cast(wp_url as VARCHAR(100))),
    trim(cast(wp_type as VARCHAR(50))),
    cast(wp_char_count as INT),
    cast(wp_link_count as INT),
    cast(wp_image_count as INT),
    cast(wp_max_ad_count as INT)
FROM tpcds.sf1.web_page;

INSERT INTO web_returns
SELECT
    cast(wr_returned_date_sk as INT),
    cast(wr_returned_time_sk as INT),
    cast(wr_item_sk as INT),
    cast(wr_refunded_customer_sk as INT),
    cast(wr_refunded_cdemo_sk as INT),
    cast(wr_refunded_hdemo_sk as INT),
    cast(wr_refunded_addr_sk as INT),
    cast(wr_returning_customer_sk as INT),
    cast(wr_returning_cdemo_sk as INT),
    cast(wr_returning_hdemo_sk as INT),
    cast(wr_returning_addr_sk as INT),
    cast(wr_web_page_sk as INT),
    cast(wr_reason_sk as INT),
    cast(wr_order_number as BIGINT),
    cast(wr_return_quantity as INT),
    cast(wr_return_amt as DECIMAL(7,2)),
    cast(wr_return_tax as DECIMAL(7,2)),
    cast(wr_return_amt_inc_tax as DECIMAL(7,2)),
    cast(wr_fee as DECIMAL(7,2)),
    cast(wr_return_ship_cost as DECIMAL(7,2)),
    cast(wr_refunded_cash as DECIMAL(7,2)),
    cast(wr_reversed_charge as DECIMAL(7,2)),
    cast(wr_account_credit as DECIMAL(7,2)),
    cast(wr_net_loss as DECIMAL(7,2))
FROM tpcds.sf1.web_returns;

INSERT INTO web_sales
SELECT
    cast(ws_sold_date_sk as INT),
    cast(ws_sold_time_sk as INT),
    cast(ws_ship_date_sk as INT),
    cast(ws_item_sk as INT),
    cast(ws_bill_customer_sk as INT),
    cast(ws_bill_cdemo_sk as INT),
    cast(ws_bill_hdemo_sk as INT),
    cast(ws_bill_addr_sk as INT),
    cast(ws_ship_customer_sk as INT),
    cast(ws_ship_cdemo_sk as INT),
    cast(ws_ship_hdemo_sk as INT),
    cast(ws_ship_addr_sk as INT),
    cast(ws_web_page_sk as INT),
    cast(ws_web_site_sk as INT),
    cast(ws_ship_mode_sk as INT),
    cast(ws_warehouse_sk as INT),
    cast(ws_promo_sk as INT),
    cast(ws_order_number as BIGINT),
    cast(ws_quantity as INT),
    cast(ws_wholesale_cost as DECIMAL(7,2)),
    cast(ws_list_price as DECIMAL(7,2)),
    cast(ws_sales_price as DECIMAL(7,2)),
    cast(ws_ext_discount_amt as DECIMAL(7,2)),
    cast(ws_ext_sales_price as DECIMAL(7,2)),
    cast(ws_ext_wholesale_cost as DECIMAL(7,2)),
    cast(ws_ext_list_price as DECIMAL(7,2)),
    cast(ws_ext_tax as DECIMAL(7,2)),
    cast(ws_coupon_amt as DECIMAL(7,2)),
    cast(ws_ext_ship_cost as DECIMAL(7,2)),
    cast(ws_net_paid as DECIMAL(7,2)),
    cast(ws_net_paid_inc_tax as DECIMAL(7,2)),
    cast(ws_net_paid_inc_ship as DECIMAL(7,2)),
    cast(ws_net_paid_inc_ship_tax as DECIMAL(7,2)),
    cast(ws_net_profit as DECIMAL(7,2))
FROM tpcds.sf1.web_sales;

INSERT INTO web_site
SELECT
    cast(web_site_sk as INT),
    trim(cast(web_site_id as VARCHAR(16))),
    cast(web_rec_start_date as DATE),
    cast(web_rec_end_date as DATE),
    trim(cast(web_name as VARCHAR(50))),
    cast(web_open_date_sk as INT),
    cast(web_close_date_sk as INT),
    trim(cast(web_class as VARCHAR(50))),
    trim(cast(web_manager as VARCHAR(40))),
    cast(web_mkt_id as INT),
    trim(cast(web_mkt_class as VARCHAR(50))),
    trim(cast(web_mkt_desc as VARCHAR(100))),
    trim(cast(web_market_manager as VARCHAR(40))),
    cast(web_company_id as INT),
    trim(cast(web_company_name as VARCHAR(50))),
    trim(cast(web_street_number as VARCHAR(10))),
    trim(cast(web_street_name as VARCHAR(60))),
    trim(cast(web_street_type as VARCHAR(15))),
    trim(cast(web_suite_number as VARCHAR(10))),
    trim(cast(web_city as VARCHAR(60))),
    trim(cast(web_county as VARCHAR(30))),
    trim(cast(web_state as VARCHAR(2))),
    trim(cast(web_zip as VARCHAR(10))),
    trim(cast(web_country as VARCHAR(20))),
    cast(web_gmt_offset as DECIMAL(5,2)),
    cast(web_tax_percentage as DECIMAL(5,2))
FROM tpcds.sf1.web_site;

ANALYZE call_center;
ANALYZE catalog_page;
ANALYZE catalog_returns;
ANALYZE catalog_sales;
ANALYZE customer;
ANALYZE customer_address;
ANALYZE customer_demographics;
ANALYZE date_dim;
ANALYZE household_demographics;
ANALYZE income_band;
ANALYZE inventory;
ANALYZE item;
ANALYZE promotion;
ANALYZE reason;
ANALYZE ship_mode;
ANALYZE store;
ANALYZE store_returns;
ANALYZE store_sales;
ANALYZE time_dim;
ANALYZE warehouse;
ANALYZE web_page;
ANALYZE web_returns;
ANALYZE web_sales;
ANALYZE web_site;
