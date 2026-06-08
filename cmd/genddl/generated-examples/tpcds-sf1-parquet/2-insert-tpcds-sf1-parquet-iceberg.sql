
USE iceberg.tpcds_target;


INSERT INTO iceberg.tpcds_target.call_center
SELECT
  CAST(NULLIF(cc_call_center_sk, '') AS INT) AS cc_call_center_sk,
  NULLIF(cc_call_center_id, '') AS cc_call_center_id,
  CAST(NULLIF(cc_rec_start_date, '') AS DATE) AS cc_rec_start_date,
  CAST(NULLIF(cc_rec_end_date, '') AS DATE) AS cc_rec_end_date,
  CAST(NULLIF(cc_closed_date_sk, '') AS INT) AS cc_closed_date_sk,
  CAST(NULLIF(cc_open_date_sk, '') AS INT) AS cc_open_date_sk,
  NULLIF(cc_name, '') AS cc_name,
  NULLIF(cc_class, '') AS cc_class,
  CAST(NULLIF(cc_employees, '') AS INT) AS cc_employees,
  CAST(NULLIF(cc_sq_ft, '') AS INT) AS cc_sq_ft,
  NULLIF(cc_hours, '') AS cc_hours,
  NULLIF(cc_manager, '') AS cc_manager,
  CAST(NULLIF(cc_mkt_id, '') AS INT) AS cc_mkt_id,
  NULLIF(cc_mkt_class, '') AS cc_mkt_class,
  NULLIF(cc_mkt_desc, '') AS cc_mkt_desc,
  NULLIF(cc_market_manager, '') AS cc_market_manager,
  CAST(NULLIF(cc_division, '') AS INT) AS cc_division,
  NULLIF(cc_division_name, '') AS cc_division_name,
  CAST(NULLIF(cc_company, '') AS INT) AS cc_company,
  NULLIF(cc_company_name, '') AS cc_company_name,
  NULLIF(cc_street_number, '') AS cc_street_number,
  NULLIF(cc_street_name, '') AS cc_street_name,
  NULLIF(cc_street_type, '') AS cc_street_type,
  NULLIF(cc_suite_number, '') AS cc_suite_number,
  NULLIF(cc_city, '') AS cc_city,
  NULLIF(cc_county, '') AS cc_county,
  NULLIF(cc_state, '') AS cc_state,
  NULLIF(cc_zip, '') AS cc_zip,
  NULLIF(cc_country, '') AS cc_country,
  CAST(NULLIF(cc_gmt_offset, '') AS DECIMAL(5,2)) AS cc_gmt_offset,
  CAST(NULLIF(cc_tax_percentage, '') AS DECIMAL(5,2)) AS cc_tax_percentage
FROM hive.tpcds_source.call_center;


INSERT INTO iceberg.tpcds_target.catalog_page
SELECT
  CAST(NULLIF(cp_catalog_page_sk, '') AS INT) AS cp_catalog_page_sk,
  NULLIF(cp_catalog_page_id, '') AS cp_catalog_page_id,
  CAST(NULLIF(cp_start_date_sk, '') AS INT) AS cp_start_date_sk,
  CAST(NULLIF(cp_end_date_sk, '') AS INT) AS cp_end_date_sk,
  NULLIF(cp_department, '') AS cp_department,
  CAST(NULLIF(cp_catalog_number, '') AS INT) AS cp_catalog_number,
  CAST(NULLIF(cp_catalog_page_number, '') AS INT) AS cp_catalog_page_number,
  NULLIF(cp_description, '') AS cp_description,
  NULLIF(cp_type, '') AS cp_type
FROM hive.tpcds_source.catalog_page;


INSERT INTO iceberg.tpcds_target.catalog_returns
SELECT
  CAST(NULLIF(cr_returned_date_sk, '') AS INT) AS cr_returned_date_sk,
  CAST(NULLIF(cr_returned_time_sk, '') AS INT) AS cr_returned_time_sk,
  CAST(NULLIF(cr_item_sk, '') AS INT) AS cr_item_sk,
  CAST(NULLIF(cr_refunded_customer_sk, '') AS INT) AS cr_refunded_customer_sk,
  CAST(NULLIF(cr_refunded_cdemo_sk, '') AS INT) AS cr_refunded_cdemo_sk,
  CAST(NULLIF(cr_refunded_hdemo_sk, '') AS INT) AS cr_refunded_hdemo_sk,
  CAST(NULLIF(cr_refunded_addr_sk, '') AS INT) AS cr_refunded_addr_sk,
  CAST(NULLIF(cr_returning_customer_sk, '') AS INT) AS cr_returning_customer_sk,
  CAST(NULLIF(cr_returning_cdemo_sk, '') AS INT) AS cr_returning_cdemo_sk,
  CAST(NULLIF(cr_returning_hdemo_sk, '') AS INT) AS cr_returning_hdemo_sk,
  CAST(NULLIF(cr_returning_addr_sk, '') AS INT) AS cr_returning_addr_sk,
  CAST(NULLIF(cr_call_center_sk, '') AS INT) AS cr_call_center_sk,
  CAST(NULLIF(cr_catalog_page_sk, '') AS INT) AS cr_catalog_page_sk,
  CAST(NULLIF(cr_ship_mode_sk, '') AS INT) AS cr_ship_mode_sk,
  CAST(NULLIF(cr_warehouse_sk, '') AS INT) AS cr_warehouse_sk,
  CAST(NULLIF(cr_reason_sk, '') AS INT) AS cr_reason_sk,
  CAST(NULLIF(cr_order_number, '') AS BIGINT) AS cr_order_number,
  CAST(NULLIF(cr_return_quantity, '') AS INT) AS cr_return_quantity,
  CAST(NULLIF(cr_return_amount, '') AS DECIMAL(7,2)) AS cr_return_amount,
  CAST(NULLIF(cr_return_tax, '') AS DECIMAL(7,2)) AS cr_return_tax,
  CAST(NULLIF(cr_return_amt_inc_tax, '') AS DECIMAL(7,2)) AS cr_return_amt_inc_tax,
  CAST(NULLIF(cr_fee, '') AS DECIMAL(7,2)) AS cr_fee,
  CAST(NULLIF(cr_return_ship_cost, '') AS DECIMAL(7,2)) AS cr_return_ship_cost,
  CAST(NULLIF(cr_refunded_cash, '') AS DECIMAL(7,2)) AS cr_refunded_cash,
  CAST(NULLIF(cr_reversed_charge, '') AS DECIMAL(7,2)) AS cr_reversed_charge,
  CAST(NULLIF(cr_store_credit, '') AS DECIMAL(7,2)) AS cr_store_credit,
  CAST(NULLIF(cr_net_loss, '') AS DECIMAL(7,2)) AS cr_net_loss
FROM hive.tpcds_source.catalog_returns;


INSERT INTO iceberg.tpcds_target.catalog_sales
SELECT
  CAST(NULLIF(cs_sold_date_sk, '') AS INT) AS cs_sold_date_sk,
  CAST(NULLIF(cs_sold_time_sk, '') AS INT) AS cs_sold_time_sk,
  CAST(NULLIF(cs_ship_date_sk, '') AS INT) AS cs_ship_date_sk,
  CAST(NULLIF(cs_bill_customer_sk, '') AS INT) AS cs_bill_customer_sk,
  CAST(NULLIF(cs_bill_cdemo_sk, '') AS INT) AS cs_bill_cdemo_sk,
  CAST(NULLIF(cs_bill_hdemo_sk, '') AS INT) AS cs_bill_hdemo_sk,
  CAST(NULLIF(cs_bill_addr_sk, '') AS INT) AS cs_bill_addr_sk,
  CAST(NULLIF(cs_ship_customer_sk, '') AS INT) AS cs_ship_customer_sk,
  CAST(NULLIF(cs_ship_cdemo_sk, '') AS INT) AS cs_ship_cdemo_sk,
  CAST(NULLIF(cs_ship_hdemo_sk, '') AS INT) AS cs_ship_hdemo_sk,
  CAST(NULLIF(cs_ship_addr_sk, '') AS INT) AS cs_ship_addr_sk,
  CAST(NULLIF(cs_call_center_sk, '') AS INT) AS cs_call_center_sk,
  CAST(NULLIF(cs_catalog_page_sk, '') AS INT) AS cs_catalog_page_sk,
  CAST(NULLIF(cs_ship_mode_sk, '') AS INT) AS cs_ship_mode_sk,
  CAST(NULLIF(cs_warehouse_sk, '') AS INT) AS cs_warehouse_sk,
  CAST(NULLIF(cs_item_sk, '') AS INT) AS cs_item_sk,
  CAST(NULLIF(cs_promo_sk, '') AS INT) AS cs_promo_sk,
  CAST(NULLIF(cs_order_number, '') AS BIGINT) AS cs_order_number,
  CAST(NULLIF(cs_quantity, '') AS INT) AS cs_quantity,
  CAST(NULLIF(cs_wholesale_cost, '') AS DECIMAL(7,2)) AS cs_wholesale_cost,
  CAST(NULLIF(cs_list_price, '') AS DECIMAL(7,2)) AS cs_list_price,
  CAST(NULLIF(cs_sales_price, '') AS DECIMAL(7,2)) AS cs_sales_price,
  CAST(NULLIF(cs_ext_discount_amt, '') AS DECIMAL(7,2)) AS cs_ext_discount_amt,
  CAST(NULLIF(cs_ext_sales_price, '') AS DECIMAL(7,2)) AS cs_ext_sales_price,
  CAST(NULLIF(cs_ext_wholesale_cost, '') AS DECIMAL(7,2)) AS cs_ext_wholesale_cost,
  CAST(NULLIF(cs_ext_list_price, '') AS DECIMAL(7,2)) AS cs_ext_list_price,
  CAST(NULLIF(cs_ext_tax, '') AS DECIMAL(7,2)) AS cs_ext_tax,
  CAST(NULLIF(cs_coupon_amt, '') AS DECIMAL(7,2)) AS cs_coupon_amt,
  CAST(NULLIF(cs_ext_ship_cost, '') AS DECIMAL(7,2)) AS cs_ext_ship_cost,
  CAST(NULLIF(cs_net_paid, '') AS DECIMAL(7,2)) AS cs_net_paid,
  CAST(NULLIF(cs_net_paid_inc_tax, '') AS DECIMAL(7,2)) AS cs_net_paid_inc_tax,
  CAST(NULLIF(cs_net_paid_inc_ship, '') AS DECIMAL(7,2)) AS cs_net_paid_inc_ship,
  CAST(NULLIF(cs_net_paid_inc_ship_tax, '') AS DECIMAL(7,2)) AS cs_net_paid_inc_ship_tax,
  CAST(NULLIF(cs_net_profit, '') AS DECIMAL(7,2)) AS cs_net_profit
FROM hive.tpcds_source.catalog_sales;


INSERT INTO iceberg.tpcds_target.customer
SELECT
  CAST(NULLIF(c_customer_sk, '') AS INT) AS c_customer_sk,
  NULLIF(c_customer_id, '') AS c_customer_id,
  CAST(NULLIF(c_current_cdemo_sk, '') AS INT) AS c_current_cdemo_sk,
  CAST(NULLIF(c_current_hdemo_sk, '') AS INT) AS c_current_hdemo_sk,
  CAST(NULLIF(c_current_addr_sk, '') AS INT) AS c_current_addr_sk,
  CAST(NULLIF(c_first_shipto_date_sk, '') AS INT) AS c_first_shipto_date_sk,
  CAST(NULLIF(c_first_sales_date_sk, '') AS INT) AS c_first_sales_date_sk,
  NULLIF(c_salutation, '') AS c_salutation,
  NULLIF(c_first_name, '') AS c_first_name,
  NULLIF(c_last_name, '') AS c_last_name,
  NULLIF(c_preferred_cust_flag, '') AS c_preferred_cust_flag,
  CAST(NULLIF(c_birth_day, '') AS INT) AS c_birth_day,
  CAST(NULLIF(c_birth_month, '') AS INT) AS c_birth_month,
  CAST(NULLIF(c_birth_year, '') AS INT) AS c_birth_year,
  NULLIF(c_birth_country, '') AS c_birth_country,
  NULLIF(c_login, '') AS c_login,
  NULLIF(c_email_address, '') AS c_email_address,
  CAST(NULLIF(c_last_review_date_sk, '') AS INT) AS c_last_review_date_sk
FROM hive.tpcds_source.customer;


INSERT INTO iceberg.tpcds_target.customer_address
SELECT
  CAST(NULLIF(ca_address_sk, '') AS INT) AS ca_address_sk,
  NULLIF(ca_address_id, '') AS ca_address_id,
  NULLIF(ca_street_number, '') AS ca_street_number,
  NULLIF(ca_street_name, '') AS ca_street_name,
  NULLIF(ca_street_type, '') AS ca_street_type,
  NULLIF(ca_suite_number, '') AS ca_suite_number,
  NULLIF(ca_city, '') AS ca_city,
  NULLIF(ca_county, '') AS ca_county,
  NULLIF(ca_state, '') AS ca_state,
  NULLIF(ca_zip, '') AS ca_zip,
  NULLIF(ca_country, '') AS ca_country,
  CAST(NULLIF(ca_gmt_offset, '') AS DECIMAL(5,2)) AS ca_gmt_offset,
  NULLIF(ca_location_type, '') AS ca_location_type
FROM hive.tpcds_source.customer_address;


INSERT INTO iceberg.tpcds_target.customer_demographics
SELECT
  CAST(NULLIF(cd_demo_sk, '') AS INT) AS cd_demo_sk,
  NULLIF(cd_gender, '') AS cd_gender,
  NULLIF(cd_marital_status, '') AS cd_marital_status,
  NULLIF(cd_education_status, '') AS cd_education_status,
  CAST(NULLIF(cd_purchase_estimate, '') AS INT) AS cd_purchase_estimate,
  NULLIF(cd_credit_rating, '') AS cd_credit_rating,
  CAST(NULLIF(cd_dep_count, '') AS INT) AS cd_dep_count,
  CAST(NULLIF(cd_dep_employed_count, '') AS INT) AS cd_dep_employed_count,
  CAST(NULLIF(cd_dep_college_count, '') AS INT) AS cd_dep_college_count
FROM hive.tpcds_source.customer_demographics;


INSERT INTO iceberg.tpcds_target.date_dim
SELECT
  CAST(NULLIF(d_date_sk, '') AS INT) AS d_date_sk,
  NULLIF(d_date_id, '') AS d_date_id,
  CAST(NULLIF(d_date, '') AS DATE) AS d_date,
  CAST(NULLIF(d_month_seq, '') AS INT) AS d_month_seq,
  CAST(NULLIF(d_week_seq, '') AS INT) AS d_week_seq,
  CAST(NULLIF(d_quarter_seq, '') AS INT) AS d_quarter_seq,
  CAST(NULLIF(d_year, '') AS INT) AS d_year,
  CAST(NULLIF(d_dow, '') AS INT) AS d_dow,
  CAST(NULLIF(d_moy, '') AS INT) AS d_moy,
  CAST(NULLIF(d_dom, '') AS INT) AS d_dom,
  CAST(NULLIF(d_qoy, '') AS INT) AS d_qoy,
  CAST(NULLIF(d_fy_year, '') AS INT) AS d_fy_year,
  CAST(NULLIF(d_fy_quarter_seq, '') AS INT) AS d_fy_quarter_seq,
  CAST(NULLIF(d_fy_week_seq, '') AS INT) AS d_fy_week_seq,
  NULLIF(d_day_name, '') AS d_day_name,
  NULLIF(d_quarter_name, '') AS d_quarter_name,
  NULLIF(d_holiday, '') AS d_holiday,
  NULLIF(d_weekend, '') AS d_weekend,
  NULLIF(d_following_holiday, '') AS d_following_holiday,
  CAST(NULLIF(d_first_dom, '') AS INT) AS d_first_dom,
  CAST(NULLIF(d_last_dom, '') AS INT) AS d_last_dom,
  CAST(NULLIF(d_same_day_ly, '') AS INT) AS d_same_day_ly,
  CAST(NULLIF(d_same_day_lq, '') AS INT) AS d_same_day_lq,
  NULLIF(d_current_day, '') AS d_current_day,
  NULLIF(d_current_week, '') AS d_current_week,
  NULLIF(d_current_month, '') AS d_current_month,
  NULLIF(d_current_quarter, '') AS d_current_quarter,
  NULLIF(d_current_year, '') AS d_current_year
FROM hive.tpcds_source.date_dim;


INSERT INTO iceberg.tpcds_target.household_demographics
SELECT
  CAST(NULLIF(hd_demo_sk, '') AS INT) AS hd_demo_sk,
  CAST(NULLIF(hd_income_band_sk, '') AS INT) AS hd_income_band_sk,
  NULLIF(hd_buy_potential, '') AS hd_buy_potential,
  CAST(NULLIF(hd_dep_count, '') AS INT) AS hd_dep_count,
  CAST(NULLIF(hd_vehicle_count, '') AS INT) AS hd_vehicle_count
FROM hive.tpcds_source.household_demographics;


INSERT INTO iceberg.tpcds_target.income_band
SELECT
  CAST(NULLIF(ib_income_band_sk, '') AS INT) AS ib_income_band_sk,
  CAST(NULLIF(ib_lower_bound, '') AS INT) AS ib_lower_bound,
  CAST(NULLIF(ib_upper_bound, '') AS INT) AS ib_upper_bound
FROM hive.tpcds_source.income_band;


INSERT INTO iceberg.tpcds_target.inventory
SELECT
  CAST(NULLIF(inv_date_sk, '') AS INT) AS inv_date_sk,
  CAST(NULLIF(inv_item_sk, '') AS INT) AS inv_item_sk,
  CAST(NULLIF(inv_warehouse_sk, '') AS INT) AS inv_warehouse_sk,
  CAST(NULLIF(inv_quantity_on_hand, '') AS INT) AS inv_quantity_on_hand
FROM hive.tpcds_source.inventory;


INSERT INTO iceberg.tpcds_target.item
SELECT
  CAST(NULLIF(i_item_sk, '') AS INT) AS i_item_sk,
  NULLIF(i_item_id, '') AS i_item_id,
  CAST(NULLIF(i_rec_start_date, '') AS DATE) AS i_rec_start_date,
  CAST(NULLIF(i_rec_end_date, '') AS DATE) AS i_rec_end_date,
  NULLIF(i_item_desc, '') AS i_item_desc,
  CAST(NULLIF(i_current_price, '') AS DECIMAL(7,2)) AS i_current_price,
  CAST(NULLIF(i_wholesale_cost, '') AS DECIMAL(7,2)) AS i_wholesale_cost,
  CAST(NULLIF(i_brand_id, '') AS INT) AS i_brand_id,
  NULLIF(i_brand, '') AS i_brand,
  CAST(NULLIF(i_class_id, '') AS INT) AS i_class_id,
  NULLIF(i_class, '') AS i_class,
  CAST(NULLIF(i_category_id, '') AS INT) AS i_category_id,
  NULLIF(i_category, '') AS i_category,
  CAST(NULLIF(i_manufact_id, '') AS INT) AS i_manufact_id,
  NULLIF(i_manufact, '') AS i_manufact,
  NULLIF(i_size, '') AS i_size,
  NULLIF(i_formulation, '') AS i_formulation,
  NULLIF(i_color, '') AS i_color,
  NULLIF(i_units, '') AS i_units,
  NULLIF(i_container, '') AS i_container,
  CAST(NULLIF(i_manager_id, '') AS INT) AS i_manager_id,
  NULLIF(i_product_name, '') AS i_product_name
FROM hive.tpcds_source.item;


INSERT INTO iceberg.tpcds_target.promotion
SELECT
  CAST(NULLIF(p_promo_sk, '') AS INT) AS p_promo_sk,
  NULLIF(p_promo_id, '') AS p_promo_id,
  CAST(NULLIF(p_start_date_sk, '') AS INT) AS p_start_date_sk,
  CAST(NULLIF(p_end_date_sk, '') AS INT) AS p_end_date_sk,
  CAST(NULLIF(p_item_sk, '') AS INT) AS p_item_sk,
  CAST(NULLIF(p_cost, '') AS DECIMAL(15,2)) AS p_cost,
  CAST(NULLIF(p_response_target, '') AS INT) AS p_response_target,
  NULLIF(p_promo_name, '') AS p_promo_name,
  NULLIF(p_channel_dmail, '') AS p_channel_dmail,
  NULLIF(p_channel_email, '') AS p_channel_email,
  NULLIF(p_channel_catalog, '') AS p_channel_catalog,
  NULLIF(p_channel_tv, '') AS p_channel_tv,
  NULLIF(p_channel_radio, '') AS p_channel_radio,
  NULLIF(p_channel_press, '') AS p_channel_press,
  NULLIF(p_channel_event, '') AS p_channel_event,
  NULLIF(p_channel_demo, '') AS p_channel_demo,
  NULLIF(p_channel_details, '') AS p_channel_details,
  NULLIF(p_purpose, '') AS p_purpose,
  NULLIF(p_discount_active, '') AS p_discount_active
FROM hive.tpcds_source.promotion;


INSERT INTO iceberg.tpcds_target.reason
SELECT
  CAST(NULLIF(r_reason_sk, '') AS INT) AS r_reason_sk,
  NULLIF(r_reason_id, '') AS r_reason_id,
  NULLIF(r_reason_desc, '') AS r_reason_desc
FROM hive.tpcds_source.reason;


INSERT INTO iceberg.tpcds_target.ship_mode
SELECT
  CAST(NULLIF(sm_ship_mode_sk, '') AS INT) AS sm_ship_mode_sk,
  NULLIF(sm_ship_mode_id, '') AS sm_ship_mode_id,
  NULLIF(sm_type, '') AS sm_type,
  NULLIF(sm_code, '') AS sm_code,
  NULLIF(sm_carrier, '') AS sm_carrier,
  NULLIF(sm_contract, '') AS sm_contract
FROM hive.tpcds_source.ship_mode;


INSERT INTO iceberg.tpcds_target.store
SELECT
  CAST(NULLIF(s_store_sk, '') AS INT) AS s_store_sk,
  NULLIF(s_store_id, '') AS s_store_id,
  CAST(NULLIF(s_rec_start_date, '') AS DATE) AS s_rec_start_date,
  CAST(NULLIF(s_rec_end_date, '') AS DATE) AS s_rec_end_date,
  CAST(NULLIF(s_closed_date_sk, '') AS INT) AS s_closed_date_sk,
  NULLIF(s_store_name, '') AS s_store_name,
  CAST(NULLIF(s_number_employees, '') AS INT) AS s_number_employees,
  CAST(NULLIF(s_floor_space, '') AS INT) AS s_floor_space,
  NULLIF(s_hours, '') AS s_hours,
  NULLIF(s_manager, '') AS s_manager,
  CAST(NULLIF(s_market_id, '') AS INT) AS s_market_id,
  NULLIF(s_geography_class, '') AS s_geography_class,
  NULLIF(s_market_desc, '') AS s_market_desc,
  NULLIF(s_market_manager, '') AS s_market_manager,
  CAST(NULLIF(s_division_id, '') AS INT) AS s_division_id,
  NULLIF(s_division_name, '') AS s_division_name,
  CAST(NULLIF(s_company_id, '') AS INT) AS s_company_id,
  NULLIF(s_company_name, '') AS s_company_name,
  NULLIF(s_street_number, '') AS s_street_number,
  NULLIF(s_street_name, '') AS s_street_name,
  NULLIF(s_street_type, '') AS s_street_type,
  NULLIF(s_suite_number, '') AS s_suite_number,
  NULLIF(s_city, '') AS s_city,
  NULLIF(s_county, '') AS s_county,
  NULLIF(s_state, '') AS s_state,
  NULLIF(s_zip, '') AS s_zip,
  NULLIF(s_country, '') AS s_country,
  CAST(NULLIF(s_gmt_offset, '') AS DECIMAL(5,2)) AS s_gmt_offset,
  CAST(NULLIF(s_tax_precentage, '') AS DECIMAL(5,2)) AS s_tax_precentage
FROM hive.tpcds_source.store;


INSERT INTO iceberg.tpcds_target.store_returns
SELECT
  CAST(NULLIF(sr_returned_date_sk, '') AS INT) AS sr_returned_date_sk,
  CAST(NULLIF(sr_return_time_sk, '') AS INT) AS sr_return_time_sk,
  CAST(NULLIF(sr_item_sk, '') AS INT) AS sr_item_sk,
  CAST(NULLIF(sr_customer_sk, '') AS INT) AS sr_customer_sk,
  CAST(NULLIF(sr_cdemo_sk, '') AS INT) AS sr_cdemo_sk,
  CAST(NULLIF(sr_hdemo_sk, '') AS INT) AS sr_hdemo_sk,
  CAST(NULLIF(sr_addr_sk, '') AS INT) AS sr_addr_sk,
  CAST(NULLIF(sr_store_sk, '') AS INT) AS sr_store_sk,
  CAST(NULLIF(sr_reason_sk, '') AS INT) AS sr_reason_sk,
  CAST(NULLIF(sr_ticket_number, '') AS BIGINT) AS sr_ticket_number,
  CAST(NULLIF(sr_return_quantity, '') AS INT) AS sr_return_quantity,
  CAST(NULLIF(sr_return_amt, '') AS DECIMAL(7,2)) AS sr_return_amt,
  CAST(NULLIF(sr_return_tax, '') AS DECIMAL(7,2)) AS sr_return_tax,
  CAST(NULLIF(sr_return_amt_inc_tax, '') AS DECIMAL(7,2)) AS sr_return_amt_inc_tax,
  CAST(NULLIF(sr_fee, '') AS DECIMAL(7,2)) AS sr_fee,
  CAST(NULLIF(sr_return_ship_cost, '') AS DECIMAL(7,2)) AS sr_return_ship_cost,
  CAST(NULLIF(sr_refunded_cash, '') AS DECIMAL(7,2)) AS sr_refunded_cash,
  CAST(NULLIF(sr_reversed_charge, '') AS DECIMAL(7,2)) AS sr_reversed_charge,
  CAST(NULLIF(sr_store_credit, '') AS DECIMAL(7,2)) AS sr_store_credit,
  CAST(NULLIF(sr_net_loss, '') AS DECIMAL(7,2)) AS sr_net_loss
FROM hive.tpcds_source.store_returns;


INSERT INTO iceberg.tpcds_target.store_sales
SELECT
  CAST(NULLIF(ss_sold_date_sk, '') AS INT) AS ss_sold_date_sk,
  CAST(NULLIF(ss_sold_time_sk, '') AS INT) AS ss_sold_time_sk,
  CAST(NULLIF(ss_item_sk, '') AS INT) AS ss_item_sk,
  CAST(NULLIF(ss_customer_sk, '') AS INT) AS ss_customer_sk,
  CAST(NULLIF(ss_cdemo_sk, '') AS INT) AS ss_cdemo_sk,
  CAST(NULLIF(ss_hdemo_sk, '') AS INT) AS ss_hdemo_sk,
  CAST(NULLIF(ss_addr_sk, '') AS INT) AS ss_addr_sk,
  CAST(NULLIF(ss_store_sk, '') AS INT) AS ss_store_sk,
  CAST(NULLIF(ss_promo_sk, '') AS INT) AS ss_promo_sk,
  CAST(NULLIF(ss_ticket_number, '') AS BIGINT) AS ss_ticket_number,
  CAST(NULLIF(ss_quantity, '') AS INT) AS ss_quantity,
  CAST(NULLIF(ss_wholesale_cost, '') AS DECIMAL(7,2)) AS ss_wholesale_cost,
  CAST(NULLIF(ss_list_price, '') AS DECIMAL(7,2)) AS ss_list_price,
  CAST(NULLIF(ss_sales_price, '') AS DECIMAL(7,2)) AS ss_sales_price,
  CAST(NULLIF(ss_ext_discount_amt, '') AS DECIMAL(7,2)) AS ss_ext_discount_amt,
  CAST(NULLIF(ss_ext_sales_price, '') AS DECIMAL(7,2)) AS ss_ext_sales_price,
  CAST(NULLIF(ss_ext_wholesale_cost, '') AS DECIMAL(7,2)) AS ss_ext_wholesale_cost,
  CAST(NULLIF(ss_ext_list_price, '') AS DECIMAL(7,2)) AS ss_ext_list_price,
  CAST(NULLIF(ss_ext_tax, '') AS DECIMAL(7,2)) AS ss_ext_tax,
  CAST(NULLIF(ss_coupon_amt, '') AS DECIMAL(7,2)) AS ss_coupon_amt,
  CAST(NULLIF(ss_net_paid, '') AS DECIMAL(7,2)) AS ss_net_paid,
  CAST(NULLIF(ss_net_paid_inc_tax, '') AS DECIMAL(7,2)) AS ss_net_paid_inc_tax,
  CAST(NULLIF(ss_net_profit, '') AS DECIMAL(7,2)) AS ss_net_profit
FROM hive.tpcds_source.store_sales;


INSERT INTO iceberg.tpcds_target.time_dim
SELECT
  CAST(NULLIF(t_time_sk, '') AS INT) AS t_time_sk,
  NULLIF(t_time_id, '') AS t_time_id,
  CAST(NULLIF(t_time, '') AS INT) AS t_time,
  CAST(NULLIF(t_hour, '') AS INT) AS t_hour,
  CAST(NULLIF(t_minute, '') AS INT) AS t_minute,
  CAST(NULLIF(t_second, '') AS INT) AS t_second,
  NULLIF(t_am_pm, '') AS t_am_pm,
  NULLIF(t_shift, '') AS t_shift,
  NULLIF(t_sub_shift, '') AS t_sub_shift,
  NULLIF(t_meal_time, '') AS t_meal_time
FROM hive.tpcds_source.time_dim;


INSERT INTO iceberg.tpcds_target.warehouse
SELECT
  CAST(NULLIF(w_warehouse_sk, '') AS INT) AS w_warehouse_sk,
  NULLIF(w_warehouse_id, '') AS w_warehouse_id,
  NULLIF(w_warehouse_name, '') AS w_warehouse_name,
  CAST(NULLIF(w_warehouse_sq_ft, '') AS INT) AS w_warehouse_sq_ft,
  NULLIF(w_street_number, '') AS w_street_number,
  NULLIF(w_street_name, '') AS w_street_name,
  NULLIF(w_street_type, '') AS w_street_type,
  NULLIF(w_suite_number, '') AS w_suite_number,
  NULLIF(w_city, '') AS w_city,
  NULLIF(w_county, '') AS w_county,
  NULLIF(w_state, '') AS w_state,
  NULLIF(w_zip, '') AS w_zip,
  NULLIF(w_country, '') AS w_country,
  CAST(NULLIF(w_gmt_offset, '') AS DECIMAL(5,2)) AS w_gmt_offset
FROM hive.tpcds_source.warehouse;


INSERT INTO iceberg.tpcds_target.web_page
SELECT
  CAST(NULLIF(wp_web_page_sk, '') AS INT) AS wp_web_page_sk,
  NULLIF(wp_web_page_id, '') AS wp_web_page_id,
  CAST(NULLIF(wp_rec_start_date, '') AS DATE) AS wp_rec_start_date,
  CAST(NULLIF(wp_rec_end_date, '') AS DATE) AS wp_rec_end_date,
  CAST(NULLIF(wp_creation_date_sk, '') AS INT) AS wp_creation_date_sk,
  CAST(NULLIF(wp_access_date_sk, '') AS INT) AS wp_access_date_sk,
  NULLIF(wp_autogen_flag, '') AS wp_autogen_flag,
  CAST(NULLIF(wp_customer_sk, '') AS INT) AS wp_customer_sk,
  NULLIF(wp_url, '') AS wp_url,
  NULLIF(wp_type, '') AS wp_type,
  CAST(NULLIF(wp_char_count, '') AS INT) AS wp_char_count,
  CAST(NULLIF(wp_link_count, '') AS INT) AS wp_link_count,
  CAST(NULLIF(wp_image_count, '') AS INT) AS wp_image_count,
  CAST(NULLIF(wp_max_ad_count, '') AS INT) AS wp_max_ad_count
FROM hive.tpcds_source.web_page;


INSERT INTO iceberg.tpcds_target.web_returns
SELECT
  CAST(NULLIF(wr_returned_date_sk, '') AS INT) AS wr_returned_date_sk,
  CAST(NULLIF(wr_returned_time_sk, '') AS INT) AS wr_returned_time_sk,
  CAST(NULLIF(wr_item_sk, '') AS INT) AS wr_item_sk,
  CAST(NULLIF(wr_refunded_customer_sk, '') AS INT) AS wr_refunded_customer_sk,
  CAST(NULLIF(wr_refunded_cdemo_sk, '') AS INT) AS wr_refunded_cdemo_sk,
  CAST(NULLIF(wr_refunded_hdemo_sk, '') AS INT) AS wr_refunded_hdemo_sk,
  CAST(NULLIF(wr_refunded_addr_sk, '') AS INT) AS wr_refunded_addr_sk,
  CAST(NULLIF(wr_returning_customer_sk, '') AS INT) AS wr_returning_customer_sk,
  CAST(NULLIF(wr_returning_cdemo_sk, '') AS INT) AS wr_returning_cdemo_sk,
  CAST(NULLIF(wr_returning_hdemo_sk, '') AS INT) AS wr_returning_hdemo_sk,
  CAST(NULLIF(wr_returning_addr_sk, '') AS INT) AS wr_returning_addr_sk,
  CAST(NULLIF(wr_web_page_sk, '') AS INT) AS wr_web_page_sk,
  CAST(NULLIF(wr_reason_sk, '') AS INT) AS wr_reason_sk,
  CAST(NULLIF(wr_order_number, '') AS BIGINT) AS wr_order_number,
  CAST(NULLIF(wr_return_quantity, '') AS INT) AS wr_return_quantity,
  CAST(NULLIF(wr_return_amt, '') AS DECIMAL(7,2)) AS wr_return_amt,
  CAST(NULLIF(wr_return_tax, '') AS DECIMAL(7,2)) AS wr_return_tax,
  CAST(NULLIF(wr_return_amt_inc_tax, '') AS DECIMAL(7,2)) AS wr_return_amt_inc_tax,
  CAST(NULLIF(wr_fee, '') AS DECIMAL(7,2)) AS wr_fee,
  CAST(NULLIF(wr_return_ship_cost, '') AS DECIMAL(7,2)) AS wr_return_ship_cost,
  CAST(NULLIF(wr_refunded_cash, '') AS DECIMAL(7,2)) AS wr_refunded_cash,
  CAST(NULLIF(wr_reversed_charge, '') AS DECIMAL(7,2)) AS wr_reversed_charge,
  CAST(NULLIF(wr_account_credit, '') AS DECIMAL(7,2)) AS wr_account_credit,
  CAST(NULLIF(wr_net_loss, '') AS DECIMAL(7,2)) AS wr_net_loss
FROM hive.tpcds_source.web_returns;


INSERT INTO iceberg.tpcds_target.web_sales
SELECT
  CAST(NULLIF(ws_sold_date_sk, '') AS INT) AS ws_sold_date_sk,
  CAST(NULLIF(ws_sold_time_sk, '') AS INT) AS ws_sold_time_sk,
  CAST(NULLIF(ws_ship_date_sk, '') AS INT) AS ws_ship_date_sk,
  CAST(NULLIF(ws_item_sk, '') AS INT) AS ws_item_sk,
  CAST(NULLIF(ws_bill_customer_sk, '') AS INT) AS ws_bill_customer_sk,
  CAST(NULLIF(ws_bill_cdemo_sk, '') AS INT) AS ws_bill_cdemo_sk,
  CAST(NULLIF(ws_bill_hdemo_sk, '') AS INT) AS ws_bill_hdemo_sk,
  CAST(NULLIF(ws_bill_addr_sk, '') AS INT) AS ws_bill_addr_sk,
  CAST(NULLIF(ws_ship_customer_sk, '') AS INT) AS ws_ship_customer_sk,
  CAST(NULLIF(ws_ship_cdemo_sk, '') AS INT) AS ws_ship_cdemo_sk,
  CAST(NULLIF(ws_ship_hdemo_sk, '') AS INT) AS ws_ship_hdemo_sk,
  CAST(NULLIF(ws_ship_addr_sk, '') AS INT) AS ws_ship_addr_sk,
  CAST(NULLIF(ws_web_page_sk, '') AS INT) AS ws_web_page_sk,
  CAST(NULLIF(ws_web_site_sk, '') AS INT) AS ws_web_site_sk,
  CAST(NULLIF(ws_ship_mode_sk, '') AS INT) AS ws_ship_mode_sk,
  CAST(NULLIF(ws_warehouse_sk, '') AS INT) AS ws_warehouse_sk,
  CAST(NULLIF(ws_promo_sk, '') AS INT) AS ws_promo_sk,
  CAST(NULLIF(ws_order_number, '') AS BIGINT) AS ws_order_number,
  CAST(NULLIF(ws_quantity, '') AS INT) AS ws_quantity,
  CAST(NULLIF(ws_wholesale_cost, '') AS DECIMAL(7,2)) AS ws_wholesale_cost,
  CAST(NULLIF(ws_list_price, '') AS DECIMAL(7,2)) AS ws_list_price,
  CAST(NULLIF(ws_sales_price, '') AS DECIMAL(7,2)) AS ws_sales_price,
  CAST(NULLIF(ws_ext_discount_amt, '') AS DECIMAL(7,2)) AS ws_ext_discount_amt,
  CAST(NULLIF(ws_ext_sales_price, '') AS DECIMAL(7,2)) AS ws_ext_sales_price,
  CAST(NULLIF(ws_ext_wholesale_cost, '') AS DECIMAL(7,2)) AS ws_ext_wholesale_cost,
  CAST(NULLIF(ws_ext_list_price, '') AS DECIMAL(7,2)) AS ws_ext_list_price,
  CAST(NULLIF(ws_ext_tax, '') AS DECIMAL(7,2)) AS ws_ext_tax,
  CAST(NULLIF(ws_coupon_amt, '') AS DECIMAL(7,2)) AS ws_coupon_amt,
  CAST(NULLIF(ws_ext_ship_cost, '') AS DECIMAL(7,2)) AS ws_ext_ship_cost,
  CAST(NULLIF(ws_net_paid, '') AS DECIMAL(7,2)) AS ws_net_paid,
  CAST(NULLIF(ws_net_paid_inc_tax, '') AS DECIMAL(7,2)) AS ws_net_paid_inc_tax,
  CAST(NULLIF(ws_net_paid_inc_ship, '') AS DECIMAL(7,2)) AS ws_net_paid_inc_ship,
  CAST(NULLIF(ws_net_paid_inc_ship_tax, '') AS DECIMAL(7,2)) AS ws_net_paid_inc_ship_tax,
  CAST(NULLIF(ws_net_profit, '') AS DECIMAL(7,2)) AS ws_net_profit
FROM hive.tpcds_source.web_sales;


INSERT INTO iceberg.tpcds_target.web_site
SELECT
  CAST(NULLIF(web_site_sk, '') AS INT) AS web_site_sk,
  NULLIF(web_site_id, '') AS web_site_id,
  CAST(NULLIF(web_rec_start_date, '') AS DATE) AS web_rec_start_date,
  CAST(NULLIF(web_rec_end_date, '') AS DATE) AS web_rec_end_date,
  NULLIF(web_name, '') AS web_name,
  CAST(NULLIF(web_open_date_sk, '') AS INT) AS web_open_date_sk,
  CAST(NULLIF(web_close_date_sk, '') AS INT) AS web_close_date_sk,
  NULLIF(web_class, '') AS web_class,
  NULLIF(web_manager, '') AS web_manager,
  CAST(NULLIF(web_mkt_id, '') AS INT) AS web_mkt_id,
  NULLIF(web_mkt_class, '') AS web_mkt_class,
  NULLIF(web_mkt_desc, '') AS web_mkt_desc,
  NULLIF(web_market_manager, '') AS web_market_manager,
  CAST(NULLIF(web_company_id, '') AS INT) AS web_company_id,
  NULLIF(web_company_name, '') AS web_company_name,
  NULLIF(web_street_number, '') AS web_street_number,
  NULLIF(web_street_name, '') AS web_street_name,
  NULLIF(web_street_type, '') AS web_street_type,
  NULLIF(web_suite_number, '') AS web_suite_number,
  NULLIF(web_city, '') AS web_city,
  NULLIF(web_county, '') AS web_county,
  NULLIF(web_state, '') AS web_state,
  NULLIF(web_zip, '') AS web_zip,
  NULLIF(web_country, '') AS web_country,
  CAST(NULLIF(web_gmt_offset, '') AS DECIMAL(5,2)) AS web_gmt_offset,
  CAST(NULLIF(web_tax_percentage, '') AS DECIMAL(5,2)) AS web_tax_percentage
FROM hive.tpcds_source.web_site;

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
