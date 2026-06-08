
CREATE SCHEMA IF NOT EXISTS hive.tpcds_source
WITH (
  location = 's3a://test-bucket/source/'
);

USE hive.tpcds_source;


CREATE TABLE IF NOT EXISTS call_center (
  cc_call_center_sk varchar,
  cc_call_center_id varchar,
  cc_rec_start_date varchar,
  cc_rec_end_date varchar,
  cc_closed_date_sk varchar,
  cc_open_date_sk varchar,
  cc_name varchar,
  cc_class varchar,
  cc_employees varchar,
  cc_sq_ft varchar,
  cc_hours varchar,
  cc_manager varchar,
  cc_mkt_id varchar,
  cc_mkt_class varchar,
  cc_mkt_desc varchar,
  cc_market_manager varchar,
  cc_division varchar,
  cc_division_name varchar,
  cc_company varchar,
  cc_company_name varchar,
  cc_street_number varchar,
  cc_street_name varchar,
  cc_street_type varchar,
  cc_suite_number varchar,
  cc_city varchar,
  cc_county varchar,
  cc_state varchar,
  cc_zip varchar,
  cc_country varchar,
  cc_gmt_offset varchar,
  cc_tax_percentage varchar)
WITH (
  external_location = 's3a://test-bucket/source/call_center/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS catalog_page (
  cp_catalog_page_sk varchar,
  cp_catalog_page_id varchar,
  cp_start_date_sk varchar,
  cp_end_date_sk varchar,
  cp_department varchar,
  cp_catalog_number varchar,
  cp_catalog_page_number varchar,
  cp_description varchar,
  cp_type varchar)
WITH (
  external_location = 's3a://test-bucket/source/catalog_page/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS catalog_returns (
  cr_returned_date_sk varchar,
  cr_returned_time_sk varchar,
  cr_item_sk varchar,
  cr_refunded_customer_sk varchar,
  cr_refunded_cdemo_sk varchar,
  cr_refunded_hdemo_sk varchar,
  cr_refunded_addr_sk varchar,
  cr_returning_customer_sk varchar,
  cr_returning_cdemo_sk varchar,
  cr_returning_hdemo_sk varchar,
  cr_returning_addr_sk varchar,
  cr_call_center_sk varchar,
  cr_catalog_page_sk varchar,
  cr_ship_mode_sk varchar,
  cr_warehouse_sk varchar,
  cr_reason_sk varchar,
  cr_order_number varchar,
  cr_return_quantity varchar,
  cr_return_amount varchar,
  cr_return_tax varchar,
  cr_return_amt_inc_tax varchar,
  cr_fee varchar,
  cr_return_ship_cost varchar,
  cr_refunded_cash varchar,
  cr_reversed_charge varchar,
  cr_store_credit varchar,
  cr_net_loss varchar)
WITH (
  external_location = 's3a://test-bucket/source/catalog_returns/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS catalog_sales (
  cs_sold_date_sk varchar,
  cs_sold_time_sk varchar,
  cs_ship_date_sk varchar,
  cs_bill_customer_sk varchar,
  cs_bill_cdemo_sk varchar,
  cs_bill_hdemo_sk varchar,
  cs_bill_addr_sk varchar,
  cs_ship_customer_sk varchar,
  cs_ship_cdemo_sk varchar,
  cs_ship_hdemo_sk varchar,
  cs_ship_addr_sk varchar,
  cs_call_center_sk varchar,
  cs_catalog_page_sk varchar,
  cs_ship_mode_sk varchar,
  cs_warehouse_sk varchar,
  cs_item_sk varchar,
  cs_promo_sk varchar,
  cs_order_number varchar,
  cs_quantity varchar,
  cs_wholesale_cost varchar,
  cs_list_price varchar,
  cs_sales_price varchar,
  cs_ext_discount_amt varchar,
  cs_ext_sales_price varchar,
  cs_ext_wholesale_cost varchar,
  cs_ext_list_price varchar,
  cs_ext_tax varchar,
  cs_coupon_amt varchar,
  cs_ext_ship_cost varchar,
  cs_net_paid varchar,
  cs_net_paid_inc_tax varchar,
  cs_net_paid_inc_ship varchar,
  cs_net_paid_inc_ship_tax varchar,
  cs_net_profit varchar)
WITH (
  external_location = 's3a://test-bucket/source/catalog_sales/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS customer (
  c_customer_sk varchar,
  c_customer_id varchar,
  c_current_cdemo_sk varchar,
  c_current_hdemo_sk varchar,
  c_current_addr_sk varchar,
  c_first_shipto_date_sk varchar,
  c_first_sales_date_sk varchar,
  c_salutation varchar,
  c_first_name varchar,
  c_last_name varchar,
  c_preferred_cust_flag varchar,
  c_birth_day varchar,
  c_birth_month varchar,
  c_birth_year varchar,
  c_birth_country varchar,
  c_login varchar,
  c_email_address varchar,
  c_last_review_date_sk varchar)
WITH (
  external_location = 's3a://test-bucket/source/customer/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS customer_address (
  ca_address_sk varchar,
  ca_address_id varchar,
  ca_street_number varchar,
  ca_street_name varchar,
  ca_street_type varchar,
  ca_suite_number varchar,
  ca_city varchar,
  ca_county varchar,
  ca_state varchar,
  ca_zip varchar,
  ca_country varchar,
  ca_gmt_offset varchar,
  ca_location_type varchar)
WITH (
  external_location = 's3a://test-bucket/source/customer_address/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS customer_demographics (
  cd_demo_sk varchar,
  cd_gender varchar,
  cd_marital_status varchar,
  cd_education_status varchar,
  cd_purchase_estimate varchar,
  cd_credit_rating varchar,
  cd_dep_count varchar,
  cd_dep_employed_count varchar,
  cd_dep_college_count varchar)
WITH (
  external_location = 's3a://test-bucket/source/customer_demographics/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS date_dim (
  d_date_sk varchar,
  d_date_id varchar,
  d_date varchar,
  d_month_seq varchar,
  d_week_seq varchar,
  d_quarter_seq varchar,
  d_year varchar,
  d_dow varchar,
  d_moy varchar,
  d_dom varchar,
  d_qoy varchar,
  d_fy_year varchar,
  d_fy_quarter_seq varchar,
  d_fy_week_seq varchar,
  d_day_name varchar,
  d_quarter_name varchar,
  d_holiday varchar,
  d_weekend varchar,
  d_following_holiday varchar,
  d_first_dom varchar,
  d_last_dom varchar,
  d_same_day_ly varchar,
  d_same_day_lq varchar,
  d_current_day varchar,
  d_current_week varchar,
  d_current_month varchar,
  d_current_quarter varchar,
  d_current_year varchar)
WITH (
  external_location = 's3a://test-bucket/source/date_dim/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS household_demographics (
  hd_demo_sk varchar,
  hd_income_band_sk varchar,
  hd_buy_potential varchar,
  hd_dep_count varchar,
  hd_vehicle_count varchar)
WITH (
  external_location = 's3a://test-bucket/source/household_demographics/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS income_band (
  ib_income_band_sk varchar,
  ib_lower_bound varchar,
  ib_upper_bound varchar)
WITH (
  external_location = 's3a://test-bucket/source/income_band/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS inventory (
  inv_date_sk varchar,
  inv_item_sk varchar,
  inv_warehouse_sk varchar,
  inv_quantity_on_hand varchar)
WITH (
  external_location = 's3a://test-bucket/source/inventory/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS item (
  i_item_sk varchar,
  i_item_id varchar,
  i_rec_start_date varchar,
  i_rec_end_date varchar,
  i_item_desc varchar,
  i_current_price varchar,
  i_wholesale_cost varchar,
  i_brand_id varchar,
  i_brand varchar,
  i_class_id varchar,
  i_class varchar,
  i_category_id varchar,
  i_category varchar,
  i_manufact_id varchar,
  i_manufact varchar,
  i_size varchar,
  i_formulation varchar,
  i_color varchar,
  i_units varchar,
  i_container varchar,
  i_manager_id varchar,
  i_product_name varchar)
WITH (
  external_location = 's3a://test-bucket/source/item/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS promotion (
  p_promo_sk varchar,
  p_promo_id varchar,
  p_start_date_sk varchar,
  p_end_date_sk varchar,
  p_item_sk varchar,
  p_cost varchar,
  p_response_target varchar,
  p_promo_name varchar,
  p_channel_dmail varchar,
  p_channel_email varchar,
  p_channel_catalog varchar,
  p_channel_tv varchar,
  p_channel_radio varchar,
  p_channel_press varchar,
  p_channel_event varchar,
  p_channel_demo varchar,
  p_channel_details varchar,
  p_purpose varchar,
  p_discount_active varchar)
WITH (
  external_location = 's3a://test-bucket/source/promotion/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS reason (
  r_reason_sk varchar,
  r_reason_id varchar,
  r_reason_desc varchar)
WITH (
  external_location = 's3a://test-bucket/source/reason/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS ship_mode (
  sm_ship_mode_sk varchar,
  sm_ship_mode_id varchar,
  sm_type varchar,
  sm_code varchar,
  sm_carrier varchar,
  sm_contract varchar)
WITH (
  external_location = 's3a://test-bucket/source/ship_mode/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS store (
  s_store_sk varchar,
  s_store_id varchar,
  s_rec_start_date varchar,
  s_rec_end_date varchar,
  s_closed_date_sk varchar,
  s_store_name varchar,
  s_number_employees varchar,
  s_floor_space varchar,
  s_hours varchar,
  s_manager varchar,
  s_market_id varchar,
  s_geography_class varchar,
  s_market_desc varchar,
  s_market_manager varchar,
  s_division_id varchar,
  s_division_name varchar,
  s_company_id varchar,
  s_company_name varchar,
  s_street_number varchar,
  s_street_name varchar,
  s_street_type varchar,
  s_suite_number varchar,
  s_city varchar,
  s_county varchar,
  s_state varchar,
  s_zip varchar,
  s_country varchar,
  s_gmt_offset varchar,
  s_tax_precentage varchar)
WITH (
  external_location = 's3a://test-bucket/source/store/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS store_returns (
  sr_returned_date_sk varchar,
  sr_return_time_sk varchar,
  sr_item_sk varchar,
  sr_customer_sk varchar,
  sr_cdemo_sk varchar,
  sr_hdemo_sk varchar,
  sr_addr_sk varchar,
  sr_store_sk varchar,
  sr_reason_sk varchar,
  sr_ticket_number varchar,
  sr_return_quantity varchar,
  sr_return_amt varchar,
  sr_return_tax varchar,
  sr_return_amt_inc_tax varchar,
  sr_fee varchar,
  sr_return_ship_cost varchar,
  sr_refunded_cash varchar,
  sr_reversed_charge varchar,
  sr_store_credit varchar,
  sr_net_loss varchar)
WITH (
  external_location = 's3a://test-bucket/source/store_returns/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS store_sales (
  ss_sold_date_sk varchar,
  ss_sold_time_sk varchar,
  ss_item_sk varchar,
  ss_customer_sk varchar,
  ss_cdemo_sk varchar,
  ss_hdemo_sk varchar,
  ss_addr_sk varchar,
  ss_store_sk varchar,
  ss_promo_sk varchar,
  ss_ticket_number varchar,
  ss_quantity varchar,
  ss_wholesale_cost varchar,
  ss_list_price varchar,
  ss_sales_price varchar,
  ss_ext_discount_amt varchar,
  ss_ext_sales_price varchar,
  ss_ext_wholesale_cost varchar,
  ss_ext_list_price varchar,
  ss_ext_tax varchar,
  ss_coupon_amt varchar,
  ss_net_paid varchar,
  ss_net_paid_inc_tax varchar,
  ss_net_profit varchar)
WITH (
  external_location = 's3a://test-bucket/source/store_sales/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS time_dim (
  t_time_sk varchar,
  t_time_id varchar,
  t_time varchar,
  t_hour varchar,
  t_minute varchar,
  t_second varchar,
  t_am_pm varchar,
  t_shift varchar,
  t_sub_shift varchar,
  t_meal_time varchar)
WITH (
  external_location = 's3a://test-bucket/source/time_dim/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS warehouse (
  w_warehouse_sk varchar,
  w_warehouse_id varchar,
  w_warehouse_name varchar,
  w_warehouse_sq_ft varchar,
  w_street_number varchar,
  w_street_name varchar,
  w_street_type varchar,
  w_suite_number varchar,
  w_city varchar,
  w_county varchar,
  w_state varchar,
  w_zip varchar,
  w_country varchar,
  w_gmt_offset varchar)
WITH (
  external_location = 's3a://test-bucket/source/warehouse/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS web_page (
  wp_web_page_sk varchar,
  wp_web_page_id varchar,
  wp_rec_start_date varchar,
  wp_rec_end_date varchar,
  wp_creation_date_sk varchar,
  wp_access_date_sk varchar,
  wp_autogen_flag varchar,
  wp_customer_sk varchar,
  wp_url varchar,
  wp_type varchar,
  wp_char_count varchar,
  wp_link_count varchar,
  wp_image_count varchar,
  wp_max_ad_count varchar)
WITH (
  external_location = 's3a://test-bucket/source/web_page/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS web_returns (
  wr_returned_date_sk varchar,
  wr_returned_time_sk varchar,
  wr_item_sk varchar,
  wr_refunded_customer_sk varchar,
  wr_refunded_cdemo_sk varchar,
  wr_refunded_hdemo_sk varchar,
  wr_refunded_addr_sk varchar,
  wr_returning_customer_sk varchar,
  wr_returning_cdemo_sk varchar,
  wr_returning_hdemo_sk varchar,
  wr_returning_addr_sk varchar,
  wr_web_page_sk varchar,
  wr_reason_sk varchar,
  wr_order_number varchar,
  wr_return_quantity varchar,
  wr_return_amt varchar,
  wr_return_tax varchar,
  wr_return_amt_inc_tax varchar,
  wr_fee varchar,
  wr_return_ship_cost varchar,
  wr_refunded_cash varchar,
  wr_reversed_charge varchar,
  wr_account_credit varchar,
  wr_net_loss varchar)
WITH (
  external_location = 's3a://test-bucket/source/web_returns/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS web_sales (
  ws_sold_date_sk varchar,
  ws_sold_time_sk varchar,
  ws_ship_date_sk varchar,
  ws_item_sk varchar,
  ws_bill_customer_sk varchar,
  ws_bill_cdemo_sk varchar,
  ws_bill_hdemo_sk varchar,
  ws_bill_addr_sk varchar,
  ws_ship_customer_sk varchar,
  ws_ship_cdemo_sk varchar,
  ws_ship_hdemo_sk varchar,
  ws_ship_addr_sk varchar,
  ws_web_page_sk varchar,
  ws_web_site_sk varchar,
  ws_ship_mode_sk varchar,
  ws_warehouse_sk varchar,
  ws_promo_sk varchar,
  ws_order_number varchar,
  ws_quantity varchar,
  ws_wholesale_cost varchar,
  ws_list_price varchar,
  ws_sales_price varchar,
  ws_ext_discount_amt varchar,
  ws_ext_sales_price varchar,
  ws_ext_wholesale_cost varchar,
  ws_ext_list_price varchar,
  ws_ext_tax varchar,
  ws_coupon_amt varchar,
  ws_ext_ship_cost varchar,
  ws_net_paid varchar,
  ws_net_paid_inc_tax varchar,
  ws_net_paid_inc_ship varchar,
  ws_net_paid_inc_ship_tax varchar,
  ws_net_profit varchar)
WITH (
  external_location = 's3a://test-bucket/source/web_sales/',
  format = 'CSV',
  csv_separator = '|'
);

CREATE TABLE IF NOT EXISTS web_site (
  web_site_sk varchar,
  web_site_id varchar,
  web_rec_start_date varchar,
  web_rec_end_date varchar,
  web_name varchar,
  web_open_date_sk varchar,
  web_close_date_sk varchar,
  web_class varchar,
  web_manager varchar,
  web_mkt_id varchar,
  web_mkt_class varchar,
  web_mkt_desc varchar,
  web_market_manager varchar,
  web_company_id varchar,
  web_company_name varchar,
  web_street_number varchar,
  web_street_name varchar,
  web_street_type varchar,
  web_suite_number varchar,
  web_city varchar,
  web_county varchar,
  web_state varchar,
  web_zip varchar,
  web_country varchar,
  web_gmt_offset varchar,
  web_tax_percentage varchar)
WITH (
  external_location = 's3a://test-bucket/source/web_site/',
  format = 'CSV',
  csv_separator = '|'
);
