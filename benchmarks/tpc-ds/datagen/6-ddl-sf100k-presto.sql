CREATE SCHEMA IF NOT EXISTS tpcds_sf100000_parquet_varchar_part
WITH (
  location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/'
);

USE hive.tpcds_sf100000_parquet_varchar_part;

CREATE TABLE IF NOT EXISTS call_center (
  cc_call_center_sk INT, -- identifier not null primary key
  cc_call_center_id VARCHAR(16), -- char(16) not null
  cc_rec_start_date DATE,
  cc_rec_end_date DATE,
  cc_closed_date_sk INT, -- identifier foreign key d_date_sk
  cc_open_date_sk INT, -- identifier foreign key d_date_sk
  cc_name VARCHAR(50),
  cc_class VARCHAR(50),
  cc_employees INT,
  cc_sq_ft INT,
  cc_hours VARCHAR(20), -- char(20)
  cc_manager VARCHAR(40),
  cc_mkt_id INT,
  cc_mkt_class VARCHAR(50), -- char(50)
  cc_mkt_desc VARCHAR(100),
  cc_market_manager VARCHAR(40),
  cc_division INT,
  cc_division_name VARCHAR(50),
  cc_company INT,
  cc_company_name VARCHAR(50), -- char(50)
  cc_street_number VARCHAR(10), -- char(10)
  cc_street_name VARCHAR(60),
  cc_street_type VARCHAR(15), -- char(15)
  cc_suite_number VARCHAR(10), -- char(10)
  cc_city VARCHAR(60),
  cc_county VARCHAR(30),
  cc_state VARCHAR(2), -- char(2)
  cc_zip VARCHAR(10), -- char(10)
  cc_country VARCHAR(20),
  cc_gmt_offset DECIMAL(5,2),
  cc_tax_percentage DECIMAL(5,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/call_center/data'
);

CREATE TABLE IF NOT EXISTS catalog_page (
  cp_catalog_page_sk INT, -- identifier not null primary key
  cp_catalog_page_id VARCHAR(16), -- char(16) not null
  cp_start_date_sk INT, -- identifier foreign key d_date_sk
  cp_end_date_sk INT, -- identifier foreign key d_date_sk
  cp_department VARCHAR(50),
  cp_catalog_number INT,
  cp_catalog_page_number INT,
  cp_description VARCHAR(100),
  cp_type VARCHAR(100))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/catalog_page/data'
);

CREATE TABLE IF NOT EXISTS catalog_returns (
  cr_returned_date_sk INT, -- identifier foreign key d_date_sk
  cr_returned_time_sk INT,  -- identifier foreign key t_time_sk
  cr_item_sk INT, -- identifier not null primary key foreign key i_item_sk,cs_item_sk
  cr_refunded_customer_sk INT,  -- identifier foreign key c_customer_sk
  cr_refunded_cdemo_sk INT,  -- identifier foreign key cd_demo_sk
  cr_refunded_hdemo_sk INT,  -- identifier foreign key hd_demo_sk
  cr_refunded_addr_sk INT,  -- identifier foreign key ca_address_sk
  cr_returning_customer_sk INT,  -- identifier foreign key c_customer_sk
  cr_returning_cdemo_sk INT,  -- identifier foreign key cd_demo_sk
  cr_returning_hdemo_sk INT,  -- identifier foreign key hd_demo_sk
  cr_returning_addr_sk INT,  -- identifier foreign key ca_address_sk
  cr_call_center_sk INT,  -- identifier foreign key cc_call_center_sk
  cr_catalog_page_sk INT,  -- identifier foreign key cp_catalog_page_sk
  cr_ship_mode_sk INT,  -- identifier foreign key sm_ship_mode_sk
  cr_warehouse_sk INT,  -- identifier foreign key w_warehouse_sk
  cr_reason_sk INT,  -- identifier foreign key r_reason_sk
  cr_order_number BIGINT, -- identifier not null primary key foreign key cs_order_number
  cr_return_quantity INT,
  cr_return_amount DECIMAL(7,2),
  cr_return_tax DECIMAL(7,2),
  cr_return_amt_inc_tax DECIMAL(7,2),
  cr_fee DECIMAL(7,2),
  cr_return_ship_cost DECIMAL(7,2),
  cr_refunded_cash DECIMAL(7,2),
  cr_reversed_charge DECIMAL(7,2),
  cr_store_credit DECIMAL(7,2),
  cr_net_loss DECIMAL(7,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/catalog_returns/data'
);

CREATE TABLE IF NOT EXISTS catalog_sales (
  cs_sold_time_sk INT, -- identifier foreign key t_time_sk
  cs_ship_date_sk INT, -- identifier foreign key d_date_sk
  cs_bill_customer_sk INT, -- identifier foreign key c_customer_sk
  cs_bill_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  cs_bill_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  cs_bill_addr_sk INT, -- identifier foreign key ca_address_sk
  cs_ship_customer_sk INT, -- identifier foreign key c_customer_sk
  cs_ship_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  cs_ship_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  cs_ship_addr_sk INT, -- identifier foreign key ca_address_sk
  cs_call_center_sk INT, -- identifier foreign key cc_call_center_sk
  cs_catalog_page_sk INT, -- identifier foreign key cp_catalog_page_sk
  cs_ship_mode_sk INT, -- identifier foreign key sm_ship_mode_sk
  cs_warehouse_sk INT, -- identifier foreign key w_warehouse_sk
  cs_item_sk INT, -- identifier not null primary key foreign key i_item_sk
  cs_promo_sk INT, -- identifier foreign key p_promo_sk
  cs_order_number BIGINT, -- identifier not null primary key
  cs_quantity INT,
  cs_wholesale_cost DECIMAL(7,2),
  cs_list_price DECIMAL(7,2),
  cs_sales_price DECIMAL(7,2),
  cs_ext_discount_amt DECIMAL(7,2),
  cs_ext_sales_price DECIMAL(7,2),
  cs_ext_wholesale_cost DECIMAL(7,2),
  cs_ext_list_price DECIMAL(7,2),
  cs_ext_tax DECIMAL(7,2),
  cs_coupon_amt DECIMAL(7,2),
  cs_ext_ship_cost DECIMAL(7,2),
  cs_net_paid DECIMAL(7,2),
  cs_net_paid_inc_tax DECIMAL(7,2),
  cs_net_paid_inc_ship DECIMAL(7,2),
  cs_net_paid_inc_ship_tax DECIMAL(7,2),
  cs_net_profit DECIMAL(7,2),
  cs_sold_date_sk INT) -- identifier foreign key d_date_sk
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/catalog_sales/data',
  partitioned_by = array['cs_sold_date_sk']
);

CREATE TABLE IF NOT EXISTS customer (
  c_customer_sk INT, -- identifier not null primary key
  c_customer_id VARCHAR(16), -- char(16) not null
  c_current_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  c_current_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  c_current_addr_sk INT, -- identifier foreign key ca_address_sk
  c_first_shipto_date_sk INT, -- identifier foreign key d_date_sk
  c_first_sales_date_sk INT, -- identifier foreign key d_date_sk
  c_salutation VARCHAR(10), -- char(10)
  c_first_name VARCHAR(20), -- char(20)
  c_last_name VARCHAR(30), -- char(30)
  c_preferred_cust_flag VARCHAR(1), -- char(1)
  c_birth_day INT,
  c_birth_month INT,
  c_birth_year INT,
  c_birth_country VARCHAR(20),
  c_login VARCHAR(13), -- char(13)
  c_email_address VARCHAR(50), -- char(50)
  c_last_review_date_sk INT) -- identifier foreign key d_date_sk
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/customer/data'
);

CREATE TABLE IF NOT EXISTS customer_address (
  ca_address_sk INT, -- identifier not null primary key
  ca_address_id VARCHAR(16), -- char(16) not null
  ca_street_number VARCHAR(10), -- char(10)
  ca_street_name VARCHAR(60),
  ca_street_type VARCHAR(15), -- char(15)
  ca_suite_number VARCHAR(10), -- char(10)
  ca_city VARCHAR(60),
  ca_county VARCHAR(30),
  ca_state VARCHAR(2), -- char(2)
  ca_zip VARCHAR(10), -- char(10)
  ca_country VARCHAR(20),
  ca_gmt_offset DECIMAL(5,2),
  ca_location_type VARCHAR(20)) -- char(20)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/customer_address/data'
);

CREATE TABLE IF NOT EXISTS customer_demographics (
  cd_demo_sk INT, -- identifier not null primary key
  cd_gender VARCHAR(1), -- char(1)
  cd_marital_status VARCHAR(1), -- char(1)
  cd_education_status VARCHAR(20), -- char(20)
  cd_purchase_estimate INT,
  cd_credit_rating VARCHAR(10), -- char(10)
  cd_dep_count INT,
  cd_dep_employed_count INT,
  cd_dep_college_count INT)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/customer_demographics/data'
);

CREATE TABLE IF NOT EXISTS date_dim (
  d_date_sk INT, -- identifier not null primary key
  d_date_id VARCHAR(16), -- char(16) not null
  d_date DATE, -- not null
  d_month_seq INT,
  d_week_seq INT,
  d_quarter_seq INT,
  d_year INT,
  d_dow INT,
  d_moy INT,
  d_dom INT,
  d_qoy INT,
  d_fy_year INT,
  d_fy_quarter_seq INT,
  d_fy_week_seq INT,
  d_day_name VARCHAR(9), -- char(9)
  d_quarter_name VARCHAR(6), -- char(6)
  d_holiday VARCHAR(1), -- char(1)
  d_weekend VARCHAR(1), -- char(1)
  d_following_holiday VARCHAR(1),
  d_first_dom INT,
  d_last_dom INT,
  d_same_day_ly INT,
  d_same_day_lq INT,
  d_current_day VARCHAR(1), -- char(1)
  d_current_week VARCHAR(1), -- char(1)
  d_current_month VARCHAR(1), -- char(1)
  d_current_quarter VARCHAR(1), -- char(1)
  d_current_year VARCHAR(1)) -- char(1)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/date_dim/data'
);

CREATE TABLE IF NOT EXISTS household_demographics (
  hd_demo_sk INT, -- identifier not null primary key
  hd_income_band_sk INT, -- identifier foreign key ib_income_band_sk
  hd_buy_potential VARCHAR(15), -- char(15)
  hd_dep_count INT,
  hd_vehicle_count INT)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/household_demographics/data'
);

CREATE TABLE IF NOT EXISTS income_band (
  ib_income_band_sk INT, -- identifier not null primary key
  ib_lower_bound INT,
  ib_upper_bound INT)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/income_band/data'
);

CREATE TABLE IF NOT EXISTS inventory (
  inv_item_sk INT, -- identifier not null primary key foreign key i_item_sk
  inv_warehouse_sk INT, -- identifier not null primary key foreign key w_warehouse_sk
  inv_quantity_on_hand INT,
  inv_date_sk INT) -- identifier not null primary key foreign key d_date_sk
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/inventory/data',
  partitioned_by = array['inv_date_sk']
);

CREATE TABLE IF NOT EXISTS item (
  i_item_sk INT, -- identifier not null primary key
  i_item_id VARCHAR(16), -- char(16) not null
  i_rec_start_date DATE,
  i_rec_end_date DATE,
  i_item_desc VARCHAR(200),
  i_current_price DECIMAL(7,2),
  i_wholesale_cost DECIMAL(7,2),
  i_brand_id INT,
  i_brand VARCHAR(50), -- char(50)
  i_class_id INT,
  i_class VARCHAR(50), -- char(50)
  i_category_id INT,
  i_category VARCHAR(50), -- char(50)
  i_manufact_id INT,
  i_manufact VARCHAR(50), -- char(50)
  i_size VARCHAR(20), -- char(20)
  i_formulation VARCHAR(20), -- char(20)
  i_color VARCHAR(20), -- char(20)
  i_units VARCHAR(10), -- char(10)
  i_container VARCHAR(10), -- char(10)
  i_manager_id INT,
  i_product_name VARCHAR(50)) -- char(50)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/item/data'
);

CREATE TABLE IF NOT EXISTS promotion (
  p_promo_sk INT, -- identifier not null primary key
  p_promo_id VARCHAR(16), -- char(16) not null
  p_start_date_sk INT, -- identifier foreign key d_date_sk
  p_end_date_sk INT, -- identifier foreign key d_date_sk
  p_item_sk INT, -- identifier foreign key i_item_sk
  p_cost DECIMAL(15,2),
  p_response_targe INT,
  p_promo_name VARCHAR(50), -- char(50)
  p_channel_dmail VARCHAR(1), -- char(1)
  p_channel_email VARCHAR(1), -- char(1)
  p_channel_catalog VARCHAR(1), -- char(1)
  p_channel_tv VARCHAR(1), -- char(1)
  p_channel_radio VARCHAR(1), -- char(1)
  p_channel_press VARCHAR(1), -- char(1)
  p_channel_event VARCHAR(1), -- char(1)
  p_channel_demo VARCHAR(1), -- char(1)
  p_channel_details VARCHAR(100),
  p_purpose VARCHAR(15), -- char(15)
  p_discount_active VARCHAR(1)) -- char(1)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/promotion/data'
);

CREATE TABLE IF NOT EXISTS reason (
  r_reason_sk INT, -- identifier not null primary key
  r_reason_id VARCHAR(16), -- char(16) not null
  r_reason_desc VARCHAR(100)) -- char(100)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/reason/data'
);

CREATE TABLE IF NOT EXISTS ship_mode (
  sm_ship_mode_sk INT, -- identifier not null primary key
  sm_ship_mode_id VARCHAR(16), -- char(16) not null
  sm_type VARCHAR(30), -- char(30)
  sm_code VARCHAR(10), -- char(10)
  sm_carrier VARCHAR(20), -- char(20)
  sm_contract VARCHAR(20)) -- char(20)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/ship_mode/data'
);

CREATE TABLE IF NOT EXISTS store (
  s_store_sk INT, -- identifier not null primary key
  s_store_id VARCHAR(16), -- char(16) not null
  s_rec_start_date DATE,
  s_rec_end_date DATE,
  s_closed_date_sk INT, -- identifier foreign key d_date_sk
  s_store_name VARCHAR(50),
  s_number_employees INT,
  s_floor_space INT,
  s_hours VARCHAR(20), -- char(20)
  s_manager VARCHAR(40),
  s_market_id INT,
  s_geography_class VARCHAR(100),
  s_market_desc VARCHAR(100),
  s_market_manager VARCHAR(40),
  s_division_id INT,
  s_division_name VARCHAR(50),
  s_company_id INT,
  s_company_name VARCHAR(50),
  s_street_number VARCHAR(10),
  s_street_name VARCHAR(60),
  s_street_type VARCHAR(15), -- char(15)
  s_suite_number VARCHAR(10), -- char(10)
  s_city VARCHAR(60),
  s_county VARCHAR(30),
  s_state VARCHAR(2), -- char(2)
  s_zip VARCHAR(10), -- char(10)
  s_country VARCHAR(20),
  s_gmt_offset DECIMAL(5,2),
  s_tax_precentage DECIMAL(5,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store/data'
);

CREATE TABLE IF NOT EXISTS store_returns (
  sr_returned_date_sk INT, -- identifier foreign key d_date_sk
  sr_return_time_sk INT, -- identifier foreign key t_time_sk
  sr_item_sk INT, -- identifier not null primary key foreign key i_item_sk,ss_item_sk
  sr_customer_sk INT, -- identifier foreign key c_customer_sk
  sr_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  sr_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  sr_addr_sk INT, -- identifier foreign key ca_address_sk
  sr_store_sk INT, -- identifier foreign key s_store_sk
  sr_reason_sk INT, -- identifier foreign key r_reason_sk
  sr_ticket_number BIGINT, -- identifier not null primary key foreign key ss_ticket_number
  sr_return_quantity INT,
  sr_return_amt DECIMAL(7,2),
  sr_return_tax DECIMAL(7,2),
  sr_return_amt_inc_tax DECIMAL(7,2),
  sr_fee DECIMAL(7,2),
  sr_return_ship_cost DECIMAL(7,2),
  sr_refunded_cash DECIMAL(7,2),
  sr_reversed_charge DECIMAL(7,2),
  sr_store_credit DECIMAL(7,2),
  sr_net_loss DECIMAL(7,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store_returns/data'
);

CREATE TABLE IF NOT EXISTS store_sales (
  ss_sold_time_sk INT, -- identifier foreign key t_time_sk
  ss_item_sk INT, -- identifier not null primary key foreign key i_item_sk
  ss_customer_sk INT, -- identifier foreign key c_customer_sk
  ss_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  ss_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  ss_addr_sk INT, -- identifier foreign key ca_address_sk
  ss_store_sk INT, -- identifier foreign key s_store_sk
  ss_promo_sk INT, -- identifier foreign key p_promo_sk
  ss_ticket_number BIGINT, -- identifier not null primary key
  ss_quantity INT,
  ss_wholesale_cost DECIMAL(7,2),
  ss_list_price DECIMAL(7,2),
  ss_sales_price DECIMAL(7,2),
  ss_ext_discount_amt DECIMAL(7,2),
  ss_ext_sales_price DECIMAL(7,2),
  ss_ext_wholesale_cost DECIMAL(7,2),
  ss_ext_list_price DECIMAL(7,2),
  ss_ext_tax DECIMAL(7,2),
  ss_coupon_amt DECIMAL(7,2),
  ss_net_paid DECIMAL(7,2),
  ss_net_paid_inc_tax DECIMAL(7,2),
  ss_net_profit DECIMAL(7,2),
  ss_sold_date_sk INT) -- identifier foreign key d_date_sk
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store_sales/data',
  partitioned_by = array['ss_sold_date_sk']
);

CREATE TABLE IF NOT EXISTS time_dim (
  t_time_sk INT, -- identifier not null primary key
  t_time_id VARCHAR(16), -- char(16) not null
  t_time INT, -- not null
  t_hour INT,
  t_minute INT,
  t_second INT,
  t_am_pm VARCHAR(2), -- char(2)
  t_shift VARCHAR(20), -- char(20)
  t_sub_shift VARCHAR(20), -- char(20)
  t_meal_time VARCHAR(20)) -- char(20)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/time_dim/data'
);

CREATE TABLE IF NOT EXISTS warehouse (
  w_warehouse_sk INT, -- identifier not null primary key
  w_warehouse_id VARCHAR(16), -- char(16) not null
  w_warehouse_name VARCHAR(20),
  w_warehouse_sq_ft INT,
  w_street_number VARCHAR(10), -- char(10)
  w_street_name VARCHAR(60),
  w_street_type VARCHAR(15), -- char(15)
  w_suite_number VARCHAR(10), -- char(10)
  w_city VARCHAR(60),
  w_county VARCHAR(30),
  w_state VARCHAR(2), -- char(2)
  w_zip VARCHAR(10), -- char(10)
  w_country VARCHAR(20),
  w_gmt_offset DECIMAL(5,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/warehouse/data'
);

CREATE TABLE IF NOT EXISTS web_page (
  wp_web_page_sk INT, -- identifier not null primary key
  wp_web_page_id VARCHAR(16), -- char(16) not null
  wp_rec_start_date DATE,
  wp_rec_end_date DATE,
  wp_creation_date_sk INT, -- identifier foreign key d_date_sk
  wp_access_date_sk INT, -- identifier foreign key d_date_sk
  wp_autogen_flag VARCHAR(1), -- char(1)
  wp_customer_sk INT, -- identifier foreign key c_customer_sk
  wp_url VARCHAR(100),
  wp_type VARCHAR(50), -- char(50)
  wp_char_count INT,
  wp_link_count INT,
  wp_image_count INT,
  wp_max_ad_count INT)
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_page/data'
);

CREATE TABLE IF NOT EXISTS web_returns (
  wr_returned_date_sk INT, -- identifier foreign key d_date_sk
  wr_returned_time_sk INT, -- identifier foreign key t_time_sk
  wr_item_sk INT, -- identifier not null primary key foreign key i_item_sk,ss_item_sk
  wr_refunded_customer_sk INT, -- identifier foreign key c_customer_sk
  wr_refunded_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  wr_refunded_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  wr_refunded_addr_sk INT, -- identifier foreign key ca_address_sk
  wr_returning_customer_sk INT, -- identifier foreign key c_customer_sk
  wr_returning_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  wr_returning_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  wr_returning_addr_sk INT, -- identifier foreign key ca_address_sk
  wr_web_page_sk INT, -- identifier foreign key wp_web_page_sk
  wr_reason_sk INT, -- identifier foreign key r_reason_sk
  wr_order_number BIGINT, -- identifier not null primary key foreign key ws_order_number
  wr_return_quantity INT,
  wr_return_amt DECIMAL(7,2),
  wr_return_tax DECIMAL(7,2),
  wr_return_amt_inc_tax DECIMAL(7,2),
  wr_fee DECIMAL(7,2),
  wr_return_ship_cost DECIMAL(7,2),
  wr_refunded_cash DECIMAL(7,2),
  wr_reversed_charge DECIMAL(7,2),
  wr_account_credit DECIMAL(7,2),
  wr_net_loss DECIMAL(7,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_returns/data'
);

CREATE TABLE IF NOT EXISTS web_sales (
  ws_sold_time_sk INT, -- identifier foreign key t_time_sk
  ws_ship_date_sk INT, -- identifier foreign key d_date_sk
  ws_item_sk INT, -- identifier not null primary key foreign key i_item_sk
  ws_bill_customer_sk INT, -- identifier foreign key c_customer_sk
  ws_bill_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  ws_bill_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  ws_bill_addr_sk INT, -- identifier foreign key ca_address_sk
  ws_ship_customer_sk INT, -- identifier foreign key c_customer_sk
  ws_ship_cdemo_sk INT, -- identifier foreign key cd_demo_sk
  ws_ship_hdemo_sk INT, -- identifier foreign key hd_demo_sk
  ws_ship_addr_sk INT, -- identifier foreign key ca_address_sk
  ws_web_page_sk INT, -- identifier foreign key wp_web_page_sk
  ws_web_site_sk INT, -- identifier foreign key web_site_sk
  ws_ship_mode_sk INT, -- identifier foreign key sm_ship_mode_sk
  ws_warehouse_sk INT, -- identifier foreign key w_warehouse_sk
  ws_promo_sk INT, -- identifier foreign key p_promo_sk
  ws_order_number BIGINT, -- identifier not null primary key
  ws_quantity INT,
  ws_wholesale_cost DECIMAL(7,2),
  ws_list_price DECIMAL(7,2),
  ws_sales_price DECIMAL(7,2),
  ws_ext_discount_amt DECIMAL(7,2),
  ws_ext_sales_price DECIMAL(7,2),
  ws_ext_wholesale_cost DECIMAL(7,2),
  ws_ext_list_price DECIMAL(7,2),
  ws_ext_tax DECIMAL(7,2),
  ws_coupon_amt DECIMAL(7,2),
  ws_ext_ship_cost DECIMAL(7,2),
  ws_net_paid DECIMAL(7,2),
  ws_net_paid_inc_tax DECIMAL(7,2),
  ws_net_paid_inc_ship DECIMAL(7,2),
  ws_net_paid_inc_ship_tax DECIMAL(7,2),
  ws_net_profit DECIMAL(7,2),
  ws_sold_date_sk INT) -- identifier foreign key d_date_sk
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_sales/data',
  partitioned_by = array['ws_sold_date_sk']
);

CREATE TABLE IF NOT EXISTS web_site (
  web_site_sk INT, -- identifier not null primary key
  web_site_id VARCHAR(16), -- char(16) not null
  web_rec_start_date DATE,
  web_rec_end_date DATE,
  web_name VARCHAR(50),
  web_open_date_sk INT, -- identifier foreign key d_date_sk
  web_close_date_sk INT, -- identifier foreign key d_date_sk
  web_class VARCHAR(50),
  web_manager VARCHAR(40),
  web_mkt_id INT,
  web_mkt_class VARCHAR(50),
  web_mkt_desc VARCHAR(100),
  web_market_manager VARCHAR(40),
  web_company_id INT,
  web_company_name VARCHAR(50), -- char(50)
  web_street_number VARCHAR(10), -- char(10)
  web_street_name VARCHAR(60),
  web_street_type VARCHAR(15), -- char(15)
  web_suite_number VARCHAR(10), -- char(10)
  web_city VARCHAR(60),
  web_county VARCHAR(30),
  web_state VARCHAR(2), -- char(2)
  web_zip VARCHAR(10), -- char(10)
  web_country VARCHAR(20),
  web_gmt_offset DECIMAL(5,2),
  web_tax_percentage DECIMAL(5,2))
WITH (
  format = 'PARQUET',
  external_location = 's3a://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_site/data'
);

-- aws s3 mv --recursive s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/catalog_sales/data/cs_sold_date_sk=null/ s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/catalog_sales/data/cs_sold_date_sk=__HIVE_DEFAULT_PARTITION__/
-- aws s3 mv --recursive s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store_returns/data/sr_returned_date_sk=null/ s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store_returns/data/sr_returned_date_sk=__HIVE_DEFAULT_PARTITION__/
-- aws s3 mv --recursive s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store_sales/data/ss_sold_date_sk=null/ s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/store_sales/data/ss_sold_date_sk=__HIVE_DEFAULT_PARTITION__/
-- aws s3 mv --recursive s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_returns/data/wr_returned_date_sk=null/ s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_returns/data/wr_returned_date_sk=__HIVE_DEFAULT_PARTITION__/
-- aws s3 mv --recursive s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_sales/data/ws_sold_date_sk=null/ s3://presto-workload/tcpds-sf100000-parquet-partitioned-zurich-20240313/web_sales/data/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__/
-- CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'catalog_returns', 'FULL');
CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'catalog_sales', 'FULL');
CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'inventory', 'FULL');
-- CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'store_returns', 'FULL');
CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'store_sales', 'FULL');
-- CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'web_returns', 'FULL');
CALL system.sync_partition_metadata('tpcds_sf100000_parquet_varchar_part', 'web_sales', 'FULL');

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
