-- call_center
SELECT 'cc_call_center_sk', checksum(cc_call_center_sk) FROM call_center
UNION ALL
SELECT 'cc_call_center_id', checksum(cc_call_center_id) FROM call_center
UNION ALL
SELECT 'cc_rec_start_date', checksum(cc_rec_start_date) FROM call_center
UNION ALL
SELECT 'cc_rec_end_date', checksum(cc_rec_end_date) FROM call_center
UNION ALL
SELECT 'cc_closed_date_sk', checksum(cc_closed_date_sk) FROM call_center
UNION ALL
SELECT 'cc_open_date_sk', checksum(cc_open_date_sk) FROM call_center
UNION ALL
SELECT 'cc_name', checksum(cc_name) FROM call_center
UNION ALL
SELECT 'cc_class', checksum(cc_class) FROM call_center
UNION ALL
SELECT 'cc_employees', checksum(cc_employees) FROM call_center
UNION ALL
SELECT 'cc_sq_ft', checksum(cc_sq_ft) FROM call_center
UNION ALL
SELECT 'cc_hours', checksum(cc_hours) FROM call_center
UNION ALL
SELECT 'cc_manager', checksum(cc_manager) FROM call_center
UNION ALL
SELECT 'cc_mkt_id', checksum(cc_mkt_id) FROM call_center
UNION ALL
SELECT 'cc_mkt_class', checksum(cc_mkt_class) FROM call_center
UNION ALL
SELECT 'cc_mkt_desc', checksum(cc_mkt_desc) FROM call_center
UNION ALL
SELECT 'cc_market_manager', checksum(cc_market_manager) FROM call_center
UNION ALL
SELECT 'cc_division', checksum(cc_division) FROM call_center
UNION ALL
SELECT 'cc_division_name', checksum(cc_division_name) FROM call_center
UNION ALL
SELECT 'cc_company', checksum(cc_company) FROM call_center
UNION ALL
SELECT 'cc_company_name', checksum(cc_company_name) FROM call_center
UNION ALL
SELECT 'cc_street_number', checksum(cc_street_number) FROM call_center
UNION ALL
SELECT 'cc_street_name', checksum(cc_street_name) FROM call_center
UNION ALL
SELECT 'cc_street_type', checksum(cc_street_type) FROM call_center
UNION ALL
SELECT 'cc_suite_number', checksum(cc_suite_number) FROM call_center
UNION ALL
SELECT 'cc_city', checksum(cc_city) FROM call_center
UNION ALL
SELECT 'cc_county', checksum(cc_county) FROM call_center
UNION ALL
SELECT 'cc_state', checksum(cc_state) FROM call_center
UNION ALL
SELECT 'cc_zip', checksum(cc_zip) FROM call_center
UNION ALL
SELECT 'cc_country', checksum(cc_country) FROM call_center
UNION ALL
SELECT 'cc_gmt_offset', checksum(cc_gmt_offset) FROM call_center
UNION ALL
SELECT 'cc_tax_percentage', checksum(cc_tax_percentage) FROM call_center
ORDER BY 1;

-- catalog_page
SELECT 'cp_catalog_page_sk', checksum(cp_catalog_page_sk) FROM catalog_page
UNION ALL
SELECT 'cp_catalog_page_id', checksum(cp_catalog_page_id) FROM catalog_page
UNION ALL
SELECT 'cp_start_date_sk', checksum(cp_start_date_sk) FROM catalog_page
UNION ALL
SELECT 'cp_end_date_sk', checksum(cp_end_date_sk) FROM catalog_page
UNION ALL
SELECT 'cp_department', checksum(cp_department) FROM catalog_page
UNION ALL
SELECT 'cp_catalog_number', checksum(cp_catalog_number) FROM catalog_page
UNION ALL
SELECT 'cp_catalog_page_number', checksum(cp_catalog_page_number) FROM catalog_page
UNION ALL
SELECT 'cp_description', checksum(cp_description) FROM catalog_page
UNION ALL
SELECT 'cp_type', checksum(cp_type) FROM catalog_page
ORDER BY 1;

-- catalog_returns
SELECT 'cr_returned_date_sk', checksum(cr_returned_date_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_returned_time_sk', checksum(cr_returned_time_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_item_sk', checksum(cr_item_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_refunded_customer_sk', checksum(cr_refunded_customer_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_refunded_cdemo_sk', checksum(cr_refunded_cdemo_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_refunded_hdemo_sk', checksum(cr_refunded_hdemo_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_refunded_addr_sk', checksum(cr_refunded_addr_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_returning_customer_sk', checksum(cr_returning_customer_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_returning_cdemo_sk', checksum(cr_returning_cdemo_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_returning_hdemo_sk', checksum(cr_returning_hdemo_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_returning_addr_sk', checksum(cr_returning_addr_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_call_center_sk', checksum(cr_call_center_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_catalog_page_sk', checksum(cr_catalog_page_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_ship_mode_sk', checksum(cr_ship_mode_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_warehouse_sk', checksum(cr_warehouse_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_reason_sk', checksum(cr_reason_sk) FROM catalog_returns
UNION ALL
SELECT 'cr_order_number', checksum(cr_order_number) FROM catalog_returns
UNION ALL
SELECT 'cr_return_quantity', checksum(cr_return_quantity) FROM catalog_returns
UNION ALL
SELECT 'cr_return_amount', checksum(cr_return_amount) FROM catalog_returns
UNION ALL
SELECT 'cr_return_tax', checksum(cr_return_tax) FROM catalog_returns
UNION ALL
SELECT 'cr_return_amt_inc_tax', checksum(cr_return_amt_inc_tax) FROM catalog_returns
UNION ALL
SELECT 'cr_fee', checksum(cr_fee) FROM catalog_returns
UNION ALL
SELECT 'cr_return_ship_cost', checksum(cr_return_ship_cost) FROM catalog_returns
UNION ALL
SELECT 'cr_refunded_cash', checksum(cr_refunded_cash) FROM catalog_returns
UNION ALL
SELECT 'cr_reversed_charge', checksum(cr_reversed_charge) FROM catalog_returns
UNION ALL
SELECT 'cr_store_credit', checksum(cr_store_credit) FROM catalog_returns
UNION ALL
SELECT 'cr_net_loss', checksum(cr_net_loss) FROM catalog_returns
ORDER BY 1;

-- catalog_sales
SELECT 'cs_sold_time_sk', checksum(cs_sold_time_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_ship_date_sk', checksum(cs_ship_date_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_bill_customer_sk', checksum(cs_bill_customer_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_bill_cdemo_sk', checksum(cs_bill_cdemo_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_bill_hdemo_sk', checksum(cs_bill_hdemo_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_bill_addr_sk', checksum(cs_bill_addr_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_ship_customer_sk', checksum(cs_ship_customer_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_ship_cdemo_sk', checksum(cs_ship_cdemo_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_ship_hdemo_sk', checksum(cs_ship_hdemo_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_ship_addr_sk', checksum(cs_ship_addr_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_call_center_sk', checksum(cs_call_center_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_catalog_page_sk', checksum(cs_catalog_page_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_ship_mode_sk', checksum(cs_ship_mode_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_warehouse_sk', checksum(cs_warehouse_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_item_sk', checksum(cs_item_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_promo_sk', checksum(cs_promo_sk) FROM catalog_sales
UNION ALL
SELECT 'cs_order_number', checksum(cs_order_number) FROM catalog_sales
UNION ALL
SELECT 'cs_quantity', checksum(cs_quantity) FROM catalog_sales
UNION ALL
SELECT 'cs_wholesale_cost', checksum(cs_wholesale_cost) FROM catalog_sales
UNION ALL
SELECT 'cs_list_price', checksum(cs_list_price) FROM catalog_sales
UNION ALL
SELECT 'cs_sales_price', checksum(cs_sales_price) FROM catalog_sales
UNION ALL
SELECT 'cs_ext_discount_amt', checksum(cs_ext_discount_amt) FROM catalog_sales
UNION ALL
SELECT 'cs_ext_sales_price', checksum(cs_ext_sales_price) FROM catalog_sales
UNION ALL
SELECT 'cs_ext_wholesale_cost', checksum(cs_ext_wholesale_cost) FROM catalog_sales
UNION ALL
SELECT 'cs_ext_list_price', checksum(cs_ext_list_price) FROM catalog_sales
UNION ALL
SELECT 'cs_ext_tax', checksum(cs_ext_tax) FROM catalog_sales
UNION ALL
SELECT 'cs_coupon_amt', checksum(cs_coupon_amt) FROM catalog_sales
UNION ALL
SELECT 'cs_ext_ship_cost', checksum(cs_ext_ship_cost) FROM catalog_sales
UNION ALL
SELECT 'cs_net_paid', checksum(cs_net_paid) FROM catalog_sales
UNION ALL
SELECT 'cs_net_paid_inc_tax', checksum(cs_net_paid_inc_tax) FROM catalog_sales
UNION ALL
SELECT 'cs_net_paid_inc_ship', checksum(cs_net_paid_inc_ship) FROM catalog_sales
UNION ALL
SELECT 'cs_net_paid_inc_ship_tax', checksum(cs_net_paid_inc_ship_tax) FROM catalog_sales
UNION ALL
SELECT 'cs_net_profit', checksum(cs_net_profit) FROM catalog_sales
UNION ALL
SELECT 'cs_sold_date_sk', checksum(cs_sold_date_sk) FROM catalog_sales
ORDER BY 1;

-- customer
SELECT 'c_customer_sk', checksum(c_customer_sk) FROM customer
UNION ALL
SELECT 'c_customer_id', checksum(c_customer_id) FROM customer
UNION ALL
SELECT 'c_current_cdemo_sk', checksum(c_current_cdemo_sk) FROM customer
UNION ALL
SELECT 'c_current_hdemo_sk', checksum(c_current_hdemo_sk) FROM customer
UNION ALL
SELECT 'c_current_addr_sk', checksum(c_current_addr_sk) FROM customer
UNION ALL
SELECT 'c_first_shipto_date_sk', checksum(c_first_shipto_date_sk) FROM customer
UNION ALL
SELECT 'c_first_sales_date_sk', checksum(c_first_sales_date_sk) FROM customer
UNION ALL
SELECT 'c_salutation', checksum(c_salutation) FROM customer
UNION ALL
SELECT 'c_first_name', checksum(c_first_name) FROM customer
UNION ALL
SELECT 'c_last_name', checksum(c_last_name) FROM customer
UNION ALL
SELECT 'c_preferred_cust_flag', checksum(c_preferred_cust_flag) FROM customer
UNION ALL
SELECT 'c_birth_day', checksum(c_birth_day) FROM customer
UNION ALL
SELECT 'c_birth_month', checksum(c_birth_month) FROM customer
UNION ALL
SELECT 'c_birth_year', checksum(c_birth_year) FROM customer
UNION ALL
SELECT 'c_birth_country', checksum(c_birth_country) FROM customer
UNION ALL
SELECT 'c_login', checksum(c_login) FROM customer
UNION ALL
SELECT 'c_email_address', checksum(c_email_address) FROM customer
UNION ALL
SELECT 'c_last_review_date_sk', checksum(c_last_review_date_sk) FROM customer
ORDER BY 1;

-- customer_address
SELECT 'ca_address_sk', checksum(ca_address_sk) FROM customer_address
UNION ALL
SELECT 'ca_address_id', checksum(ca_address_id) FROM customer_address
UNION ALL
SELECT 'ca_street_number', checksum(ca_street_number) FROM customer_address
UNION ALL
SELECT 'ca_street_name', checksum(ca_street_name) FROM customer_address
UNION ALL
SELECT 'ca_street_type', checksum(ca_street_type) FROM customer_address
UNION ALL
SELECT 'ca_suite_number', checksum(ca_suite_number) FROM customer_address
UNION ALL
SELECT 'ca_city', checksum(ca_city) FROM customer_address
UNION ALL
SELECT 'ca_county', checksum(ca_county) FROM customer_address
UNION ALL
SELECT 'ca_state', checksum(ca_state) FROM customer_address
UNION ALL
SELECT 'ca_zip', checksum(ca_zip) FROM customer_address
UNION ALL
SELECT 'ca_country', checksum(ca_country) FROM customer_address
UNION ALL
SELECT 'ca_gmt_offset', checksum(ca_gmt_offset) FROM customer_address
UNION ALL
SELECT 'ca_location_type', checksum(ca_location_type) FROM customer_address
ORDER BY 1;

-- customer_demographics
SELECT 'cd_demo_sk', checksum(cd_demo_sk) FROM customer_demographics
UNION ALL
SELECT 'cd_gender', checksum(cd_gender) FROM customer_demographics
UNION ALL
SELECT 'cd_marital_status', checksum(cd_marital_status) FROM customer_demographics
UNION ALL
SELECT 'cd_education_status', checksum(cd_education_status) FROM customer_demographics
UNION ALL
SELECT 'cd_purchase_estimate', checksum(cd_purchase_estimate) FROM customer_demographics
UNION ALL
SELECT 'cd_credit_rating', checksum(cd_credit_rating) FROM customer_demographics
UNION ALL
SELECT 'cd_dep_count', checksum(cd_dep_count) FROM customer_demographics
UNION ALL
SELECT 'cd_dep_employed_count', checksum(cd_dep_employed_count) FROM customer_demographics
UNION ALL
SELECT 'cd_dep_college_count', checksum(cd_dep_college_count) FROM customer_demographics
ORDER BY 1;

-- date_dim
SELECT 'd_date_sk', checksum(d_date_sk) FROM date_dim
UNION ALL
SELECT 'd_date_id', checksum(d_date_id) FROM date_dim
UNION ALL
SELECT 'd_date', checksum(d_date) FROM date_dim
UNION ALL
SELECT 'd_month_seq', checksum(d_month_seq) FROM date_dim
UNION ALL
SELECT 'd_week_seq', checksum(d_week_seq) FROM date_dim
UNION ALL
SELECT 'd_quarter_seq', checksum(d_quarter_seq) FROM date_dim
UNION ALL
SELECT 'd_year', checksum(d_year) FROM date_dim
UNION ALL
SELECT 'd_dow', checksum(d_dow) FROM date_dim
UNION ALL
SELECT 'd_moy', checksum(d_moy) FROM date_dim
UNION ALL
SELECT 'd_dom', checksum(d_dom) FROM date_dim
UNION ALL
SELECT 'd_qoy', checksum(d_qoy) FROM date_dim
UNION ALL
SELECT 'd_fy_year', checksum(d_fy_year) FROM date_dim
UNION ALL
SELECT 'd_fy_quarter_seq', checksum(d_fy_quarter_seq) FROM date_dim
UNION ALL
SELECT 'd_fy_week_seq', checksum(d_fy_week_seq) FROM date_dim
UNION ALL
SELECT 'd_day_name', checksum(d_day_name) FROM date_dim
UNION ALL
SELECT 'd_quarter_name', checksum(d_quarter_name) FROM date_dim
UNION ALL
SELECT 'd_holiday', checksum(d_holiday) FROM date_dim
UNION ALL
SELECT 'd_weekend', checksum(d_weekend) FROM date_dim
UNION ALL
SELECT 'd_following_holiday', checksum(d_following_holiday) FROM date_dim
UNION ALL
SELECT 'd_first_dom', checksum(d_first_dom) FROM date_dim
UNION ALL
SELECT 'd_last_dom', checksum(d_last_dom) FROM date_dim
UNION ALL
SELECT 'd_same_day_ly', checksum(d_same_day_ly) FROM date_dim
UNION ALL
SELECT 'd_same_day_lq', checksum(d_same_day_lq) FROM date_dim
UNION ALL
SELECT 'd_current_day', checksum(d_current_day) FROM date_dim
UNION ALL
SELECT 'd_current_week', checksum(d_current_week) FROM date_dim
UNION ALL
SELECT 'd_current_month', checksum(d_current_month) FROM date_dim
UNION ALL
SELECT 'd_current_quarter', checksum(d_current_quarter) FROM date_dim
UNION ALL
SELECT 'd_current_year', checksum(d_current_year) FROM date_dim
ORDER BY 1;

-- household_demographics
SELECT 'hd_demo_sk', checksum(hd_demo_sk) FROM household_demographics
UNION ALL
SELECT 'hd_income_band_sk', checksum(hd_income_band_sk) FROM household_demographics
UNION ALL
SELECT 'hd_buy_potential', checksum(hd_buy_potential) FROM household_demographics
UNION ALL
SELECT 'hd_dep_count', checksum(hd_dep_count) FROM household_demographics
UNION ALL
SELECT 'hd_vehicle_count', checksum(hd_vehicle_count) FROM household_demographics
ORDER BY 1;

-- income_band
SELECT 'ib_income_band_sk', checksum(ib_income_band_sk) FROM income_band
UNION ALL
SELECT 'ib_lower_bound', checksum(ib_lower_bound) FROM income_band
UNION ALL
SELECT 'ib_upper_bound', checksum(ib_upper_bound) FROM income_band
ORDER BY 1;

-- inventory
SELECT 'inv_item_sk', checksum(inv_item_sk) FROM inventory
UNION ALL
SELECT 'inv_warehouse_sk', checksum(inv_warehouse_sk) FROM inventory
UNION ALL
SELECT 'inv_quantity_on_hand', checksum(inv_quantity_on_hand) FROM inventory
UNION ALL
SELECT 'inv_date_sk', checksum(inv_date_sk) FROM inventory
ORDER BY 1;

-- item
SELECT 'i_item_sk', checksum(i_item_sk) FROM item
UNION ALL
SELECT 'i_item_id', checksum(i_item_id) FROM item
UNION ALL
SELECT 'i_rec_start_date', checksum(i_rec_start_date) FROM item
UNION ALL
SELECT 'i_rec_end_date', checksum(i_rec_end_date) FROM item
UNION ALL
SELECT 'i_item_desc', checksum(i_item_desc) FROM item
UNION ALL
SELECT 'i_current_price', checksum(i_current_price) FROM item
UNION ALL
SELECT 'i_wholesale_cost', checksum(i_wholesale_cost) FROM item
UNION ALL
SELECT 'i_brand_id', checksum(i_brand_id) FROM item
UNION ALL
SELECT 'i_brand', checksum(i_brand) FROM item
UNION ALL
SELECT 'i_class_id', checksum(i_class_id) FROM item
UNION ALL
SELECT 'i_class', checksum(i_class) FROM item
UNION ALL
SELECT 'i_category_id', checksum(i_category_id) FROM item
UNION ALL
SELECT 'i_category', checksum(i_category) FROM item
UNION ALL
SELECT 'i_manufact_id', checksum(i_manufact_id) FROM item
UNION ALL
SELECT 'i_manufact', checksum(i_manufact) FROM item
UNION ALL
SELECT 'i_size', checksum(i_size) FROM item
UNION ALL
SELECT 'i_formulation', checksum(i_formulation) FROM item
UNION ALL
SELECT 'i_color', checksum(i_color) FROM item
UNION ALL
SELECT 'i_units', checksum(i_units) FROM item
UNION ALL
SELECT 'i_container', checksum(i_container) FROM item
UNION ALL
SELECT 'i_manager_id', checksum(i_manager_id) FROM item
UNION ALL
SELECT 'i_product_name', checksum(i_product_name) FROM item
ORDER BY 1;

-- promotion
SELECT 'p_promo_sk', checksum(p_promo_sk) FROM promotion
UNION ALL
SELECT 'p_promo_id', checksum(p_promo_id) FROM promotion
UNION ALL
SELECT 'p_start_date_sk', checksum(p_start_date_sk) FROM promotion
UNION ALL
SELECT 'p_end_date_sk', checksum(p_end_date_sk) FROM promotion
UNION ALL
SELECT 'p_item_sk', checksum(p_item_sk) FROM promotion
UNION ALL
SELECT 'p_cost', checksum(p_cost) FROM promotion
UNION ALL
SELECT 'p_response_targe', checksum(p_response_targe) FROM promotion
UNION ALL
SELECT 'p_promo_name', checksum(p_promo_name) FROM promotion
UNION ALL
SELECT 'p_channel_dmail', checksum(p_channel_dmail) FROM promotion
UNION ALL
SELECT 'p_channel_email', checksum(p_channel_email) FROM promotion
UNION ALL
SELECT 'p_channel_catalog', checksum(p_channel_catalog) FROM promotion
UNION ALL
SELECT 'p_channel_tv', checksum(p_channel_tv) FROM promotion
UNION ALL
SELECT 'p_channel_radio', checksum(p_channel_radio) FROM promotion
UNION ALL
SELECT 'p_channel_press', checksum(p_channel_press) FROM promotion
UNION ALL
SELECT 'p_channel_event', checksum(p_channel_event) FROM promotion
UNION ALL
SELECT 'p_channel_demo', checksum(p_channel_demo) FROM promotion
UNION ALL
SELECT 'p_channel_details', checksum(p_channel_details) FROM promotion
UNION ALL
SELECT 'p_purpose', checksum(p_purpose) FROM promotion
UNION ALL
SELECT 'p_discount_active', checksum(p_discount_active) FROM promotion
ORDER BY 1;

-- reason
SELECT 'r_reason_sk', checksum(r_reason_sk) FROM reason
UNION ALL
SELECT 'r_reason_id', checksum(r_reason_id) FROM reason
UNION ALL
SELECT 'r_reason_desc', checksum(r_reason_desc) FROM reason
ORDER BY 1;

-- ship_mode
SELECT 'sm_ship_mode_sk', checksum(sm_ship_mode_sk) FROM ship_mode
UNION ALL
SELECT 'sm_ship_mode_id', checksum(sm_ship_mode_id) FROM ship_mode
UNION ALL
SELECT 'sm_type', checksum(sm_type) FROM ship_mode
UNION ALL
SELECT 'sm_code', checksum(sm_code) FROM ship_mode
UNION ALL
SELECT 'sm_carrier', checksum(sm_carrier) FROM ship_mode
UNION ALL
SELECT 'sm_contract', checksum(sm_contract) FROM ship_mode
ORDER BY 1;

-- store
SELECT 's_store_sk', checksum(s_store_sk) FROM store
UNION ALL
SELECT 's_store_id', checksum(s_store_id) FROM store
UNION ALL
SELECT 's_rec_start_date', checksum(s_rec_start_date) FROM store
UNION ALL
SELECT 's_rec_end_date', checksum(s_rec_end_date) FROM store
UNION ALL
SELECT 's_closed_date_sk', checksum(s_closed_date_sk) FROM store
UNION ALL
SELECT 's_store_name', checksum(s_store_name) FROM store
UNION ALL
SELECT 's_number_employees', checksum(s_number_employees) FROM store
UNION ALL
SELECT 's_floor_space', checksum(s_floor_space) FROM store
UNION ALL
SELECT 's_hours', checksum(s_hours) FROM store
UNION ALL
SELECT 's_manager', checksum(s_manager) FROM store
UNION ALL
SELECT 's_market_id', checksum(s_market_id) FROM store
UNION ALL
SELECT 's_geography_class', checksum(s_geography_class) FROM store
UNION ALL
SELECT 's_market_desc', checksum(s_market_desc) FROM store
UNION ALL
SELECT 's_market_manager', checksum(s_market_manager) FROM store
UNION ALL
SELECT 's_division_id', checksum(s_division_id) FROM store
UNION ALL
SELECT 's_division_name', checksum(s_division_name) FROM store
UNION ALL
SELECT 's_company_id', checksum(s_company_id) FROM store
UNION ALL
SELECT 's_company_name', checksum(s_company_name) FROM store
UNION ALL
SELECT 's_street_number', checksum(s_street_number) FROM store
UNION ALL
SELECT 's_street_name', checksum(s_street_name) FROM store
UNION ALL
SELECT 's_street_type', checksum(s_street_type) FROM store
UNION ALL
SELECT 's_suite_number', checksum(s_suite_number) FROM store
UNION ALL
SELECT 's_city', checksum(s_city) FROM store
UNION ALL
SELECT 's_county', checksum(s_county) FROM store
UNION ALL
SELECT 's_state', checksum(s_state) FROM store
UNION ALL
SELECT 's_zip', checksum(s_zip) FROM store
UNION ALL
SELECT 's_country', checksum(s_country) FROM store
UNION ALL
SELECT 's_gmt_offset', checksum(s_gmt_offset) FROM store
UNION ALL
SELECT 's_tax_precentage', checksum(s_tax_precentage) FROM store
ORDER BY 1;

-- store_returns
SELECT 'sr_returned_date_sk', checksum(sr_returned_date_sk) FROM store_returns
UNION ALL
SELECT 'sr_return_time_sk', checksum(sr_return_time_sk) FROM store_returns
UNION ALL
SELECT 'sr_item_sk', checksum(sr_item_sk) FROM store_returns
UNION ALL
SELECT 'sr_customer_sk', checksum(sr_customer_sk) FROM store_returns
UNION ALL
SELECT 'sr_cdemo_sk', checksum(sr_cdemo_sk) FROM store_returns
UNION ALL
SELECT 'sr_hdemo_sk', checksum(sr_hdemo_sk) FROM store_returns
UNION ALL
SELECT 'sr_addr_sk', checksum(sr_addr_sk) FROM store_returns
UNION ALL
SELECT 'sr_store_sk', checksum(sr_store_sk) FROM store_returns
UNION ALL
SELECT 'sr_reason_sk', checksum(sr_reason_sk) FROM store_returns
UNION ALL
SELECT 'sr_ticket_number', checksum(sr_ticket_number) FROM store_returns
UNION ALL
SELECT 'sr_return_quantity', checksum(sr_return_quantity) FROM store_returns
UNION ALL
SELECT 'sr_return_amt', checksum(sr_return_amt) FROM store_returns
UNION ALL
SELECT 'sr_return_tax', checksum(sr_return_tax) FROM store_returns
UNION ALL
SELECT 'sr_return_amt_inc_tax', checksum(sr_return_amt_inc_tax) FROM store_returns
UNION ALL
SELECT 'sr_fee', checksum(sr_fee) FROM store_returns
UNION ALL
SELECT 'sr_return_ship_cost', checksum(sr_return_ship_cost) FROM store_returns
UNION ALL
SELECT 'sr_refunded_cash', checksum(sr_refunded_cash) FROM store_returns
UNION ALL
SELECT 'sr_reversed_charge', checksum(sr_reversed_charge) FROM store_returns
UNION ALL
SELECT 'sr_store_credit', checksum(sr_store_credit) FROM store_returns
UNION ALL
SELECT 'sr_net_loss', checksum(sr_net_loss) FROM store_returns
ORDER BY 1;

-- store_sales
SELECT 'ss_sold_time_sk', checksum(ss_sold_time_sk) FROM store_sales
UNION ALL
SELECT 'ss_item_sk', checksum(ss_item_sk) FROM store_sales
UNION ALL
SELECT 'ss_customer_sk', checksum(ss_customer_sk) FROM store_sales
UNION ALL
SELECT 'ss_cdemo_sk', checksum(ss_cdemo_sk) FROM store_sales
UNION ALL
SELECT 'ss_hdemo_sk', checksum(ss_hdemo_sk) FROM store_sales
UNION ALL
SELECT 'ss_addr_sk', checksum(ss_addr_sk) FROM store_sales
UNION ALL
SELECT 'ss_store_sk', checksum(ss_store_sk) FROM store_sales
UNION ALL
SELECT 'ss_promo_sk', checksum(ss_promo_sk) FROM store_sales
UNION ALL
SELECT 'ss_ticket_number', checksum(ss_ticket_number) FROM store_sales
UNION ALL
SELECT 'ss_quantity', checksum(ss_quantity) FROM store_sales
UNION ALL
SELECT 'ss_wholesale_cost', checksum(ss_wholesale_cost) FROM store_sales
UNION ALL
SELECT 'ss_list_price', checksum(ss_list_price) FROM store_sales
UNION ALL
SELECT 'ss_sales_price', checksum(ss_sales_price) FROM store_sales
UNION ALL
SELECT 'ss_ext_discount_amt', checksum(ss_ext_discount_amt) FROM store_sales
UNION ALL
SELECT 'ss_ext_sales_price', checksum(ss_ext_sales_price) FROM store_sales
UNION ALL
SELECT 'ss_ext_wholesale_cost', checksum(ss_ext_wholesale_cost) FROM store_sales
UNION ALL
SELECT 'ss_ext_list_price', checksum(ss_ext_list_price) FROM store_sales
UNION ALL
SELECT 'ss_ext_tax', checksum(ss_ext_tax) FROM store_sales
UNION ALL
SELECT 'ss_coupon_amt', checksum(ss_coupon_amt) FROM store_sales
UNION ALL
SELECT 'ss_net_paid', checksum(ss_net_paid) FROM store_sales
UNION ALL
SELECT 'ss_net_paid_inc_tax', checksum(ss_net_paid_inc_tax) FROM store_sales
UNION ALL
SELECT 'ss_net_profit', checksum(ss_net_profit) FROM store_sales
UNION ALL
SELECT 'ss_sold_date_sk', checksum(ss_sold_date_sk) FROM store_sales
ORDER BY 1;

-- time_dim
SELECT 't_time_sk', checksum(t_time_sk) FROM time_dim
UNION ALL
SELECT 't_time_id', checksum(t_time_id) FROM time_dim
UNION ALL
SELECT 't_time', checksum(t_time) FROM time_dim
UNION ALL
SELECT 't_hour', checksum(t_hour) FROM time_dim
UNION ALL
SELECT 't_minute', checksum(t_minute) FROM time_dim
UNION ALL
SELECT 't_second', checksum(t_second) FROM time_dim
UNION ALL
SELECT 't_am_pm', checksum(t_am_pm) FROM time_dim
UNION ALL
SELECT 't_shift', checksum(t_shift) FROM time_dim
UNION ALL
SELECT 't_sub_shift', checksum(t_sub_shift) FROM time_dim
UNION ALL
SELECT 't_meal_time', checksum(t_meal_time) FROM time_dim
ORDER BY 1;

-- warehouse
SELECT 'w_warehouse_sk', checksum(w_warehouse_sk) FROM warehouse
UNION ALL
SELECT 'w_warehouse_id', checksum(w_warehouse_id) FROM warehouse
UNION ALL
SELECT 'w_warehouse_name', checksum(w_warehouse_name) FROM warehouse
UNION ALL
SELECT 'w_warehouse_sq_ft', checksum(w_warehouse_sq_ft) FROM warehouse
UNION ALL
SELECT 'w_street_number', checksum(w_street_number) FROM warehouse
UNION ALL
SELECT 'w_street_name', checksum(w_street_name) FROM warehouse
UNION ALL
SELECT 'w_street_type', checksum(w_street_type) FROM warehouse
UNION ALL
SELECT 'w_suite_number', checksum(w_suite_number) FROM warehouse
UNION ALL
SELECT 'w_city', checksum(w_city) FROM warehouse
UNION ALL
SELECT 'w_county', checksum(w_county) FROM warehouse
UNION ALL
SELECT 'w_state', checksum(w_state) FROM warehouse
UNION ALL
SELECT 'w_zip', checksum(w_zip) FROM warehouse
UNION ALL
SELECT 'w_country', checksum(w_country) FROM warehouse
UNION ALL
SELECT 'w_gmt_offset', checksum(w_gmt_offset) FROM warehouse
ORDER BY 1;

-- web_page
SELECT 'wp_web_page_sk', checksum(wp_web_page_sk) FROM web_page
UNION ALL
SELECT 'wp_web_page_id', checksum(wp_web_page_id) FROM web_page
UNION ALL
SELECT 'wp_rec_start_date', checksum(wp_rec_start_date) FROM web_page
UNION ALL
SELECT 'wp_rec_end_date', checksum(wp_rec_end_date) FROM web_page
UNION ALL
SELECT 'wp_creation_date_sk', checksum(wp_creation_date_sk) FROM web_page
UNION ALL
SELECT 'wp_access_date_sk', checksum(wp_access_date_sk) FROM web_page
UNION ALL
SELECT 'wp_autogen_flag', checksum(wp_autogen_flag) FROM web_page
UNION ALL
SELECT 'wp_customer_sk', checksum(wp_customer_sk) FROM web_page
UNION ALL
SELECT 'wp_url', checksum(wp_url) FROM web_page
UNION ALL
SELECT 'wp_type', checksum(wp_type) FROM web_page
UNION ALL
SELECT 'wp_char_count', checksum(wp_char_count) FROM web_page
UNION ALL
SELECT 'wp_link_count', checksum(wp_link_count) FROM web_page
UNION ALL
SELECT 'wp_image_count', checksum(wp_image_count) FROM web_page
UNION ALL
SELECT 'wp_max_ad_count', checksum(wp_max_ad_count) FROM web_page
ORDER BY 1;

-- web_returns
SELECT 'wr_returned_date_sk', checksum(wr_returned_date_sk) FROM web_returns
UNION ALL
SELECT 'wr_returned_time_sk', checksum(wr_returned_time_sk) FROM web_returns
UNION ALL
SELECT 'wr_item_sk', checksum(wr_item_sk) FROM web_returns
UNION ALL
SELECT 'wr_refunded_customer_sk', checksum(wr_refunded_customer_sk) FROM web_returns
UNION ALL
SELECT 'wr_refunded_cdemo_sk', checksum(wr_refunded_cdemo_sk) FROM web_returns
UNION ALL
SELECT 'wr_refunded_hdemo_sk', checksum(wr_refunded_hdemo_sk) FROM web_returns
UNION ALL
SELECT 'wr_refunded_addr_sk', checksum(wr_refunded_addr_sk) FROM web_returns
UNION ALL
SELECT 'wr_returning_customer_sk', checksum(wr_returning_customer_sk) FROM web_returns
UNION ALL
SELECT 'wr_returning_cdemo_sk', checksum(wr_returning_cdemo_sk) FROM web_returns
UNION ALL
SELECT 'wr_returning_hdemo_sk', checksum(wr_returning_hdemo_sk) FROM web_returns
UNION ALL
SELECT 'wr_returning_addr_sk', checksum(wr_returning_addr_sk) FROM web_returns
UNION ALL
SELECT 'wr_web_page_sk', checksum(wr_web_page_sk) FROM web_returns
UNION ALL
SELECT 'wr_reason_sk', checksum(wr_reason_sk) FROM web_returns
UNION ALL
SELECT 'wr_order_number', checksum(wr_order_number) FROM web_returns
UNION ALL
SELECT 'wr_return_quantity', checksum(wr_return_quantity) FROM web_returns
UNION ALL
SELECT 'wr_return_amt', checksum(wr_return_amt) FROM web_returns
UNION ALL
SELECT 'wr_return_tax', checksum(wr_return_tax) FROM web_returns
UNION ALL
SELECT 'wr_return_amt_inc_tax', checksum(wr_return_amt_inc_tax) FROM web_returns
UNION ALL
SELECT 'wr_fee', checksum(wr_fee) FROM web_returns
UNION ALL
SELECT 'wr_return_ship_cost', checksum(wr_return_ship_cost) FROM web_returns
UNION ALL
SELECT 'wr_refunded_cash', checksum(wr_refunded_cash) FROM web_returns
UNION ALL
SELECT 'wr_reversed_charge', checksum(wr_reversed_charge) FROM web_returns
UNION ALL
SELECT 'wr_account_credit', checksum(wr_account_credit) FROM web_returns
UNION ALL
SELECT 'wr_net_loss', checksum(wr_net_loss) FROM web_returns
ORDER BY 1;

-- web_sales
SELECT 'ws_sold_time_sk', checksum(ws_sold_time_sk) FROM web_sales
UNION ALL
SELECT 'ws_ship_date_sk', checksum(ws_ship_date_sk) FROM web_sales
UNION ALL
SELECT 'ws_item_sk', checksum(ws_item_sk) FROM web_sales
UNION ALL
SELECT 'ws_bill_customer_sk', checksum(ws_bill_customer_sk) FROM web_sales
UNION ALL
SELECT 'ws_bill_cdemo_sk', checksum(ws_bill_cdemo_sk) FROM web_sales
UNION ALL
SELECT 'ws_bill_hdemo_sk', checksum(ws_bill_hdemo_sk) FROM web_sales
UNION ALL
SELECT 'ws_bill_addr_sk', checksum(ws_bill_addr_sk) FROM web_sales
UNION ALL
SELECT 'ws_ship_customer_sk', checksum(ws_ship_customer_sk) FROM web_sales
UNION ALL
SELECT 'ws_ship_cdemo_sk', checksum(ws_ship_cdemo_sk) FROM web_sales
UNION ALL
SELECT 'ws_ship_hdemo_sk', checksum(ws_ship_hdemo_sk) FROM web_sales
UNION ALL
SELECT 'ws_ship_addr_sk', checksum(ws_ship_addr_sk) FROM web_sales
UNION ALL
SELECT 'ws_web_page_sk', checksum(ws_web_page_sk) FROM web_sales
UNION ALL
SELECT 'ws_web_site_sk', checksum(ws_web_site_sk) FROM web_sales
UNION ALL
SELECT 'ws_ship_mode_sk', checksum(ws_ship_mode_sk) FROM web_sales
UNION ALL
SELECT 'ws_warehouse_sk', checksum(ws_warehouse_sk) FROM web_sales
UNION ALL
SELECT 'ws_promo_sk', checksum(ws_promo_sk) FROM web_sales
UNION ALL
SELECT 'ws_order_number', checksum(ws_order_number) FROM web_sales
UNION ALL
SELECT 'ws_quantity', checksum(ws_quantity) FROM web_sales
UNION ALL
SELECT 'ws_wholesale_cost', checksum(ws_wholesale_cost) FROM web_sales
UNION ALL
SELECT 'ws_list_price', checksum(ws_list_price) FROM web_sales
UNION ALL
SELECT 'ws_sales_price', checksum(ws_sales_price) FROM web_sales
UNION ALL
SELECT 'ws_ext_discount_amt', checksum(ws_ext_discount_amt) FROM web_sales
UNION ALL
SELECT 'ws_ext_sales_price', checksum(ws_ext_sales_price) FROM web_sales
UNION ALL
SELECT 'ws_ext_wholesale_cost', checksum(ws_ext_wholesale_cost) FROM web_sales
UNION ALL
SELECT 'ws_ext_list_price', checksum(ws_ext_list_price) FROM web_sales
UNION ALL
SELECT 'ws_ext_tax', checksum(ws_ext_tax) FROM web_sales
UNION ALL
SELECT 'ws_coupon_amt', checksum(ws_coupon_amt) FROM web_sales
UNION ALL
SELECT 'ws_ext_ship_cost', checksum(ws_ext_ship_cost) FROM web_sales
UNION ALL
SELECT 'ws_net_paid', checksum(ws_net_paid) FROM web_sales
UNION ALL
SELECT 'ws_net_paid_inc_tax', checksum(ws_net_paid_inc_tax) FROM web_sales
UNION ALL
SELECT 'ws_net_paid_inc_ship', checksum(ws_net_paid_inc_ship) FROM web_sales
UNION ALL
SELECT 'ws_net_paid_inc_ship_tax', checksum(ws_net_paid_inc_ship_tax) FROM web_sales
UNION ALL
SELECT 'ws_net_profit', checksum(ws_net_profit) FROM web_sales
UNION ALL
SELECT 'ws_sold_date_sk', checksum(ws_sold_date_sk) FROM web_sales
ORDER BY 1;

-- web_site
SELECT 'web_site_sk', checksum(web_site_sk) FROM web_site
UNION ALL
SELECT 'web_site_id', checksum(web_site_id) FROM web_site
UNION ALL
SELECT 'web_rec_start_date', checksum(web_rec_start_date) FROM web_site
UNION ALL
SELECT 'web_rec_end_date', checksum(web_rec_end_date) FROM web_site
UNION ALL
SELECT 'web_name', checksum(web_name) FROM web_site
UNION ALL
SELECT 'web_open_date_sk', checksum(web_open_date_sk) FROM web_site
UNION ALL
SELECT 'web_close_date_sk', checksum(web_close_date_sk) FROM web_site
UNION ALL
SELECT 'web_class', checksum(web_class) FROM web_site
UNION ALL
SELECT 'web_manager', checksum(web_manager) FROM web_site
UNION ALL
SELECT 'web_mkt_id', checksum(web_mkt_id) FROM web_site
UNION ALL
SELECT 'web_mkt_class', checksum(web_mkt_class) FROM web_site
UNION ALL
SELECT 'web_mkt_desc', checksum(web_mkt_desc) FROM web_site
UNION ALL
SELECT 'web_market_manager', checksum(web_market_manager) FROM web_site
UNION ALL
SELECT 'web_company_id', checksum(web_company_id) FROM web_site
UNION ALL
SELECT 'web_company_name', checksum(web_company_name) FROM web_site
UNION ALL
SELECT 'web_street_number', checksum(web_street_number) FROM web_site
UNION ALL
SELECT 'web_street_name', checksum(web_street_name) FROM web_site
UNION ALL
SELECT 'web_street_type', checksum(web_street_type) FROM web_site
UNION ALL
SELECT 'web_suite_number', checksum(web_suite_number) FROM web_site
UNION ALL
SELECT 'web_city', checksum(web_city) FROM web_site
UNION ALL
SELECT 'web_county', checksum(web_county) FROM web_site
UNION ALL
SELECT 'web_state', checksum(web_state) FROM web_site
UNION ALL
SELECT 'web_zip', checksum(web_zip) FROM web_site
UNION ALL
SELECT 'web_country', checksum(web_country) FROM web_site
UNION ALL
SELECT 'web_gmt_offset', checksum(web_gmt_offset) FROM web_site
UNION ALL
SELECT 'web_tax_percentage', checksum(web_tax_percentage) FROM web_site
ORDER BY 1;
