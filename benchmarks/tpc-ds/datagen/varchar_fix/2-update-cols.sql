-- USE iceberg.tpcds_sf1000_parquet_iceberg;
-- USE iceberg.tpcds_sf1000_parquet_varchar_iceberg_part;
-- USE iceberg.tpcds_sf10000_parquet_iceberg;
-- USE iceberg.tpcds_sf10000_parquet_varchar_iceberg_part;

UPDATE call_center SET
    cc_hours = trim(cc_hours),
    cc_mkt_class = trim(cc_mkt_class),
    cc_company_name = trim(cc_company_name),
    cc_street_number = trim(cc_street_number),
    cc_street_type = trim(cc_street_type),
    cc_suite_number = trim(cc_suite_number),
    cc_zip = trim(cc_zip);

UPDATE customer SET
    c_salutation = trim(c_salutation),
    c_first_name = trim(c_first_name),
    c_last_name = trim(c_last_name),
    c_email_address = trim(c_email_address);

UPDATE customer_address SET
    ca_street_number = trim(ca_street_number),
    ca_street_type = trim(ca_street_type),
    ca_suite_number = trim(ca_suite_number),
    ca_zip = trim(ca_zip),
    ca_location_type = trim(ca_location_type);

UPDATE customer_demographics SET
    cd_education_status = trim(cd_education_status),
    cd_credit_rating = trim(cd_credit_rating);

UPDATE date_dim SET d_day_name = trim(d_day_name);

UPDATE household_demographics SET hd_buy_potential = trim(hd_buy_potential);

UPDATE item SET
    i_brand = trim(i_brand),
    i_class = trim(i_class),
    i_category = trim(i_category),
    i_manufact = trim(i_manufact),
    i_size = trim(i_size),
    i_color = trim(i_color),
    i_units = trim(i_units),
    i_container = trim(i_container),
    i_product_name = trim(i_product_name);

UPDATE promotion SET
    p_promo_name = trim(p_promo_name),
    p_purpose = trim(p_purpose);

UPDATE reason SET r_reason_desc = trim(r_reason_desc);

UPDATE ship_mode SET
    sm_type = trim(sm_type),
    sm_code = trim(sm_code),
    sm_carrier = trim(sm_carrier),
    sm_contract = trim(sm_contract);

UPDATE store SET
    s_hours = trim(s_hours),
    s_street_type = trim(s_street_type),
    s_suite_number = trim(s_suite_number),
    s_zip = trim(s_zip);

UPDATE time_dim SET
    t_shift = trim(t_shift),
    t_sub_shift = trim(t_sub_shift),
    t_meal_time = trim(t_meal_time);

UPDATE warehouse SET
    w_street_number = trim(w_street_number),
    w_street_type = trim(w_street_type),
    w_suite_number = trim(w_suite_number),
    w_zip = trim(w_zip);

UPDATE web_page SET wp_type = trim(wp_type);

UPDATE web_site SET
    web_company_name = trim(web_company_name),
    web_street_number = trim(web_street_number),
    web_street_type = trim(web_street_type),
    web_suite_number = trim(web_suite_number),
    web_zip = trim(web_zip);
