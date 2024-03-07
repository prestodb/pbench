--#BGBLK 10

 --set current schema bdinsights; 
--CREATE EXTERNAL TABLE '/dev/null' USING (FORMAT 'TEXT' NULLVALUE 'NULL' ESCAPECHAR '\') AS
  SELECT c_salutation, c_first_name, c_last_name, c_customer_id, c_birth_country, c_login, c_email_address, c_last_review_date_sk
  FROM customer ORDER BY c_birth_country, c_last_name, c_first_name;

--#EOBLK
