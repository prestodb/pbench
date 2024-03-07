--#BGBLK 6

 --set current schema bdinsights; 
SELECT c_birth_country, c_salutation, c_last_name, c_first_name, c_customer_id
  FROM customer
  ORDER BY c_birth_country, c_last_name, c_first_name;

--#EOBLK
