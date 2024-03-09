SELECT * FROM customer_demographics
WHERE cd_gender = 'M'
    and cd_marital_status = 'M'
    and trim(cd_education_status) = '4 yr Degree' limit 10;

SELECT * FROM customer_demographics
WHERE cd_gender = 'M'
    and cd_marital_status = 'M'
    and cd_education_status = '4 yr Degree' limit 10;
