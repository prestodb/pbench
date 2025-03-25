SELECT
    A1.C_CUSTOMER_ID as CUSTOMER_ID,
    coalesce(c_last_name,'') + ', ' + coalesce(c_first_name,'') as CUSTOMERNAME
FROM
    (
        (
            store_returns A0
            INNER JOIN (
                (
                    customer A1
                    INNER JOIN customer_address A2 ON (
                        (A1.C_CURRENT_ADDR_SK = A2.CA_ADDRESS_SK)
                        AND (A1.C_CURRENT_CDEMO_SK IS NOT NULL)
                        AND (A2.CA_CITY = 'Edgewood')
                    )
                )
                INNER JOIN household_demographics A3 ON (
                    (A3.HD_DEMO_SK = A1.C_CURRENT_HDEMO_SK)
                    AND (
                        (A3.HD_INCOME_BAND_SK <= 8)
                        AND (A3.HD_INCOME_BAND_SK >= 5)
                    )
                )
            ) ON (
                (A0.SR_CDEMO_SK = A1.C_CURRENT_CDEMO_SK)
                AND (A0.SR_CDEMO_SK IS NOT NULL)
            )
        )
        INNER JOIN income_band A4 ON (
            (A4.IB_INCOME_BAND_SK = A3.HD_INCOME_BAND_SK)
            AND (38128 <= A4.IB_LOWER_BOUND)
            AND (A4.IB_UPPER_BOUND <= 88128)
        )
    )
ORDER BY
    1 ASC
limit
    100