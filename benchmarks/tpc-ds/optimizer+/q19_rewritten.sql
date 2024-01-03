SELECT
    A4.I_BRAND_ID "BRAND_ID",
    A4.I_BRAND "BRAND",
    A4.I_MANUFACT_ID "I_MANUFACT_ID",
    A4.I_MANUFACT "I_MANUFACT",
    SUM(A2.SS_EXT_SALES_PRICE) "EXT_PRICE"
FROM
    (
        CUSTOMER_ADDRESS A0
        INNER JOIN (
            (
                CUSTOMER A1
                INNER JOIN (
                    (
                        STORE_SALES A2
                        INNER JOIN DATE_DIM A3 ON (A3.D_DATE_SK = A2.SS_SOLD_DATE_SK)
                    )
                    INNER JOIN ITEM A4 ON (A2.SS_ITEM_SK = A4.I_ITEM_SK)
                ) ON (A2.SS_CUSTOMER_SK = A1.C_CUSTOMER_SK)
            )
            INNER JOIN STORE A5 ON (A2.SS_STORE_SK = A5.S_STORE_SK)
        ) ON (
            CAST(A1.C_CURRENT_ADDR_SK AS BIGINT) = A0.CA_ADDRESS_SK
        )
        AND (
            SUBSTR(A0.CA_ZIP, 1, 5) <> SUBSTR(A5.S_ZIP, 1, 5)
        )
    )
WHERE
    (A3.D_MOY = 11)
    AND (A3.D_YEAR = 1999)
    AND (A4.I_MANAGER_ID = 8)
GROUP BY
    A4.I_BRAND,
    A4.I_BRAND_ID,
    A4.I_MANUFACT_ID,
    A4.I_MANUFACT
ORDER BY
    5 DESC,
    2 ASC,
    1 ASC,
    3 ASC,
    4 ASC
limit
    100;