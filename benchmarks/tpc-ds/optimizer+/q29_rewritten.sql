SELECT
    A7.I_ITEM_ID "I_ITEM_ID",
    A7.I_ITEM_DESC "I_ITEM_DESC",
    A6.S_STORE_ID "S_STORE_ID",
    A6.S_STORE_NAME "S_STORE_NAME",
    STDDEV_SAMP(A2.SS_QUANTITY) "STORE_SALES_QUANTITY",
    STDDEV_SAMP(A4.SR_RETURN_QUANTITY) "STORE_RETURNS_QUANTITY",
    STDDEV_SAMP(A0.CS_QUANTITY) "CATALOG_SALES_QUANTITY"
FROM
    (
        (
            (
                CATALOG_SALES A0
                INNER JOIN DATE_DIM A1 ON (A0.CS_SOLD_DATE_SK = A1.D_DATE_SK)
            )
            INNER JOIN (
                (
                    (
                        STORE_SALES A2
                        INNER JOIN DATE_DIM A3 ON (A3.D_DATE_SK = A2.SS_SOLD_DATE_SK)
                    )
                    INNER JOIN (
                        STORE_RETURNS A4
                        INNER JOIN DATE_DIM A5 ON (A4.SR_RETURNED_DATE_SK = A5.D_DATE_SK)
                    ) ON (A2.SS_TICKET_NUMBER = A4.SR_TICKET_NUMBER)
                    AND (A3.D_YEAR = A5.D_YEAR)
                    AND (A2.SS_ITEM_SK = A4.SR_ITEM_SK)
                    AND (A2.SS_CUSTOMER_SK = A4.SR_CUSTOMER_SK)
                )
                INNER JOIN STORE A6 ON (A6.S_STORE_SK = A2.SS_STORE_SK)
            ) ON (A4.SR_CUSTOMER_SK = A0.CS_BILL_CUSTOMER_SK)
            AND (A4.SR_ITEM_SK = A0.CS_ITEM_SK)
        )
        INNER JOIN ITEM A7 ON (A7.I_ITEM_SK = A4.SR_ITEM_SK)
    )
WHERE
    (A1.D_YEAR IN (1999, 2000, 2001))
    AND (A3.D_MOY = 4)
    AND (A3.D_YEAR = 1999)
    AND (4 <= A5.D_MOY)
    AND (A5.D_MOY <= 7)
    AND (A5.D_YEAR = 1999)
GROUP BY
    A7.I_ITEM_ID,
    A7.I_ITEM_DESC,
    A6.S_STORE_ID,
    A6.S_STORE_NAME
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;