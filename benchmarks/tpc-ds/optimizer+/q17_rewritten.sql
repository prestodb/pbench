SELECT
    A0.C0 "I_ITEM_ID",
    A0.C1 "I_ITEM_DESC",
    A0.C2 "S_STATE",
    CAST(A0.C11 AS INTEGER) "STORE_SALES_QUANTITYCOUNT",
    CAST((A0.C10 / A0.C11) AS INTEGER) "STORE_SALES_QUANTITYAVE",
    A0.C3 "STORE_SALES_QUANTITYSTDEV",
    (A0.C3 / CAST((A0.C10 / A0.C11) AS INTEGER)) "STORE_SALES_QUANTITYCOV",
    CAST(A0.C9 AS INTEGER) "STORE_RETURNS_QUANTITYCOUNT",
    CAST((A0.C8 / A0.C9) AS INTEGER) "STORE_RETURNS_QUANTITYAVE",
    A0.C4 "STORE_RETURNS_QUANTITYSTDEV",
    (A0.C4 / CAST((A0.C8 / A0.C9) AS INTEGER)) "STORE_RETURNS_QUANTITYCOV",
    CAST(A0.C7 AS INTEGER) "CATALOG_SALES_QUANTITYCOUNT",
    CAST((A0.C6 / A0.C7) AS INTEGER) "CATALOG_SALES_QUANTITYAVE",
    A0.C5 "CATALOG_SALES_QUANTITYSTDEV",
    (A0.C5 / CAST((A0.C6 / A0.C7) AS INTEGER)) "CATALOG_SALES_QUANTITYCOV"
FROM
    (
        SELECT
            A7.I_ITEM_ID C0,
            A7.I_ITEM_DESC C1,
            A8.S_STATE C2,
            STDDEV_SAMP(A1.SS_QUANTITY) C3,
            STDDEV_SAMP(A5.SR_RETURN_QUANTITY) C4,
            STDDEV_SAMP(A3.CS_QUANTITY) C5,
            SUM(A3.CS_QUANTITY) C6,
            COUNT(A3.CS_QUANTITY) C7,
            SUM(A5.SR_RETURN_QUANTITY) C8,
            COUNT(A5.SR_RETURN_QUANTITY) C9,
            SUM(A1.SS_QUANTITY) C10,
            COUNT(A1.SS_QUANTITY) C11
        FROM
            (
                (
                    (
                        STORE_SALES A1
                        INNER JOIN DATE_DIM A2 ON (A2.D_DATE_SK = A1.SS_SOLD_DATE_SK)
                    )
                    INNER JOIN (
                        (
                            (
                                CATALOG_SALES A3
                                INNER JOIN DATE_DIM A4 ON (A3.CS_SOLD_DATE_SK = A4.D_DATE_SK)
                            )
                            INNER JOIN (
                                STORE_RETURNS A5
                                INNER JOIN DATE_DIM A6 ON (A5.SR_RETURNED_DATE_SK = A6.D_DATE_SK)
                            ) ON (A5.SR_CUSTOMER_SK = A3.CS_BILL_CUSTOMER_SK)
                            AND (A5.SR_ITEM_SK = A3.CS_ITEM_SK)
                        )
                        INNER JOIN ITEM A7 ON (A7.I_ITEM_SK = A5.SR_ITEM_SK)
                    ) ON (A1.SS_TICKET_NUMBER = A5.SR_TICKET_NUMBER)
                    AND (A1.SS_ITEM_SK = A5.SR_ITEM_SK)
                    AND (A1.SS_CUSTOMER_SK = A5.SR_CUSTOMER_SK)
                )
                INNER JOIN STORE A8 ON (A8.S_STORE_SK = A1.SS_STORE_SK)
            )
        WHERE
            (A2.D_QUARTER_NAME = '1999Q1')
            AND (
                A4.D_QUARTER_NAME IN ('1999Q1', '1999Q2', '1999Q3')
            )
            AND (
                A6.D_QUARTER_NAME IN ('1999Q1', '1999Q2', '1999Q3')
            )
        GROUP BY
            A7.I_ITEM_ID,
            A7.I_ITEM_DESC,
            A8.S_STATE
    ) A0
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC
limit
    100;