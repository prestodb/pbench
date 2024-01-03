SELECT
    A0.C2 "I_ITEM_DESC",
    A0.C3 "W_WAREHOUSE_NAME",
    A0.C1 "D_WEEK_SEQ",
    SUM(
        CASE
            WHEN (A10.P_PROMO_SK IS NULL) THEN 1
            ELSE 0
        END
    ) "NO_PROMO",
    SUM(
        CASE
            WHEN (A10.P_PROMO_SK IS NOT NULL) THEN 1
            ELSE 0
        END
    ) "PROMO",
    COUNT(*) "TOTAL_CNT"
FROM
    (
        (
            SELECT
                A3.CS_PROMO_SK C0,
                A4.D_WEEK_SEQ C1,
                A1.I_ITEM_DESC C2,
                A9.W_WAREHOUSE_NAME C3
            FROM
                (
                    (
                        ITEM A1
                        INNER JOIN (
                            (
                                INVENTORY A2
                                INNER JOIN (
                                    (
                                        (
                                            (
                                                CATALOG_SALES A3
                                                INNER JOIN DATE_DIM A4 ON (A3.CS_SOLD_DATE_SK = A4.D_DATE_SK)
                                            )
                                            INNER JOIN HOUSEHOLD_DEMOGRAPHICS A5 ON (A3.CS_BILL_HDEMO_SK = A5.HD_DEMO_SK)
                                        )
                                        INNER JOIN CUSTOMER_DEMOGRAPHICS A6 ON (A3.CS_BILL_CDEMO_SK = A6.CD_DEMO_SK)
                                    )
                                    INNER JOIN DATE_DIM A7 ON (A3.CS_SHIP_DATE_SK = A7.D_DATE_SK)
                                    AND (DATE_ADD('day', 5, A4.D_DATE) < A7.D_DATE)
                                ) ON (A3.CS_ITEM_SK = A2.INV_ITEM_SK)
                                AND (A2.INV_QUANTITY_ON_HAND < A3.CS_QUANTITY)
                            )
                            INNER JOIN DATE_DIM A8 ON (A4.D_WEEK_SEQ = A8.D_WEEK_SEQ)
                            AND (A2.INV_DATE_SK = A8.D_DATE_SK)
                        ) ON (A1.I_ITEM_SK = A3.CS_ITEM_SK)
                    )
                    INNER JOIN WAREHOUSE A9 ON (A9.W_WAREHOUSE_SK = A2.INV_WAREHOUSE_SK)
                )
            WHERE
                (A4.D_YEAR = 1999)
                AND (A5.HD_BUY_POTENTIAL = '501-1000       ')
                AND (A6.CD_MARITAL_STATUS = 'S')
        ) A0
        LEFT OUTER JOIN PROMOTION A10 ON (A0.C0 = A10.P_PROMO_SK)
    )
GROUP BY
    A0.C2,
    A0.C3,
    A0.C1
ORDER BY
    6 DESC,
    1 ASC,
    2 ASC,
    3 ASC
limit
    100;