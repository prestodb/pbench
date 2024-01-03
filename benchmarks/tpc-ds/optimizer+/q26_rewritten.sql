SELECT
    A0.C0 "I_ITEM_ID",
    CAST((A0.C7 / A0.C8) AS INTEGER) "AGG1",
    (A0.C5 / A0.C6) "AGG2",
    (A0.C3 / A0.C4) "AGG3",
    (A0.C1 / A0.C2) "AGG4"
FROM
    (
        SELECT
            A5.I_ITEM_ID C0,
            SUM(A1.CS_SALES_PRICE) C1,
            COUNT(A1.CS_SALES_PRICE) C2,
            SUM(A1.CS_COUPON_AMT) C3,
            COUNT(A1.CS_COUPON_AMT) C4,
            SUM(A1.CS_LIST_PRICE) C5,
            COUNT(A1.CS_LIST_PRICE) C6,
            SUM(A1.CS_QUANTITY) C7,
            COUNT(A1.CS_QUANTITY) C8
        FROM
            (
                (
                    (
                        (
                            CATALOG_SALES A1
                            INNER JOIN DATE_DIM A2 ON (A1.CS_SOLD_DATE_SK = A2.D_DATE_SK)
                        )
                        INNER JOIN CUSTOMER_DEMOGRAPHICS A3 ON (A1.CS_BILL_CDEMO_SK = A3.CD_DEMO_SK)
                    )
                    INNER JOIN PROMOTION A4 ON (A1.CS_PROMO_SK = A4.P_PROMO_SK)
                )
                INNER JOIN ITEM A5 ON (A1.CS_ITEM_SK = A5.I_ITEM_SK)
            )
        WHERE
            (A2.D_YEAR = 2000)
            AND (A3.CD_GENDER = 'F')
            AND (A3.CD_MARITAL_STATUS = 'M')
            AND (A3.CD_EDUCATION_STATUS = '4 yr Degree         ')
            AND (
                (A4.P_CHANNEL_EMAIL = 'N')
                OR (A4.P_CHANNEL_EVENT = 'N')
            )
        GROUP BY
            A5.I_ITEM_ID
    ) A0
ORDER BY
    1 ASC
limit
    100;