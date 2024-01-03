SELECT
    A0.C0 "I_ITEM_ID",
    CAST((A0.C7 / A0.C8) AS INTEGER) "AGG1",
    (A0.C5 / A0.C6) "AGG2",
    (A0.C3 / A0.C4) "AGG3",
    (A0.C1 / A0.C2) "AGG4"
FROM
    (
        SELECT
            A1.I_ITEM_ID C0,
            SUM(A2.SS_SALES_PRICE) C1,
            COUNT(A2.SS_SALES_PRICE) C2,
            SUM(A2.SS_COUPON_AMT) C3,
            COUNT(A2.SS_COUPON_AMT) C4,
            SUM(A2.SS_LIST_PRICE) C5,
            COUNT(A2.SS_LIST_PRICE) C6,
            SUM(A2.SS_QUANTITY) C7,
            COUNT(A2.SS_QUANTITY) C8
        FROM
            (
                ITEM A1
                INNER JOIN (
                    (
                        (
                            STORE_SALES A2
                            INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                        )
                        INNER JOIN CUSTOMER_DEMOGRAPHICS A4 ON (A2.SS_CDEMO_SK = A4.CD_DEMO_SK)
                    )
                    INNER JOIN PROMOTION A5 ON (A2.SS_PROMO_SK = A5.P_PROMO_SK)
                ) ON (A2.SS_ITEM_SK = A1.I_ITEM_SK)
            )
        WHERE
            (A3.D_YEAR = 2001)
            AND (A4.CD_GENDER = 'M')
            AND (A4.CD_MARITAL_STATUS = 'M')
            AND (A4.CD_EDUCATION_STATUS = '4 yr Degree         ')
            AND (
                (A5.P_CHANNEL_EMAIL = 'N')
                OR (A5.P_CHANNEL_EVENT = 'N')
            )
        GROUP BY
            A1.I_ITEM_ID
    ) A0
ORDER BY
    1 ASC
limit
    100;