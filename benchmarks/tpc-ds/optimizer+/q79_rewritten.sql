SELECT
    A0.C_LAST_NAME "C_LAST_NAME",
    A0.C_FIRST_NAME "C_FIRST_NAME",
    SUBSTR(A1.C2, 1, 30),
    A1.C0 "SS_TICKET_NUMBER",
    A1.C3 "AMT",
    A1.C4 "PROFIT"
FROM
    (
        CUSTOMER A0
        INNER JOIN (
            SELECT
                A2.SS_TICKET_NUMBER C0,
                A2.SS_CUSTOMER_SK C1,
                A5.S_CITY C2,
                SUM(A2.SS_COUPON_AMT) C3,
                SUM(A2.SS_NET_PROFIT) C4
            FROM
                (
                    (
                        (
                            STORE_SALES A2
                            INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                        )
                        INNER JOIN HOUSEHOLD_DEMOGRAPHICS A4 ON (A2.SS_HDEMO_SK = A4.HD_DEMO_SK)
                    )
                    INNER JOIN STORE A5 ON (A2.SS_STORE_SK = A5.S_STORE_SK)
                )
            WHERE
                (A3.D_YEAR IN (1999, 2000, 2001))
                AND (A3.D_DOW = 1)
                AND (
                    (A4.HD_DEP_COUNT = 0)
                    OR (A4.HD_VEHICLE_COUNT > 4)
                )
                AND (200 <= A5.S_NUMBER_EMPLOYEES)
                AND (A5.S_NUMBER_EMPLOYEES <= 295)
            GROUP BY
                A2.SS_TICKET_NUMBER,
                A2.SS_CUSTOMER_SK,
                A5.S_CITY,
                A2.SS_ADDR_SK
        ) A1 ON (A1.C1 = A0.C_CUSTOMER_SK)
    )
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    6 ASC
limit
    100;