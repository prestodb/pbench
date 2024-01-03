SELECT
    A0.C_LAST_NAME "C_LAST_NAME",
    A0.C_FIRST_NAME "C_FIRST_NAME",
    A0.C_SALUTATION "C_SALUTATION",
    A0.C_PREFERRED_CUST_FLAG "C_PREFERRED_CUST_FLAG",
    A1.C0 "SS_TICKET_NUMBER",
    A1.C2 "CNT"
FROM
    (
        CUSTOMER A0
        INNER JOIN (
            SELECT
                A2.SS_TICKET_NUMBER C0,
                A2.SS_CUSTOMER_SK C1,
                COUNT(*) C2
            FROM
                (
                    (
                        (
                            STORE_SALES A2
                            INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                        )
                        INNER JOIN STORE A4 ON (A2.SS_STORE_SK = A4.S_STORE_SK)
                    )
                    INNER JOIN HOUSEHOLD_DEMOGRAPHICS A5 ON (A2.SS_HDEMO_SK = A5.HD_DEMO_SK)
                )
            WHERE
                (
                    (
                        (A3.D_DOM >= 1)
                        AND (A3.D_DOM <= 3)
                    )
                    OR (
                        (A3.D_DOM >= 25)
                        AND (A3.D_DOM <= 28)
                    )
                )
                AND (A3.D_YEAR IN (2000, 2001, 2002))
                AND (A4.S_COUNTY = 'Williamson County')
                AND (
                    1.2 < CASE
                        WHEN (A5.HD_VEHICLE_COUNT > 0) THEN (A5.HD_DEP_COUNT / A5.HD_VEHICLE_COUNT)
                        ELSE NULL
                    END
                )
                AND (
                    A5.HD_BUY_POTENTIAL IN ('501-1000       ', 'Unknown        ')
                )
                AND (0 < A5.HD_VEHICLE_COUNT)
            GROUP BY
                A2.SS_TICKET_NUMBER,
                A2.SS_CUSTOMER_SK
        ) A1 ON (A1.C1 = A0.C_CUSTOMER_SK)
    )
WHERE
    (15 <= A1.C2)
    AND (A1.C2 <= 20)
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 DESC,
    5 ASC;