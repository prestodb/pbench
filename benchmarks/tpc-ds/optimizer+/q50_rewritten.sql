SELECT
    A3.S_STORE_NAME "S_STORE_NAME",
    A3.S_COMPANY_ID "S_COMPANY_ID",
    A3.S_STREET_NUMBER "S_STREET_NUMBER",
    A3.S_STREET_NAME "S_STREET_NAME",
    A3.S_STREET_TYPE "S_STREET_TYPE",
    A3.S_SUITE_NUMBER "S_SUITE_NUMBER",
    A3.S_CITY "S_CITY",
    A3.S_COUNTY "S_COUNTY",
    A3.S_STATE "S_STATE",
    A3.S_ZIP "S_ZIP",
    SUM(
        CASE
            WHEN (
                (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) <= 30
            ) THEN 1
            ELSE 0
        END
    ) "30 days",
    SUM(
        CASE
            WHEN (
                (
                    (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) > 30
                )
                AND (
                    (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) <= 60
                )
            ) THEN 1
            ELSE 0
        END
    ) "31-60 days",
    SUM(
        CASE
            WHEN (
                (
                    (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) > 60
                )
                AND (
                    (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) <= 90
                )
            ) THEN 1
            ELSE 0
        END
    ) "61-90 days",
    SUM(
        CASE
            WHEN (
                (
                    (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) > 90
                )
                AND (
                    (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) <= 120
                )
            ) THEN 1
            ELSE 0
        END
    ) "91-120 days",
    SUM(
        CASE
            WHEN (
                (A1.SR_RETURNED_DATE_SK - A0.SS_SOLD_DATE_SK) > 120
            ) THEN 1
            ELSE 0
        END
    ) ">120 days"
FROM
    (
        (
            STORE_SALES A0
            INNER JOIN (
                STORE_RETURNS A1
                INNER JOIN DATE_DIM A2 ON (A1.SR_RETURNED_DATE_SK = A2.D_DATE_SK)
            ) ON (A0.SS_TICKET_NUMBER = A1.SR_TICKET_NUMBER)
            AND (A0.SS_ITEM_SK = A1.SR_ITEM_SK)
            AND (A0.SS_CUSTOMER_SK = A1.SR_CUSTOMER_SK)
        )
        INNER JOIN STORE A3 ON (A0.SS_STORE_SK = A3.S_STORE_SK)
    )
WHERE
    (A0.SS_SOLD_DATE_SK IS NOT NULL)
    AND (A2.D_YEAR = 2002)
    AND (A2.D_MOY = 8)
GROUP BY
    A3.S_STORE_NAME,
    A3.S_COMPANY_ID,
    A3.S_STREET_NUMBER,
    A3.S_STREET_NAME,
    A3.S_STREET_TYPE,
    A3.S_SUITE_NUMBER,
    A3.S_CITY,
    A3.S_COUNTY,
    A3.S_STATE,
    A3.S_ZIP
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC,
    5 ASC,
    6 ASC,
    7 ASC,
    8 ASC,
    9 ASC,
    10 ASC
limit
    100;