SELECT
    A2.S_STORE_NAME "S_STORE_NAME",
    A2.S_STORE_ID "S_STORE_ID",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Sunday   ') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "SUN_SALES",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Monday   ') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "MON_SALES",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Tuesday  ') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "TUE_SALES",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Wednesday') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "WED_SALES",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Thursday ') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "THU_SALES",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Friday   ') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "FRI_SALES",
    SUM(
        CASE
            WHEN (A1.D_DAY_NAME = 'Saturday ') THEN A0.SS_SALES_PRICE
            ELSE NULL
        END
    ) "SAT_SALES"
FROM
    (
        (
            STORE_SALES A0
            INNER JOIN DATE_DIM A1 ON (A1.D_DATE_SK = A0.SS_SOLD_DATE_SK)
        )
        INNER JOIN STORE A2 ON (A2.S_STORE_SK = A0.SS_STORE_SK)
    )
WHERE
    (A1.D_YEAR = 2000)
    AND (A2.S_GMT_OFFSET = -005.00)
GROUP BY
    A2.S_STORE_NAME,
    A2.S_STORE_ID
ORDER BY
    1 ASC,
    2 ASC
limit
    100;