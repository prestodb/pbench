SELECT
    SUBSTR(A3.W_WAREHOUSE_NAME, 1, 20),
    A4.SM_TYPE "SM_TYPE",
    A2.WEB_NAME "WEB_NAME",
    SUM(
        CASE
            WHEN ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) <= 30) THEN 1
            ELSE 0
        END
    ) "30 days",
    SUM(
        CASE
            WHEN (
                ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) > 30)
                AND ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) <= 60)
            ) THEN 1
            ELSE 0
        END
    ) "31-60 days",
    SUM(
        CASE
            WHEN (
                ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) > 60)
                AND ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) <= 90)
            ) THEN 1
            ELSE 0
        END
    ) "61-90 days",
    SUM(
        CASE
            WHEN (
                ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) > 90)
                AND ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) <= 120)
            ) THEN 1
            ELSE 0
        END
    ) "91-120 days",
    SUM(
        CASE
            WHEN ((A0.WS_SHIP_DATE_SK - A0.WS_SOLD_DATE_SK) > 120) THEN 1
            ELSE 0
        END
    ) ">120 days"
FROM
    (
        (
            (
                (
                    WEB_SALES A0
                    INNER JOIN DATE_DIM A1 ON (A0.WS_SHIP_DATE_SK = A1.D_DATE_SK)
                )
                INNER JOIN WEB_SITE A2 ON (A0.WS_WEB_SITE_SK = A2.WEB_SITE_SK)
            )
            INNER JOIN WAREHOUSE A3 ON (A0.WS_WAREHOUSE_SK = A3.W_WAREHOUSE_SK)
        )
        INNER JOIN SHIP_MODE A4 ON (A0.WS_SHIP_MODE_SK = A4.SM_SHIP_MODE_SK)
    )
WHERE
    (1217 <= A1.D_MONTH_SEQ)
    AND (A1.D_MONTH_SEQ <= 1228)
GROUP BY
    SUBSTR(A3.W_WAREHOUSE_NAME, 1, 20),
    A4.SM_TYPE,
    A2.WEB_NAME
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC
limit
    100;