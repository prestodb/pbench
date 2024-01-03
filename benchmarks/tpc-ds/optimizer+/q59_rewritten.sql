WITH A0 AS (
    SELECT
        A2.D_WEEK_SEQ C0,
        A1.SS_STORE_SK C1,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Sunday   ') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C2,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Monday   ') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C3,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Tuesday  ') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C4,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Wednesday') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C5,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Thursday ') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C6,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Friday   ') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C7,
        SUM(
            CASE
                WHEN (A2.D_DAY_NAME = 'Saturday ') THEN A1.SS_SALES_PRICE
                ELSE NULL
            END
        ) C8
    FROM
        (
            STORE_SALES A1
            INNER JOIN DATE_DIM A2 ON (A2.D_DATE_SK = A1.SS_SOLD_DATE_SK)
        )
    GROUP BY
        A2.D_WEEK_SEQ,
        A1.SS_STORE_SK
)
SELECT
    A8.S_STORE_NAME "S_STORE_NAME1",
    A8.S_STORE_ID "S_STORE_ID1",
    "A6".C0 "D_WEEK_SEQ1",
    ("A6".C2 / "A3".C2),
    ("A6".C3 / "A3".C3),
    ("A6".C4 / "A3".C4),
    ("A6".C5 / "A3".C5),
    ("A6".C6 / "A3".C6),
    ("A6".C7 / "A3".C7),
    ("A6".C8 / "A3".C8)
FROM
    (
        (
            (
                A0 "A3"
                INNER JOIN DATE_DIM A4 ON (A4.D_WEEK_SEQ = "A3".C0)
            )
            INNER JOIN STORE A5 ON ("A3".C1 = A5.S_STORE_SK)
        )
        INNER JOIN (
            (
                A0 "A6"
                INNER JOIN DATE_DIM A7 ON (A7.D_WEEK_SEQ = "A6".C0)
            )
            INNER JOIN STORE A8 ON ("A6".C1 = A8.S_STORE_SK)
        ) ON (A8.S_STORE_ID = A5.S_STORE_ID)
        AND ("A6".C0 = ("A3".C0 - 52))
    )
WHERE
    (A4.D_MONTH_SEQ <= 1229)
    AND (1218 <= A4.D_MONTH_SEQ)
    AND (A7.D_MONTH_SEQ <= 1217)
    AND (1206 <= A7.D_MONTH_SEQ)
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC
limit
    100;