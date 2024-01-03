WITH A0 AS (
    SELECT
        A1.C2 C0,
        SUM(
            CASE
                WHEN (A1.C1 = 'Sunday   ') THEN A1.C0
                ELSE NULL
            END
        ) C1,
        SUM(
            CASE
                WHEN (A1.C1 = 'Monday   ') THEN A1.C0
                ELSE NULL
            END
        ) C2,
        SUM(
            CASE
                WHEN (A1.C1 = 'Tuesday  ') THEN A1.C0
                ELSE NULL
            END
        ) C3,
        SUM(
            CASE
                WHEN (A1.C1 = 'Wednesday') THEN A1.C0
                ELSE NULL
            END
        ) C4,
        SUM(
            CASE
                WHEN (A1.C1 = 'Thursday ') THEN A1.C0
                ELSE NULL
            END
        ) C5,
        SUM(
            CASE
                WHEN (A1.C1 = 'Friday   ') THEN A1.C0
                ELSE NULL
            END
        ) C6,
        SUM(
            CASE
                WHEN (A1.C1 = 'Saturday ') THEN A1.C0
                ELSE NULL
            END
        ) C7
    FROM
        (
            (
                SELECT
                    A2.CS_EXT_SALES_PRICE C0,
                    A3.D_DAY_NAME C1,
                    A3.D_WEEK_SEQ C2
                FROM
                    (
                        CATALOG_SALES A2
                        INNER JOIN DATE_DIM A3 ON (A3.D_DATE_SK = A2.CS_SOLD_DATE_SK)
                    )
            )
            UNION
            ALL (
                SELECT
                    A4.WS_EXT_SALES_PRICE C0,
                    A5.D_DAY_NAME C1,
                    A5.D_WEEK_SEQ C2
                FROM
                    (
                        WEB_SALES A4
                        INNER JOIN DATE_DIM A5 ON (A5.D_DATE_SK = A4.WS_SOLD_DATE_SK)
                    )
            )
        ) A1
    GROUP BY
        A1.C2
)
SELECT
    "A6".C0 "D_WEEK_SEQ1",
    ROUND(("A6".C1 / "A7".C1), 2),
    ROUND(("A6".C2 / "A7".C2), 2),
    ROUND(("A6".C3 / "A7".C3), 2),
    ROUND(("A6".C4 / "A7".C4), 2),
    ROUND(("A6".C5 / "A7".C5), 2),
    ROUND(("A6".C6 / "A7".C6), 2),
    ROUND(("A6".C7 / "A7".C7), 2)
FROM
    (
        (
            A0 "A6"
            INNER JOIN (
                A0 "A7"
                INNER JOIN DATE_DIM A8 ON (A8.D_WEEK_SEQ = "A7".C0)
            ) ON ("A6".C0 = ("A7".C0 - 53))
        )
        INNER JOIN DATE_DIM A9 ON (A9.D_WEEK_SEQ = "A6".C0)
    )
WHERE
    (A8.D_YEAR = 2001)
    AND (A9.D_YEAR = 2000)
ORDER BY
    1 ASC;