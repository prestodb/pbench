WITH A0 AS (
    SELECT
        A1.C0 C0,
        A1.C1 C1,
        A1.C2 C2,
        A1.C3 C3,
        A1.C4 C4,
        A1.C5 C5,
        RANK() OVER(
            PARTITION BY A1.C0,
            A1.C1,
            A1.C2
            ORDER BY
                A1.C3 ASC,
                A1.C4 ASC
        ) C6,
        SUM(A1.C5) OVER(PARTITION BY A1.C0, A1.C1, A1.C2, A1.C3) C7,
        COUNT(A1.C5) OVER(PARTITION BY A1.C0, A1.C1, A1.C2, A1.C3) C8
    FROM
        (
            SELECT
                A4.I_CATEGORY C0,
                A4.I_BRAND C1,
                A5.CC_NAME C2,
                A3.D_YEAR C3,
                A3.D_MOY C4,
                SUM(A2.CS_SALES_PRICE) C5
            FROM
                (
                    (
                        (
                            CATALOG_SALES A2
                            INNER JOIN DATE_DIM A3 ON (A2.CS_SOLD_DATE_SK = A3.D_DATE_SK)
                        )
                        INNER JOIN ITEM A4 ON (A2.CS_ITEM_SK = A4.I_ITEM_SK)
                    )
                    INNER JOIN CALL_CENTER A5 ON (A5.CC_CALL_CENTER_SK = A2.CS_CALL_CENTER_SK)
                )
            WHERE
                (
                    (
                        (A3.D_YEAR = 2000)
                        OR (
                            (A3.D_YEAR = 1999)
                            AND (A3.D_MOY = 12)
                        )
                    )
                    OR (
                        (A3.D_YEAR = 2001)
                        AND (A3.D_MOY = 1)
                    )
                )
            GROUP BY
                A4.I_CATEGORY,
                A4.I_BRAND,
                A5.CC_NAME,
                A3.D_YEAR,
                A3.D_MOY
        ) A1
)
SELECT
    "A8".C2 "CC_NAME",
    "A8".C3 "D_YEAR",
    "A8".C4 "D_MOY",
    ("A8".C7 / "A8".C8) "AVG_MONTHLY_SALES",
    "A8".C5 "SUM_SALES",
    "A7".C5 "PSUM",
    "A6".C5 "NSUM",
    ("A8".C5 - ("A8".C7 / "A8".C8))
FROM
    (
        A0 "A6"
        INNER JOIN (
            A0 "A7"
            INNER JOIN A0 "A8" ON ("A8".C6 = ("A7".C6 + 1))
            AND ("A8".C2 = "A7".C2)
            AND ("A8".C1 = "A7".C1)
            AND ("A8".C0 = "A7".C0)
        ) ON ("A8".C6 = ("A6".C6 - 1))
        AND ("A6".C0 = "A8".C0)
        AND ("A6".C1 = "A8".C1)
        AND ("A6".C2 = "A8".C2)
    )
WHERE
    (
        0.1 < CASE
            WHEN (("A8".C7 / "A8".C8) > 0) THEN (
                ABS(("A8".C5 - ("A8".C7 / "A8".C8))) / ("A8".C7 / "A8".C8)
            )
            ELSE NULL
        END
    )
    AND ("A8".C3 = 2000)
    AND (0 < ("A8".C7 / "A8".C8))
ORDER BY
    8 ASC,
    6 ASC
limit
    100;