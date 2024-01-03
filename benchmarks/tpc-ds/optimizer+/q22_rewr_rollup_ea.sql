SELECT
    A0.C0 "I_PRODUCT_NAME",
    A0.C1 "I_BRAND",
    A0.C2 "I_CLASS",
    A0.C3 "I_CATEGORY",
    CAST(
        (
            A0.C4 / CASE
                WHEN (A0.C6 IS NOT NULL) THEN A0.C5
                ELSE 0000000000000000000000000000000.
            END
        ) AS INTEGER
    ) "QOH"
FROM
    (
        (
            SELECT
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C0
                    WHEN (A5.C0 = 2) THEN A1.C0
                    WHEN (A5.C0 = 3) THEN A1.C0
                    WHEN (A5.C0 = 4) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C1
                    WHEN (A5.C0 = 2) THEN A1.C1
                    WHEN (A5.C0 = 3) THEN A1.C1
                    WHEN (A5.C0 = 4) THEN NULL
                    ELSE NULL
                END C1,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C2
                    WHEN (A5.C0 = 2) THEN A1.C2
                    WHEN (A5.C0 = 3) THEN NULL
                    WHEN (A5.C0 = 4) THEN NULL
                    ELSE NULL
                END C2,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C3
                    WHEN (A5.C0 = 2) THEN NULL
                    WHEN (A5.C0 = 3) THEN NULL
                    WHEN (A5.C0 = 4) THEN NULL
                    ELSE NULL
                END C3,
                SUM(A1.C5) C4,
                SUM(A1.C4) C5,
                A5.C0 C6
            FROM
                (
                    (
                        SELECT
                            A4.I_PRODUCT_NAME C0,
                            A4.I_BRAND C1,
                            A4.I_CLASS C2,
                            A4.I_CATEGORY C3,
                            COUNT(A2.INV_QUANTITY_ON_HAND) C4,
                            SUM(A2.INV_QUANTITY_ON_HAND) C5
                        FROM
                            (
                                (
                                    INVENTORY A2
                                    INNER JOIN DATE_DIM A3 ON (A2.INV_DATE_SK = A3.D_DATE_SK)
                                )
                                INNER JOIN ITEM A4 ON (A2.INV_ITEM_SK = A4.I_ITEM_SK)
                            )
                        WHERE
                            (1201 <= A3.D_MONTH_SEQ)
                            AND (A3.D_MONTH_SEQ <= 1212)
                        GROUP BY
                            A4.I_PRODUCT_NAME,
                            A4.I_BRAND,
                            A4.I_CLASS,
                            A4.I_CATEGORY
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3,
                            4,
                            5
                    ) A5 (C0) ON (
                        MOD(LENGTH(A5.C0), 1) = COALESCE(MOD(LENGTH(A1.C0), 1), 0)
                    )
                )
            GROUP BY
                A5.C0,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C0
                    WHEN (A5.C0 = 2) THEN A1.C0
                    WHEN (A5.C0 = 3) THEN A1.C0
                    WHEN (A5.C0 = 4) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C1
                    WHEN (A5.C0 = 2) THEN A1.C1
                    WHEN (A5.C0 = 3) THEN A1.C1
                    WHEN (A5.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C2
                    WHEN (A5.C0 = 2) THEN A1.C2
                    WHEN (A5.C0 = 3) THEN NULL
                    WHEN (A5.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A5.C0 = 1) THEN A1.C3
                    WHEN (A5.C0 = 2) THEN NULL
                    WHEN (A5.C0 = 3) THEN NULL
                    WHEN (A5.C0 = 4) THEN NULL
                    ELSE NULL
                END
        ) A0
        RIGHT OUTER JOIN (
            VALUES
                1,
                2,
                3,
                4,
                5
        ) A6 (C0) ON (A6.C0 = A0.C6)
    )
WHERE
    (
        (
            (A6.C0 = 5)
            AND (A0.C6 IS NULL)
        )
        OR (A0.C6 IS NOT NULL)
    )
ORDER BY
    5 ASC,
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;