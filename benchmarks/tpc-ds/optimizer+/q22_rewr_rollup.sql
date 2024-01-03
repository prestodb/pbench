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
                    WHEN (A4.C0 = 1) THEN A3.I_PRODUCT_NAME
                    WHEN (A4.C0 = 2) THEN A3.I_PRODUCT_NAME
                    WHEN (A4.C0 = 3) THEN A3.I_PRODUCT_NAME
                    WHEN (A4.C0 = 4) THEN A3.I_PRODUCT_NAME
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_BRAND
                    WHEN (A4.C0 = 2) THEN A3.I_BRAND
                    WHEN (A4.C0 = 3) THEN A3.I_BRAND
                    WHEN (A4.C0 = 4) THEN NULL
                    ELSE NULL
                END C1,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_CLASS
                    WHEN (A4.C0 = 2) THEN A3.I_CLASS
                    WHEN (A4.C0 = 3) THEN NULL
                    WHEN (A4.C0 = 4) THEN NULL
                    ELSE NULL
                END C2,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_CATEGORY
                    WHEN (A4.C0 = 2) THEN NULL
                    WHEN (A4.C0 = 3) THEN NULL
                    WHEN (A4.C0 = 4) THEN NULL
                    ELSE NULL
                END C3,
                SUM(A1.INV_QUANTITY_ON_HAND) C4,
                COUNT(A1.INV_QUANTITY_ON_HAND) C5,
                A4.C0 C6
            FROM
                (
                    (
                        (
                            INVENTORY A1
                            INNER JOIN DATE_DIM A2 ON (A1.INV_DATE_SK = A2.D_DATE_SK)
                        )
                        INNER JOIN ITEM A3 ON (A1.INV_ITEM_SK = A3.I_ITEM_SK)
                    )
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3,
                            4,
                            5
                    ) A4 (C0) ON (
                        MOD(LENGTH(A4.C0), 1) = COALESCE(MOD(LENGTH(A3.I_PRODUCT_NAME), 1), 0)
                    )
                )
            WHERE
                (A2.D_MONTH_SEQ <= 1212)
                AND (1201 <= A2.D_MONTH_SEQ)
            GROUP BY
                A4.C0,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_PRODUCT_NAME
                    WHEN (A4.C0 = 2) THEN A3.I_PRODUCT_NAME
                    WHEN (A4.C0 = 3) THEN A3.I_PRODUCT_NAME
                    WHEN (A4.C0 = 4) THEN A3.I_PRODUCT_NAME
                    ELSE NULL
                END,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_BRAND
                    WHEN (A4.C0 = 2) THEN A3.I_BRAND
                    WHEN (A4.C0 = 3) THEN A3.I_BRAND
                    WHEN (A4.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_CLASS
                    WHEN (A4.C0 = 2) THEN A3.I_CLASS
                    WHEN (A4.C0 = 3) THEN NULL
                    WHEN (A4.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A4.C0 = 1) THEN A3.I_CATEGORY
                    WHEN (A4.C0 = 2) THEN NULL
                    WHEN (A4.C0 = 3) THEN NULL
                    WHEN (A4.C0 = 4) THEN NULL
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
        ) A5 (C0) ON (A5.C0 = A0.C6)
    )
WHERE
    (
        (
            (A5.C0 = 5)
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