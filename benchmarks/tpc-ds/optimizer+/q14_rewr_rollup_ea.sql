WITH A7 AS (
    SELECT
        A8.C0 C0,
        A8.C1 C1,
        A8.C2 C2
    FROM
        (
            SELECT
                A9.C0 C0,
                A9.C1 C1,
                A9.C2 C2,
                SUM(A9.C3) C3,
                COUNT(*) C4
            FROM
                (
                    (
                        SELECT
                            A12.I_BRAND_ID C0,
                            A12.I_CLASS_ID C1,
                            A12.I_CATEGORY_ID C2,
                            -1 C3
                        FROM
                            (
                                (
                                    WEB_SALES A10
                                    INNER JOIN DATE_DIM A11 ON (A10.WS_SOLD_DATE_SK = A11.D_DATE_SK)
                                )
                                INNER JOIN ITEM A12 ON (A10.WS_ITEM_SK = A12.I_ITEM_SK)
                            )
                        WHERE
                            (1999 <= A11.D_YEAR)
                            AND (A11.D_YEAR <= 2001)
                    )
                    UNION
                    ALL (
                        SELECT
                            A13.C0 C0,
                            A13.C1 C1,
                            A13.C2 C2,
                            1 C3
                        FROM
                            (
                                SELECT
                                    A14.C0 C0,
                                    A14.C1 C1,
                                    A14.C2 C2,
                                    SUM(A14.C3) C3,
                                    COUNT(*) C4
                                FROM
                                    (
                                        (
                                            SELECT
                                                A17.I_BRAND_ID C0,
                                                A17.I_CLASS_ID C1,
                                                A17.I_CATEGORY_ID C2,
                                                -1 C3
                                            FROM
                                                (
                                                    (
                                                        CATALOG_SALES A15
                                                        INNER JOIN DATE_DIM A16 ON (A15.CS_SOLD_DATE_SK = A16.D_DATE_SK)
                                                    )
                                                    INNER JOIN ITEM A17 ON (A15.CS_ITEM_SK = A17.I_ITEM_SK)
                                                )
                                            WHERE
                                                (1999 <= A16.D_YEAR)
                                                AND (A16.D_YEAR <= 2001)
                                        )
                                        UNION
                                        ALL (
                                            SELECT
                                                A20.I_BRAND_ID C0,
                                                A20.I_CLASS_ID C1,
                                                A20.I_CATEGORY_ID C2,
                                                1 C3
                                            FROM
                                                (
                                                    (
                                                        STORE_SALES A18
                                                        INNER JOIN DATE_DIM A19 ON (A18.SS_SOLD_DATE_SK = A19.D_DATE_SK)
                                                    )
                                                    INNER JOIN ITEM A20 ON (A18.SS_ITEM_SK = A20.I_ITEM_SK)
                                                )
                                            WHERE
                                                (1999 <= A19.D_YEAR)
                                                AND (A19.D_YEAR <= 2001)
                                        )
                                    ) A14
                                GROUP BY
                                    A14.C2,
                                    A14.C1,
                                    A14.C0
                            ) A13
                        WHERE
                            (
                                (
                                    A13.C4 - CASE
                                        WHEN (A13.C3 >= 0) THEN A13.C3
                                        ELSE (-(A13.C3))
                                    END
                                ) >= 2
                            )
                    )
                ) A9
            GROUP BY
                A9.C2,
                A9.C1,
                A9.C0
        ) A8
    WHERE
        (
            (
                A8.C4 - CASE
                    WHEN (A8.C3 >= 0) THEN A8.C3
                    ELSE (-(A8.C3))
                END
            ) >= 2
        )
),
A22 AS (
    SELECT
        SUM((A23.C0 * A23.C1)) C0,
        COUNT((A23.C0 * A23.C1)) C1
    FROM
        (
            (
                SELECT
                    A24.SS_QUANTITY C0,
                    A24.SS_LIST_PRICE C1
                FROM
                    (
                        STORE_SALES A24
                        INNER JOIN DATE_DIM A25 ON (A24.SS_SOLD_DATE_SK = A25.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A25.D_YEAR)
                    AND (A25.D_YEAR <= 2001)
            )
            UNION
            ALL (
                SELECT
                    A26.CS_QUANTITY C0,
                    A26.CS_LIST_PRICE C1
                FROM
                    (
                        CATALOG_SALES A26
                        INNER JOIN DATE_DIM A27 ON (A26.CS_SOLD_DATE_SK = A27.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A27.D_YEAR)
                    AND (A27.D_YEAR <= 2001)
            )
            UNION
            ALL (
                SELECT
                    A28.WS_QUANTITY C0,
                    A28.WS_LIST_PRICE C1
                FROM
                    (
                        WEB_SALES A28
                        INNER JOIN DATE_DIM A29 ON (A28.WS_SOLD_DATE_SK = A29.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A29.D_YEAR)
                    AND (A29.D_YEAR <= 2001)
            )
        ) A23
)
SELECT
    A0.C0 "CHANNEL",
    A0.C1 "I_BRAND_ID",
    A0.C2 "I_CLASS_ID",
    A0.C3 "I_CATEGORY_ID",
    A0.C4,
    A0.C5
FROM
    (
        (
            SELECT
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C0
                    WHEN (A43.C0 = 2) THEN A1.C0
                    WHEN (A43.C0 = 3) THEN A1.C0
                    WHEN (A43.C0 = 4) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C1
                    WHEN (A43.C0 = 2) THEN A1.C1
                    WHEN (A43.C0 = 3) THEN A1.C1
                    WHEN (A43.C0 = 4) THEN NULL
                    ELSE NULL
                END C1,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C2
                    WHEN (A43.C0 = 2) THEN A1.C2
                    WHEN (A43.C0 = 3) THEN NULL
                    WHEN (A43.C0 = 4) THEN NULL
                    ELSE NULL
                END C2,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C3
                    WHEN (A43.C0 = 2) THEN NULL
                    WHEN (A43.C0 = 3) THEN NULL
                    WHEN (A43.C0 = 4) THEN NULL
                    ELSE NULL
                END C3,
                SUM(A1.C4) C4,
                SUM(A1.C5) C5,
                A43.C0 C6
            FROM
                (
                    (
                        SELECT
                            A2.C0 C0,
                            A2.C1 C1,
                            A2.C2 C2,
                            A2.C3 C3,
                            SUM(A2.C4) C4,
                            SUM(A2.C5) C5
                        FROM
                            (
                                (
                                    SELECT
                                        CAST('store' AS VARCHAR(7)) C0,
                                        A3.C1 C1,
                                        A3.C2 C2,
                                        A3.C3 C3,
                                        A3.C0 C4,
                                        A3.C4 C5
                                    FROM
                                        (
                                            SELECT
                                                SUM((A4.SS_QUANTITY * A4.SS_LIST_PRICE)) C0,
                                                A6.I_BRAND_ID C1,
                                                A6.I_CLASS_ID C2,
                                                A6.I_CATEGORY_ID C3,
                                                COUNT(*) C4
                                            FROM
                                                (
                                                    (
                                                        STORE_SALES A4
                                                        INNER JOIN DATE_DIM A5 ON (A4.SS_SOLD_DATE_SK = A5.D_DATE_SK)
                                                    )
                                                    INNER JOIN (
                                                        ITEM A6
                                                        INNER JOIN A7 "A21" ON (A6.I_BRAND_ID = "A21".C0)
                                                        AND (A6.I_CLASS_ID = "A21".C1)
                                                        AND (A6.I_CATEGORY_ID = "A21".C2)
                                                    ) ON (A4.SS_ITEM_SK = A6.I_ITEM_SK)
                                                )
                                            WHERE
                                                (A5.D_YEAR = 2001)
                                                AND (A5.D_MOY = 11)
                                            GROUP BY
                                                A6.I_BRAND_ID,
                                                A6.I_CLASS_ID,
                                                A6.I_CATEGORY_ID
                                        ) A3,
                                        A22 "A30"
                                    WHERE
                                        (("A30".C0 / "A30".C1) < A3.C0)
                                )
                                UNION
                                ALL (
                                    SELECT
                                        'catalog' C0,
                                        A31.C1 C1,
                                        A31.C2 C2,
                                        A31.C3 C3,
                                        A31.C0 C4,
                                        A31.C4 C5
                                    FROM
                                        (
                                            SELECT
                                                SUM((A32.CS_QUANTITY * A32.CS_LIST_PRICE)) C0,
                                                A34.I_BRAND_ID C1,
                                                A34.I_CLASS_ID C2,
                                                A34.I_CATEGORY_ID C3,
                                                COUNT(*) C4
                                            FROM
                                                (
                                                    (
                                                        CATALOG_SALES A32
                                                        INNER JOIN DATE_DIM A33 ON (A32.CS_SOLD_DATE_SK = A33.D_DATE_SK)
                                                    )
                                                    INNER JOIN (
                                                        ITEM A34
                                                        INNER JOIN A7 "A35" ON (A34.I_BRAND_ID = "A35".C0)
                                                        AND (A34.I_CLASS_ID = "A35".C1)
                                                        AND (A34.I_CATEGORY_ID = "A35".C2)
                                                    ) ON (A32.CS_ITEM_SK = A34.I_ITEM_SK)
                                                )
                                            WHERE
                                                (A33.D_YEAR = 2001)
                                                AND (A33.D_MOY = 11)
                                            GROUP BY
                                                A34.I_BRAND_ID,
                                                A34.I_CLASS_ID,
                                                A34.I_CATEGORY_ID
                                        ) A31,
                                        A22 "A36"
                                    WHERE
                                        (("A36".C0 / "A36".C1) < A31.C0)
                                )
                                UNION
                                ALL (
                                    SELECT
                                        CAST('web' AS VARCHAR(7)) C0,
                                        A38.C1 C1,
                                        A38.C2 C2,
                                        A38.C3 C3,
                                        A38.C0 C4,
                                        A38.C4 C5
                                    FROM
                                        A22 "A37",
                                        (
                                            SELECT
                                                SUM((A39.WS_QUANTITY * A39.WS_LIST_PRICE)) C0,
                                                A41.I_BRAND_ID C1,
                                                A41.I_CLASS_ID C2,
                                                A41.I_CATEGORY_ID C3,
                                                COUNT(*) C4
                                            FROM
                                                (
                                                    (
                                                        WEB_SALES A39
                                                        INNER JOIN DATE_DIM A40 ON (A39.WS_SOLD_DATE_SK = A40.D_DATE_SK)
                                                    )
                                                    INNER JOIN (
                                                        ITEM A41
                                                        INNER JOIN A7 "A42" ON (A41.I_CATEGORY_ID = "A42".C2)
                                                        AND (A41.I_CLASS_ID = "A42".C1)
                                                        AND (A41.I_BRAND_ID = "A42".C0)
                                                    ) ON (A39.WS_ITEM_SK = A41.I_ITEM_SK)
                                                )
                                            WHERE
                                                (A40.D_YEAR = 2001)
                                                AND (A40.D_MOY = 11)
                                            GROUP BY
                                                A41.I_BRAND_ID,
                                                A41.I_CLASS_ID,
                                                A41.I_CATEGORY_ID
                                        ) A38
                                    WHERE
                                        (("A37".C0 / "A37".C1) < A38.C0)
                                )
                            ) A2
                        GROUP BY
                            A2.C0,
                            A2.C1,
                            A2.C2,
                            A2.C3
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3,
                            4,
                            5
                    ) A43 (C0) ON (MOD(LENGTH(A43.C0), 1) = MOD(LENGTH(A1.C0), 1))
                )
            GROUP BY
                A43.C0,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C0
                    WHEN (A43.C0 = 2) THEN A1.C0
                    WHEN (A43.C0 = 3) THEN A1.C0
                    WHEN (A43.C0 = 4) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C1
                    WHEN (A43.C0 = 2) THEN A1.C1
                    WHEN (A43.C0 = 3) THEN A1.C1
                    WHEN (A43.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C2
                    WHEN (A43.C0 = 2) THEN A1.C2
                    WHEN (A43.C0 = 3) THEN NULL
                    WHEN (A43.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A43.C0 = 1) THEN A1.C3
                    WHEN (A43.C0 = 2) THEN NULL
                    WHEN (A43.C0 = 3) THEN NULL
                    WHEN (A43.C0 = 4) THEN NULL
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
        ) A44 (C0) ON (A44.C0 = A0.C6)
    )
WHERE
    (
        (
            (A44.C0 = 5)
            AND (A0.C6 IS NULL)
        )
        OR (A0.C6 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;