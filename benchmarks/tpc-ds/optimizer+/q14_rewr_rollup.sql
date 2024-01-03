WITH A6 AS (
    SELECT
        A7.C0 C0,
        A7.C1 C1,
        A7.C2 C2
    FROM
        (
            SELECT
                A8.C0 C0,
                A8.C1 C1,
                A8.C2 C2,
                SUM(A8.C3) C3,
                COUNT(*) C4
            FROM
                (
                    (
                        SELECT
                            A11.I_BRAND_ID C0,
                            A11.I_CLASS_ID C1,
                            A11.I_CATEGORY_ID C2,
                            -1 C3
                        FROM
                            (
                                (
                                    WEB_SALES A9
                                    INNER JOIN DATE_DIM A10 ON (A9.WS_SOLD_DATE_SK = A10.D_DATE_SK)
                                )
                                INNER JOIN ITEM A11 ON (A9.WS_ITEM_SK = A11.I_ITEM_SK)
                            )
                        WHERE
                            (1999 <= A10.D_YEAR)
                            AND (A10.D_YEAR <= 2001)
                    )
                    UNION
                    ALL (
                        SELECT
                            A12.C0 C0,
                            A12.C1 C1,
                            A12.C2 C2,
                            1 C3
                        FROM
                            (
                                SELECT
                                    A13.C0 C0,
                                    A13.C1 C1,
                                    A13.C2 C2,
                                    SUM(A13.C3) C3,
                                    COUNT(*) C4
                                FROM
                                    (
                                        (
                                            SELECT
                                                A16.I_BRAND_ID C0,
                                                A16.I_CLASS_ID C1,
                                                A16.I_CATEGORY_ID C2,
                                                -1 C3
                                            FROM
                                                (
                                                    (
                                                        CATALOG_SALES A14
                                                        INNER JOIN DATE_DIM A15 ON (A14.CS_SOLD_DATE_SK = A15.D_DATE_SK)
                                                    )
                                                    INNER JOIN ITEM A16 ON (A14.CS_ITEM_SK = A16.I_ITEM_SK)
                                                )
                                            WHERE
                                                (1999 <= A15.D_YEAR)
                                                AND (A15.D_YEAR <= 2001)
                                        )
                                        UNION
                                        ALL (
                                            SELECT
                                                A19.I_BRAND_ID C0,
                                                A19.I_CLASS_ID C1,
                                                A19.I_CATEGORY_ID C2,
                                                1 C3
                                            FROM
                                                (
                                                    (
                                                        STORE_SALES A17
                                                        INNER JOIN DATE_DIM A18 ON (A17.SS_SOLD_DATE_SK = A18.D_DATE_SK)
                                                    )
                                                    INNER JOIN ITEM A19 ON (A17.SS_ITEM_SK = A19.I_ITEM_SK)
                                                )
                                            WHERE
                                                (1999 <= A18.D_YEAR)
                                                AND (A18.D_YEAR <= 2001)
                                        )
                                    ) A13
                                GROUP BY
                                    A13.C2,
                                    A13.C1,
                                    A13.C0
                            ) A12
                        WHERE
                            (
                                (
                                    A12.C4 - CASE
                                        WHEN (A12.C3 >= 0) THEN A12.C3
                                        ELSE (-(A12.C3))
                                    END
                                ) >= 2
                            )
                    )
                ) A8
            GROUP BY
                A8.C2,
                A8.C1,
                A8.C0
        ) A7
    WHERE
        (
            (
                A7.C4 - CASE
                    WHEN (A7.C3 >= 0) THEN A7.C3
                    ELSE (-(A7.C3))
                END
            ) >= 2
        )
),
A21 AS (
    SELECT
        SUM((A22.C0 * A22.C1)) C0,
        COUNT((A22.C0 * A22.C1)) C1
    FROM
        (
            (
                SELECT
                    A23.SS_QUANTITY C0,
                    A23.SS_LIST_PRICE C1
                FROM
                    (
                        STORE_SALES A23
                        INNER JOIN DATE_DIM A24 ON (A23.SS_SOLD_DATE_SK = A24.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A24.D_YEAR)
                    AND (A24.D_YEAR <= 2001)
            )
            UNION
            ALL (
                SELECT
                    A25.CS_QUANTITY C0,
                    A25.CS_LIST_PRICE C1
                FROM
                    (
                        CATALOG_SALES A25
                        INNER JOIN DATE_DIM A26 ON (A25.CS_SOLD_DATE_SK = A26.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A26.D_YEAR)
                    AND (A26.D_YEAR <= 2001)
            )
            UNION
            ALL (
                SELECT
                    A27.WS_QUANTITY C0,
                    A27.WS_LIST_PRICE C1
                FROM
                    (
                        WEB_SALES A27
                        INNER JOIN DATE_DIM A28 ON (A27.WS_SOLD_DATE_SK = A28.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A28.D_YEAR)
                    AND (A28.D_YEAR <= 2001)
            )
        ) A22
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
                    WHEN (A42.C0 = 1) THEN A1.C0
                    WHEN (A42.C0 = 2) THEN A1.C0
                    WHEN (A42.C0 = 3) THEN A1.C0
                    WHEN (A42.C0 = 4) THEN A1.C0
                    ELSE NULL
                END C0,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C1
                    WHEN (A42.C0 = 2) THEN A1.C1
                    WHEN (A42.C0 = 3) THEN A1.C1
                    WHEN (A42.C0 = 4) THEN NULL
                    ELSE NULL
                END C1,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C2
                    WHEN (A42.C0 = 2) THEN A1.C2
                    WHEN (A42.C0 = 3) THEN NULL
                    WHEN (A42.C0 = 4) THEN NULL
                    ELSE NULL
                END C2,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C3
                    WHEN (A42.C0 = 2) THEN NULL
                    WHEN (A42.C0 = 3) THEN NULL
                    WHEN (A42.C0 = 4) THEN NULL
                    ELSE NULL
                END C3,
                SUM(A1.C4) C4,
                SUM(A1.C5) C5,
                A42.C0 C6
            FROM
                (
                    (
                        (
                            SELECT
                                CAST('store' AS VARCHAR(7)) C0,
                                A2.C1 C1,
                                A2.C2 C2,
                                A2.C3 C3,
                                A2.C0 C4,
                                A2.C4 C5
                            FROM
                                (
                                    SELECT
                                        SUM((A3.SS_QUANTITY * A3.SS_LIST_PRICE)) C0,
                                        A5.I_BRAND_ID C1,
                                        A5.I_CLASS_ID C2,
                                        A5.I_CATEGORY_ID C3,
                                        COUNT(*) C4
                                    FROM
                                        (
                                            (
                                                STORE_SALES A3
                                                INNER JOIN DATE_DIM A4 ON (A3.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                                            )
                                            INNER JOIN (
                                                ITEM A5
                                                INNER JOIN A6 "A20" ON (A5.I_BRAND_ID = "A20".C0)
                                                AND (A5.I_CLASS_ID = "A20".C1)
                                                AND (A5.I_CATEGORY_ID = "A20".C2)
                                            ) ON (A3.SS_ITEM_SK = A5.I_ITEM_SK)
                                        )
                                    WHERE
                                        (A4.D_YEAR = 2001)
                                        AND (A4.D_MOY = 11)
                                    GROUP BY
                                        A5.I_BRAND_ID,
                                        A5.I_CLASS_ID,
                                        A5.I_CATEGORY_ID
                                ) A2,
                                A21 "A29"
                            WHERE
                                (("A29".C0 / "A29".C1) < A2.C0)
                        )
                        UNION
                        ALL (
                            SELECT
                                'catalog' C0,
                                A30.C1 C1,
                                A30.C2 C2,
                                A30.C3 C3,
                                A30.C0 C4,
                                A30.C4 C5
                            FROM
                                (
                                    SELECT
                                        SUM((A31.CS_QUANTITY * A31.CS_LIST_PRICE)) C0,
                                        A33.I_BRAND_ID C1,
                                        A33.I_CLASS_ID C2,
                                        A33.I_CATEGORY_ID C3,
                                        COUNT(*) C4
                                    FROM
                                        (
                                            (
                                                CATALOG_SALES A31
                                                INNER JOIN DATE_DIM A32 ON (A31.CS_SOLD_DATE_SK = A32.D_DATE_SK)
                                            )
                                            INNER JOIN (
                                                ITEM A33
                                                INNER JOIN A6 "A34" ON (A33.I_BRAND_ID = "A34".C0)
                                                AND (A33.I_CLASS_ID = "A34".C1)
                                                AND (A33.I_CATEGORY_ID = "A34".C2)
                                            ) ON (A31.CS_ITEM_SK = A33.I_ITEM_SK)
                                        )
                                    WHERE
                                        (A32.D_YEAR = 2001)
                                        AND (A32.D_MOY = 11)
                                    GROUP BY
                                        A33.I_BRAND_ID,
                                        A33.I_CLASS_ID,
                                        A33.I_CATEGORY_ID
                                ) A30,
                                A21 "A35"
                            WHERE
                                (("A35".C0 / "A35".C1) < A30.C0)
                        )
                        UNION
                        ALL (
                            SELECT
                                CAST('web' AS VARCHAR(7)) C0,
                                A37.C1 C1,
                                A37.C2 C2,
                                A37.C3 C3,
                                A37.C0 C4,
                                A37.C4 C5
                            FROM
                                A21 "A36",
                                (
                                    SELECT
                                        SUM((A38.WS_QUANTITY * A38.WS_LIST_PRICE)) C0,
                                        A40.I_BRAND_ID C1,
                                        A40.I_CLASS_ID C2,
                                        A40.I_CATEGORY_ID C3,
                                        COUNT(*) C4
                                    FROM
                                        (
                                            (
                                                WEB_SALES A38
                                                INNER JOIN DATE_DIM A39 ON (A38.WS_SOLD_DATE_SK = A39.D_DATE_SK)
                                            )
                                            INNER JOIN (
                                                ITEM A40
                                                INNER JOIN A6 "A41" ON (A40.I_CATEGORY_ID = "A41".C2)
                                                AND (A40.I_CLASS_ID = "A41".C1)
                                                AND (A40.I_BRAND_ID = "A41".C0)
                                            ) ON (A38.WS_ITEM_SK = A40.I_ITEM_SK)
                                        )
                                    WHERE
                                        (A39.D_YEAR = 2001)
                                        AND (A39.D_MOY = 11)
                                    GROUP BY
                                        A40.I_BRAND_ID,
                                        A40.I_CLASS_ID,
                                        A40.I_CATEGORY_ID
                                ) A37
                            WHERE
                                (("A36".C0 / "A36".C1) < A37.C0)
                        )
                    ) A1
                    INNER JOIN (
                        VALUES
                            1,
                            2,
                            3,
                            4,
                            5
                    ) A42 (C0) ON (MOD(LENGTH(A42.C0), 1) = MOD(LENGTH(A1.C0), 1))
                )
            GROUP BY
                A42.C0,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C0
                    WHEN (A42.C0 = 2) THEN A1.C0
                    WHEN (A42.C0 = 3) THEN A1.C0
                    WHEN (A42.C0 = 4) THEN A1.C0
                    ELSE NULL
                END,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C1
                    WHEN (A42.C0 = 2) THEN A1.C1
                    WHEN (A42.C0 = 3) THEN A1.C1
                    WHEN (A42.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C2
                    WHEN (A42.C0 = 2) THEN A1.C2
                    WHEN (A42.C0 = 3) THEN NULL
                    WHEN (A42.C0 = 4) THEN NULL
                    ELSE NULL
                END,
                CASE
                    WHEN (A42.C0 = 1) THEN A1.C3
                    WHEN (A42.C0 = 2) THEN NULL
                    WHEN (A42.C0 = 3) THEN NULL
                    WHEN (A42.C0 = 4) THEN NULL
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
        ) A43 (C0) ON (A43.C0 = A0.C6)
    )
WHERE
    (
        (
            (A43.C0 = 5)
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