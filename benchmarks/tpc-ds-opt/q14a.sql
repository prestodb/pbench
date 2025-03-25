SELECT
    A1.C0 `CHANNEL`,
    A1.C1 `I_BRAND_ID`,
    A1.C2 `I_CLASS_ID`,
    A1.C3 `I_CATEGORY_ID`,
    A1.C4,
    A1.C5
FROM
    (
        (
            VALUES
                1,
                2,
                3,
                4,
                5
        ) A0 (C0)
        LEFT OUTER JOIN (
            SELECT
                A2.C0 C0,
                A2.C1 C1,
                A2.C2 C2,
                A2.C3 C3,
                SUM(A2.C4) C4,
                SUM(A2.C5) C5,
                A2.C6 C6
            FROM
                (
                    SELECT
                        CASE
                            WHEN (A45.C0 < 5) THEN A3.C0
                            ELSE NULL
                        END C0,
                        CASE
                            WHEN (A45.C0 < 4) THEN A3.C1
                            ELSE NULL
                        END C1,
                        CASE
                            WHEN (A45.C0 < 3) THEN A3.C2
                            ELSE NULL
                        END C2,
                        CASE
                            WHEN (A45.C0 < 2) THEN A3.C3
                            ELSE NULL
                        END C3,
                        A3.C4 C4,
                        A3.C5 C5,
                        A45.C0 C6
                    FROM
                        (
                            (
                                SELECT
                                    A4.C0 C0,
                                    A4.C1 C1,
                                    A4.C2 C2,
                                    A4.C3 C3,
                                    SUM(A4.C4) C4,
                                    SUM(A4.C5) C5
                                FROM
                                    (
                                        (
                                            SELECT
                                                A5.C0 C0,
                                                A5.C1 C1,
                                                A5.C2 C2,
                                                A5.C3 C3,
                                                SUM(A5.C4) C4,
                                                COUNT(*) C5
                                            FROM
                                                (
                                                    (
                                                        (
                                                            SELECT
                                                                CAST('store' AS VARCHAR(7)) C0,
                                                                A8.I_BRAND_ID C1,
                                                                A8.I_CLASS_ID C2,
                                                                A8.I_CATEGORY_ID C3,
                                                                (A6.SS_QUANTITY * A6.SS_LIST_PRICE) C4,
                                                                1 C5,
                                                                A6.SS_ITEM_SK C6
                                                            FROM
                                                                (
                                                                    (
                                                                        store_sales A6
                                                                        INNER JOIN date_dim A7 ON (
                                                                            (A6.SS_SOLD_DATE_SK = A7.D_DATE_SK)
                                                                            AND (
                                                                                (A6.SS_SOLD_DATE_SK <= 2452244)
                                                                                AND (A6.SS_SOLD_DATE_SK >= 2452215)
                                                                            )
                                                                            AND (A7.D_MOY = 11)
                                                                            AND (A7.D_YEAR = 2001)
                                                                        )
                                                                    )
                                                                    INNER JOIN item A8 ON (A6.SS_ITEM_SK = A8.I_ITEM_SK)
                                                                )
                                                        )
                                                        UNION
                                                        ALL (
                                                            SELECT
                                                                'catalog' C0,
                                                                A11.I_BRAND_ID C1,
                                                                A11.I_CLASS_ID C2,
                                                                A11.I_CATEGORY_ID C3,
                                                                (A9.CS_QUANTITY * A9.CS_LIST_PRICE) C4,
                                                                2 C5,
                                                                A9.CS_ITEM_SK C6
                                                            FROM
                                                                (
                                                                    (
                                                                        catalog_sales A9
                                                                        INNER JOIN date_dim A10 ON (
                                                                            (A9.CS_SOLD_DATE_SK = A10.D_DATE_SK)
                                                                            AND (
                                                                                (A9.CS_SOLD_DATE_SK <= 2452244)
                                                                                AND (A9.CS_SOLD_DATE_SK >= 2452215)
                                                                            )
                                                                            AND (A10.D_MOY = 11)
                                                                            AND (A10.D_YEAR = 2001)
                                                                        )
                                                                    )
                                                                    INNER JOIN item A11 ON (A9.CS_ITEM_SK = A11.I_ITEM_SK)
                                                                )
                                                        )
                                                        UNION
                                                        ALL (
                                                            SELECT
                                                                CAST('web' AS VARCHAR(7)) C0,
                                                                A14.I_BRAND_ID C1,
                                                                A14.I_CLASS_ID C2,
                                                                A14.I_CATEGORY_ID C3,
                                                                (A12.WS_QUANTITY * A12.WS_LIST_PRICE) C4,
                                                                3 C5,
                                                                A12.WS_ITEM_SK C6
                                                            FROM
                                                                (
                                                                    (
                                                                        web_sales A12
                                                                        INNER JOIN date_dim A13 ON (
                                                                            (A12.WS_SOLD_DATE_SK = A13.D_DATE_SK)
                                                                            AND (
                                                                                (A12.WS_SOLD_DATE_SK <= 2452244)
                                                                                AND (A12.WS_SOLD_DATE_SK >= 2452215)
                                                                            )
                                                                            AND (A13.D_MOY = 11)
                                                                            AND (A13.D_YEAR = 2001)
                                                                        )
                                                                    )
                                                                    INNER JOIN item A14 ON (A12.WS_ITEM_SK = A14.I_ITEM_SK)
                                                                )
                                                        )
                                                    ) A5
                                                    INNER JOIN (
                                                        SELECT
                                                            A16.C0 C0
                                                        FROM
                                                            (
                                                                SELECT
                                                                    A17.C0 C0
                                                                FROM
                                                                    (
                                                                        SELECT
                                                                            A35.I_ITEM_SK C0
                                                                        FROM
                                                                            (
                                                                                (
                                                                                    SELECT
                                                                                        A19.C0 C0,
                                                                                        A19.C1 C1,
                                                                                        A19.C2 C2
                                                                                    FROM
                                                                                        (
                                                                                            SELECT
                                                                                                A20.C0 C0,
                                                                                                A20.C1 C1,
                                                                                                A20.C2 C2,
                                                                                                SUM(A20.C3) C3,
                                                                                                COUNT(*) C4
                                                                                            FROM
                                                                                                (
                                                                                                    (
                                                                                                        SELECT
                                                                                                            A24.I_BRAND_ID C0,
                                                                                                            A24.I_CLASS_ID C1,
                                                                                                            A24.I_CATEGORY_ID C2,
                                                                                                            -1 C3
                                                                                                        FROM
                                                                                                            (
                                                                                                                (
                                                                                                                    SELECT
                                                                                                                        A21.WS_ITEM_SK C0
                                                                                                                    FROM
                                                                                                                        (
                                                                                                                            web_sales A21
                                                                                                                            INNER JOIN date_dim A22 ON (
                                                                                                                                (A21.WS_SOLD_DATE_SK = A22.D_DATE_SK)
                                                                                                                                AND (
                                                                                                                                    (A21.WS_SOLD_DATE_SK <= 2452275)
                                                                                                                                    AND (A21.WS_SOLD_DATE_SK >= 2451180)
                                                                                                                                )
                                                                                                                                AND (1999 <= A22.D_YEAR)
                                                                                                                                AND (A22.D_YEAR <= 2001)
                                                                                                                            )
                                                                                                                        )
                                                                                                                    GROUP BY
                                                                                                                        A21.WS_ITEM_SK
                                                                                                                ) A23
                                                                                                                INNER JOIN item A24 ON (A23.C0 = A24.I_ITEM_SK)
                                                                                                            )
                                                                                                        GROUP BY
                                                                                                            A24.I_BRAND_ID,
                                                                                                            A24.I_CLASS_ID,
                                                                                                            A24.I_CATEGORY_ID
                                                                                                    )
                                                                                                    UNION
                                                                                                    ALL (
                                                                                                        SELECT
                                                                                                            A25.C0 C0,
                                                                                                            A25.C1 C1,
                                                                                                            A25.C2 C2,
                                                                                                            1 C3
                                                                                                        FROM
                                                                                                            (
                                                                                                                SELECT
                                                                                                                    A26.C0 C0,
                                                                                                                    A26.C1 C1,
                                                                                                                    A26.C2 C2,
                                                                                                                    SUM(A26.C3) C3,
                                                                                                                    COUNT(*) C4
                                                                                                                FROM
                                                                                                                    (
                                                                                                                        (
                                                                                                                            SELECT
                                                                                                                                A30.I_BRAND_ID C0,
                                                                                                                                A30.I_CLASS_ID C1,
                                                                                                                                A30.I_CATEGORY_ID C2,
                                                                                                                                -1 C3
                                                                                                                            FROM
                                                                                                                                (
                                                                                                                                    (
                                                                                                                                        SELECT
                                                                                                                                            A27.CS_ITEM_SK C0
                                                                                                                                        FROM
                                                                                                                                            (
                                                                                                                                                catalog_sales A27
                                                                                                                                                INNER JOIN date_dim A28 ON (
                                                                                                                                                    (A27.CS_SOLD_DATE_SK = A28.D_DATE_SK)
                                                                                                                                                    AND (
                                                                                                                                                        (A27.CS_SOLD_DATE_SK <= 2452275)
                                                                                                                                                        AND (A27.CS_SOLD_DATE_SK >= 2451180)
                                                                                                                                                    )
                                                                                                                                                    AND (1999 <= A28.D_YEAR)
                                                                                                                                                    AND (A28.D_YEAR <= 2001)
                                                                                                                                                )
                                                                                                                                            )
                                                                                                                                        GROUP BY
                                                                                                                                            A27.CS_ITEM_SK
                                                                                                                                    ) A29
                                                                                                                                    INNER JOIN item A30 ON (A29.C0 = A30.I_ITEM_SK)
                                                                                                                                )
                                                                                                                            GROUP BY
                                                                                                                                A30.I_BRAND_ID,
                                                                                                                                A30.I_CLASS_ID,
                                                                                                                                A30.I_CATEGORY_ID
                                                                                                                        )
                                                                                                                        UNION
                                                                                                                        ALL (
                                                                                                                            SELECT
                                                                                                                                A34.I_BRAND_ID C0,
                                                                                                                                A34.I_CLASS_ID C1,
                                                                                                                                A34.I_CATEGORY_ID C2,
                                                                                                                                1 C3
                                                                                                                            FROM
                                                                                                                                (
                                                                                                                                    (
                                                                                                                                        SELECT
                                                                                                                                            A31.SS_ITEM_SK C0
                                                                                                                                        FROM
                                                                                                                                            (
                                                                                                                                                store_sales A31
                                                                                                                                                INNER JOIN date_dim A32 ON (
                                                                                                                                                    (A31.SS_SOLD_DATE_SK = A32.D_DATE_SK)
                                                                                                                                                    AND (
                                                                                                                                                        (A31.SS_SOLD_DATE_SK <= 2452275)
                                                                                                                                                        AND (A31.SS_SOLD_DATE_SK >= 2451180)
                                                                                                                                                    )
                                                                                                                                                    AND (1999 <= A32.D_YEAR)
                                                                                                                                                    AND (A32.D_YEAR <= 2001)
                                                                                                                                                )
                                                                                                                                            )
                                                                                                                                        GROUP BY
                                                                                                                                            A31.SS_ITEM_SK
                                                                                                                                    ) A33
                                                                                                                                    INNER JOIN item A34 ON (A33.C0 = A34.I_ITEM_SK)
                                                                                                                                )
                                                                                                                            GROUP BY
                                                                                                                                A34.I_BRAND_ID,
                                                                                                                                A34.I_CLASS_ID,
                                                                                                                                A34.I_CATEGORY_ID
                                                                                                                        )
                                                                                                                    ) A26
                                                                                                                GROUP BY
                                                                                                                    A26.C2,
                                                                                                                    A26.C1,
                                                                                                                    A26.C0
                                                                                                            ) A25
                                                                                                        WHERE
                                                                                                            (
                                                                                                                (
                                                                                                                    A25.C4 - CASE
                                                                                                                        WHEN (A25.C3 >= 0) THEN A25.C3
                                                                                                                        ELSE (-(A25.C3))
                                                                                                                    END
                                                                                                                ) >= 2
                                                                                                            )
                                                                                                    )
                                                                                                ) A20
                                                                                            GROUP BY
                                                                                                A20.C2,
                                                                                                A20.C1,
                                                                                                A20.C0
                                                                                        ) A19
                                                                                    WHERE
                                                                                        (
                                                                                            (
                                                                                                A19.C4 - CASE
                                                                                                    WHEN (A19.C3 >= 0) THEN A19.C3
                                                                                                    ELSE (-(A19.C3))
                                                                                                END
                                                                                            ) >= 2
                                                                                        )
                                                                                ) A18
                                                                                INNER JOIN item A35 ON (A35.I_CATEGORY_ID = A18.C2)
                                                                                AND (A35.I_CLASS_ID = A18.C1)
                                                                                AND (A35.I_BRAND_ID = A18.C0)
                                                                            )
                                                                        GROUP BY
                                                                            A35.I_ITEM_SK
                                                                    ) A17
                                                                GROUP BY
                                                                    A17.C0
                                                            ) A16
                                                        GROUP BY
                                                            A16.C0
                                                    ) A15 ON (A5.C6 = A15.C0)
                                                )
                                            GROUP BY
                                                A5.C3,
                                                A5.C2,
                                                A5.C1,
                                                A5.C0,
                                                A5.C5
                                        ) A4
                                        INNER JOIN (
                                            SELECT
                                                CAST(
                                                    (
                                                        A37.C0 / COALESCE(A37.C1, 0)
                                                    ) AS DECIMAL(18, 2)
                                                ) C0
                                            FROM
                                                (
                                                    SELECT
                                                        SUM(A38.C1) C0,
                                                        SUM(A38.C0) C1
                                                    FROM
                                                        (
                                                            (
                                                                SELECT
                                                                    COUNT((A39.SS_QUANTITY * A39.SS_LIST_PRICE)) C0,
                                                                    SUM((A39.SS_QUANTITY * A39.SS_LIST_PRICE)) C1
                                                                FROM
                                                                    (
                                                                        store_sales A39
                                                                        INNER JOIN date_dim A40 ON (
                                                                            (A39.SS_SOLD_DATE_SK = A40.D_DATE_SK)
                                                                            AND (
                                                                                (A39.SS_SOLD_DATE_SK <= 2452275)
                                                                                AND (A39.SS_SOLD_DATE_SK >= 2451180)
                                                                            )
                                                                            AND (1999 <= A40.D_YEAR)
                                                                            AND (A40.D_YEAR <= 2001)
                                                                        )
                                                                    )
                                                            )
                                                            UNION
                                                            ALL (
                                                                SELECT
                                                                    COUNT((A41.CS_QUANTITY * A41.CS_LIST_PRICE)) C0,
                                                                    SUM((A41.CS_QUANTITY * A41.CS_LIST_PRICE)) C1
                                                                FROM
                                                                    (
                                                                        catalog_sales A41
                                                                        INNER JOIN date_dim A42 ON (
                                                                            (A41.CS_SOLD_DATE_SK = A42.D_DATE_SK)
                                                                            AND (
                                                                                (A41.CS_SOLD_DATE_SK <= 2452275)
                                                                                AND (A41.CS_SOLD_DATE_SK >= 2451180)
                                                                            )
                                                                            AND (1999 <= A42.D_YEAR)
                                                                            AND (A42.D_YEAR <= 2001)
                                                                        )
                                                                    )
                                                            )
                                                            UNION
                                                            ALL (
                                                                SELECT
                                                                    COUNT((A43.WS_QUANTITY * A43.WS_LIST_PRICE)) C0,
                                                                    SUM((A43.WS_QUANTITY * A43.WS_LIST_PRICE)) C1
                                                                FROM
                                                                    (
                                                                        web_sales A43
                                                                        INNER JOIN date_dim A44 ON (
                                                                            (A43.WS_SOLD_DATE_SK = A44.D_DATE_SK)
                                                                            AND (
                                                                                (A43.WS_SOLD_DATE_SK <= 2452275)
                                                                                AND (A43.WS_SOLD_DATE_SK >= 2451180)
                                                                            )
                                                                            AND (1999 <= A44.D_YEAR)
                                                                            AND (A44.D_YEAR <= 2001)
                                                                        )
                                                                    )
                                                            )
                                                        ) A38
                                                ) A37
                                        ) A36 ON ((A36.C0 < A4.C4))
                                    )
                                GROUP BY
                                    A4.C0,
                                    A4.C1,
                                    A4.C2,
                                    A4.C3
                            ) A3
                            INNER JOIN (
                                VALUES
                                    1,
                                    2,
                                    3,
                                    4,
                                    5
                            ) A45 (C0) ON (1 = 1)
                        )
                ) A2
            GROUP BY
                A2.C6,
                A2.C0,
                A2.C1,
                A2.C2,
                A2.C3
        ) A1 ON (A0.C0 = A1.C6)
    )
WHERE
    (
        (
            (A0.C0 = 5)
            AND (A1.C6 IS NULL)
        )
        OR (A1.C6 IS NOT NULL)
    )
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100