WITH A10 AS (
    SELECT
        A11.C0 C0,
        A11.C1 C1,
        A11.C2 C2
    FROM
        (
            SELECT
                A12.C0 C0,
                A12.C1 C1,
                A12.C2 C2,
                SUM(A12.C3) C3,
                COUNT(*) C4
            FROM
                (
                    (
                        SELECT
                            A15.I_BRAND_ID C0,
                            A15.I_CLASS_ID C1,
                            A15.I_CATEGORY_ID C2,
                            -1 C3
                        FROM
                            (
                                (
                                    WEB_SALES A13
                                    INNER JOIN DATE_DIM A14 ON (A13.WS_SOLD_DATE_SK = A14.D_DATE_SK)
                                )
                                INNER JOIN ITEM A15 ON (A13.WS_ITEM_SK = A15.I_ITEM_SK)
                            )
                        WHERE
                            (1999 <= A14.D_YEAR)
                            AND (A14.D_YEAR <= 2001)
                    )
                    UNION
                    ALL (
                        SELECT
                            A16.C0 C0,
                            A16.C1 C1,
                            A16.C2 C2,
                            1 C3
                        FROM
                            (
                                SELECT
                                    A17.C0 C0,
                                    A17.C1 C1,
                                    A17.C2 C2,
                                    SUM(A17.C3) C3,
                                    COUNT(*) C4
                                FROM
                                    (
                                        (
                                            SELECT
                                                A20.I_BRAND_ID C0,
                                                A20.I_CLASS_ID C1,
                                                A20.I_CATEGORY_ID C2,
                                                -1 C3
                                            FROM
                                                (
                                                    (
                                                        CATALOG_SALES A18
                                                        INNER JOIN DATE_DIM A19 ON (A18.CS_SOLD_DATE_SK = A19.D_DATE_SK)
                                                    )
                                                    INNER JOIN ITEM A20 ON (A18.CS_ITEM_SK = A20.I_ITEM_SK)
                                                )
                                            WHERE
                                                (1999 <= A19.D_YEAR)
                                                AND (A19.D_YEAR <= 2001)
                                        )
                                        UNION
                                        ALL (
                                            SELECT
                                                A23.I_BRAND_ID C0,
                                                A23.I_CLASS_ID C1,
                                                A23.I_CATEGORY_ID C2,
                                                1 C3
                                            FROM
                                                (
                                                    (
                                                        STORE_SALES A21
                                                        INNER JOIN DATE_DIM A22 ON (A21.SS_SOLD_DATE_SK = A22.D_DATE_SK)
                                                    )
                                                    INNER JOIN ITEM A23 ON (A21.SS_ITEM_SK = A23.I_ITEM_SK)
                                                )
                                            WHERE
                                                (1999 <= A22.D_YEAR)
                                                AND (A22.D_YEAR <= 2001)
                                        )
                                    ) A17
                                GROUP BY
                                    A17.C2,
                                    A17.C1,
                                    A17.C0
                            ) A16
                        WHERE
                            (
                                (
                                    A16.C4 - CASE
                                        WHEN (A16.C3 >= 0) THEN A16.C3
                                        ELSE (-(A16.C3))
                                    END
                                ) >= 2
                            )
                    )
                ) A12
            GROUP BY
                A12.C2,
                A12.C1,
                A12.C0
        ) A11
    WHERE
        (
            (
                A11.C4 - CASE
                    WHEN (A11.C3 >= 0) THEN A11.C3
                    ELSE (-(A11.C3))
                END
            ) >= 2
        )
),
A25 AS (
    SELECT
        SUM((A26.C0 * A26.C1)) C0,
        COUNT((A26.C0 * A26.C1)) C1
    FROM
        (
            (
                SELECT
                    A27.SS_QUANTITY C0,
                    A27.SS_LIST_PRICE C1
                FROM
                    (
                        STORE_SALES A27
                        INNER JOIN DATE_DIM A28 ON (A27.SS_SOLD_DATE_SK = A28.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A28.D_YEAR)
                    AND (A28.D_YEAR <= 2001)
            )
            UNION
            ALL (
                SELECT
                    A29.CS_QUANTITY C0,
                    A29.CS_LIST_PRICE C1
                FROM
                    (
                        CATALOG_SALES A29
                        INNER JOIN DATE_DIM A30 ON (A29.CS_SOLD_DATE_SK = A30.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A30.D_YEAR)
                    AND (A30.D_YEAR <= 2001)
            )
            UNION
            ALL (
                SELECT
                    A31.WS_QUANTITY C0,
                    A31.WS_LIST_PRICE C1
                FROM
                    (
                        WEB_SALES A31
                        INNER JOIN DATE_DIM A32 ON (A31.WS_SOLD_DATE_SK = A32.D_DATE_SK)
                    )
                WHERE
                    (1999 <= A32.D_YEAR)
                    AND (A32.D_YEAR <= 2001)
            )
        ) A26
),
A4 AS (
    SELECT
        A5.C0 C0,
        A5.C1 C1,
        A5.C2 C2,
        A5.C3 C3,
        SUM(A5.C4) C4,
        SUM(A5.C5) C5
    FROM
        (
            (
                SELECT
                    CAST('store' AS VARCHAR(7)) C0,
                    A6.C1 C1,
                    A6.C2 C2,
                    A6.C3 C3,
                    A6.C0 C4,
                    A6.C4 C5
                FROM
                    (
                        SELECT
                            SUM((A7.SS_QUANTITY * A7.SS_LIST_PRICE)) C0,
                            A9.I_BRAND_ID C1,
                            A9.I_CLASS_ID C2,
                            A9.I_CATEGORY_ID C3,
                            COUNT(*) C4
                        FROM
                            (
                                (
                                    STORE_SALES A7
                                    INNER JOIN DATE_DIM A8 ON (A7.SS_SOLD_DATE_SK = A8.D_DATE_SK)
                                )
                                INNER JOIN (
                                    ITEM A9
                                    INNER JOIN A10 "A24" ON (A9.I_BRAND_ID = "A24".C0)
                                    AND (A9.I_CLASS_ID = "A24".C1)
                                    AND (A9.I_CATEGORY_ID = "A24".C2)
                                ) ON (A7.SS_ITEM_SK = A9.I_ITEM_SK)
                            )
                        WHERE
                            (A8.D_YEAR = 2001)
                            AND (A8.D_MOY = 11)
                        GROUP BY
                            A9.I_BRAND_ID,
                            A9.I_CLASS_ID,
                            A9.I_CATEGORY_ID
                    ) A6,
                    A25 "A33"
                WHERE
                    (("A33".C0 / "A33".C1) < A6.C0)
            )
            UNION
            ALL (
                SELECT
                    'catalog' C0,
                    A34.C1 C1,
                    A34.C2 C2,
                    A34.C3 C3,
                    A34.C0 C4,
                    A34.C4 C5
                FROM
                    (
                        SELECT
                            SUM((A35.CS_QUANTITY * A35.CS_LIST_PRICE)) C0,
                            A37.I_BRAND_ID C1,
                            A37.I_CLASS_ID C2,
                            A37.I_CATEGORY_ID C3,
                            COUNT(*) C4
                        FROM
                            (
                                (
                                    CATALOG_SALES A35
                                    INNER JOIN DATE_DIM A36 ON (A35.CS_SOLD_DATE_SK = A36.D_DATE_SK)
                                )
                                INNER JOIN (
                                    ITEM A37
                                    INNER JOIN A10 "A38" ON (A37.I_BRAND_ID = "A38".C0)
                                    AND (A37.I_CLASS_ID = "A38".C1)
                                    AND (A37.I_CATEGORY_ID = "A38".C2)
                                ) ON (A35.CS_ITEM_SK = A37.I_ITEM_SK)
                            )
                        WHERE
                            (A36.D_YEAR = 2001)
                            AND (A36.D_MOY = 11)
                        GROUP BY
                            A37.I_BRAND_ID,
                            A37.I_CLASS_ID,
                            A37.I_CATEGORY_ID
                    ) A34,
                    A25 "A39"
                WHERE
                    (("A39".C0 / "A39".C1) < A34.C0)
            )
            UNION
            ALL (
                SELECT
                    CAST('web' AS VARCHAR(7)) C0,
                    A41.C1 C1,
                    A41.C2 C2,
                    A41.C3 C3,
                    A41.C0 C4,
                    A41.C4 C5
                FROM
                    A25 "A40",
                    (
                        SELECT
                            SUM((A42.WS_QUANTITY * A42.WS_LIST_PRICE)) C0,
                            A44.I_BRAND_ID C1,
                            A44.I_CLASS_ID C2,
                            A44.I_CATEGORY_ID C3,
                            COUNT(*) C4
                        FROM
                            (
                                (
                                    WEB_SALES A42
                                    INNER JOIN DATE_DIM A43 ON (A42.WS_SOLD_DATE_SK = A43.D_DATE_SK)
                                )
                                INNER JOIN (
                                    ITEM A44
                                    INNER JOIN A10 "A45" ON (A44.I_CATEGORY_ID = "A45".C2)
                                    AND (A44.I_CLASS_ID = "A45".C1)
                                    AND (A44.I_BRAND_ID = "A45".C0)
                                ) ON (A42.WS_ITEM_SK = A44.I_ITEM_SK)
                            )
                        WHERE
                            (A43.D_YEAR = 2001)
                            AND (A43.D_MOY = 11)
                        GROUP BY
                            A44.I_BRAND_ID,
                            A44.I_CLASS_ID,
                            A44.I_CATEGORY_ID
                    ) A41
                WHERE
                    (("A40".C0 / "A40".C1) < A41.C0)
            )
        ) A5
    GROUP BY
        A5.C0,
        A5.C1,
        A5.C2,
        A5.C3
),
A3 AS (
    SELECT
        "A46".C0 C0,
        "A46".C1 C1,
        "A46".C2 C2,
        NULL C3,
        SUM("A46".C4) C4,
        SUM("A46".C5) C5
    FROM
        A4 "A46"
    GROUP BY
        "A46".C0,
        "A46".C1,
        "A46".C2
),
A2 AS (
    SELECT
        "A47".C0 C0,
        "A47".C1 C1,
        NULL C2,
        NULL C3,
        SUM("A47".C4) C4,
        SUM("A47".C5) C5
    FROM
        A3 "A47"
    GROUP BY
        "A47".C0,
        "A47".C1
),
A1 AS (
    SELECT
        "A48".C0 C0,
        NULL C1,
        NULL C2,
        NULL C3,
        SUM("A48".C4) C4,
        SUM("A48".C5) C5
    FROM
        A2 "A48"
    GROUP BY
        "A48".C0
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
                NULL C0,
                NULL C1,
                NULL C2,
                NULL C3,
                SUM("A49".C4) C4,
                SUM("A49".C5) C5
            FROM
                A1 "A49"
        )
        UNION
        ALL (
            SELECT
                "A50".C0 C0,
                "A50".C1 C1,
                "A50".C2 C2,
                "A50".C3 C3,
                "A50".C4 C4,
                "A50".C5 C5
            FROM
                A1 "A50"
        )
        UNION
        ALL (
            SELECT
                "A51".C0 C0,
                "A51".C1 C1,
                "A51".C2 C2,
                "A51".C3 C3,
                "A51".C4 C4,
                "A51".C5 C5
            FROM
                A2 "A51"
        )
        UNION
        ALL (
            SELECT
                "A52".C0 C0,
                "A52".C1 C1,
                "A52".C2 C2,
                "A52".C3 C3,
                "A52".C4 C4,
                "A52".C5 C5
            FROM
                A3 "A52"
        )
        UNION
        ALL (
            SELECT
                "A53".C0 C0,
                "A53".C1 C1,
                "A53".C2 C2,
                "A53".C3 C3,
                "A53".C4 C4,
                "A53".C5 C5
            FROM
                A4 "A53"
        )
    ) A0
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC
limit
    100;