SELECT
    A0.C0 `GROSS_MARGIN`,
    A0.C1 `I_CATEGORY`,
    A0.C2 `I_CLASS`,
    A0.C3 `LOCHIERARCHY`,
    RANK() OVER(
        PARTITION BY A0.C3,
        A0.C5
        ORDER BY
            A0.C0 ASC
    ) `RANK_WITHIN_PARENT`
FROM
    (
        SELECT
            (A1.C0 / A1.C1) C0,
            A1.C2 C1,
            A1.C3 C2,
            (
                CASE
                    WHEN (A1.C6 IS NOT NULL) THEN A1.C4
                    ELSE 1
                END + CASE
                    WHEN (A1.C6 IS NOT NULL) THEN A1.C5
                    ELSE 1
                END
            ) C3,
            CASE
                WHEN (
                    (
                        CASE
                            WHEN (A1.C6 IS NOT NULL) THEN A1.C4
                            ELSE 1
                        END + CASE
                            WHEN (A1.C6 IS NOT NULL) THEN A1.C5
                            ELSE 1
                        END
                    ) = 0
                ) THEN A1.C2
                ELSE NULL
            END C4,
            CASE
                WHEN (
                    CASE
                        WHEN (A1.C6 IS NOT NULL) THEN A1.C5
                        ELSE 1
                    END = 0
                ) THEN A1.C2
                ELSE NULL
            END C5
        FROM
            (
                SELECT
                    A3.C0 C0,
                    A3.C1 C1,
                    A3.C2 C2,
                    A3.C3 C3,
                    CASE
                        WHEN (A3.C4 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A3.C4 < 3) THEN 0
                            ELSE 1
                        END
                    END C4,
                    CASE
                        WHEN (A3.C4 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A3.C4 < 2) THEN 0
                            ELSE 1
                        END
                    END C5,
                    A3.C4 C6,
                    A2.C0 C7
                FROM
                    (
                        (
                            VALUES
                                1,
                                2,
                                3
                        ) A2 (C0)
                        LEFT OUTER JOIN (
                            SELECT
                                SUM(A4.C2) C0,
                                SUM(A4.C3) C1,
                                A4.C0 C2,
                                A4.C1 C3,
                                A4.C4 C4
                            FROM
                                (
                                    SELECT
                                        CASE
                                            WHEN (A10.C0 < 3) THEN A5.C0
                                            ELSE NULL
                                        END C0,
                                        CASE
                                            WHEN (A10.C0 < 2) THEN A5.C1
                                            ELSE NULL
                                        END C1,
                                        A5.C2 C2,
                                        A5.C3 C3,
                                        A10.C0 C4
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A9.I_CATEGORY C0,
                                                    A9.I_CLASS C1,
                                                    SUM(A6.SS_NET_PROFIT) C2,
                                                    SUM(A6.SS_EXT_SALES_PRICE) C3
                                                FROM
                                                    (
                                                        (
                                                            (
                                                                store_sales A6
                                                                INNER JOIN store A7 ON (
                                                                    (A7.S_STORE_SK = A6.SS_STORE_SK)
                                                                    AND (
                                                                        (A6.SS_STORE_SK <= 1472)
                                                                        AND (A6.SS_STORE_SK >= 29)
                                                                    )
                                                                    AND (
                                                                        (A6.SS_SOLD_DATE_SK <= 2452275)
                                                                        AND (A6.SS_SOLD_DATE_SK >= 2451911)
                                                                    )
                                                                    AND (A7.S_STATE = 'TN')
                                                                )
                                                            )
                                                            INNER JOIN date_dim A8 ON (
                                                                (A8.D_DATE_SK = A6.SS_SOLD_DATE_SK)
                                                                AND (A8.D_YEAR = 2001)
                                                            )
                                                        )
                                                        INNER JOIN item A9 ON (A9.I_ITEM_SK = A6.SS_ITEM_SK)
                                                    )
                                                GROUP BY
                                                    A9.I_CATEGORY,
                                                    A9.I_CLASS
                                            ) A5
                                            INNER JOIN (
                                                VALUES
                                                    1,
                                                    2,
                                                    3
                                            ) A10 (C0) ON (1 = 1)
                                        )
                                ) A4
                            GROUP BY
                                A4.C4,
                                A4.C0,
                                A4.C1
                        ) A3 ON (A2.C0 = A3.C4)
                    )
            ) A1
        WHERE
            (
                (
                    (A1.C7 = 3)
                    AND (A1.C6 IS NULL)
                )
                OR (A1.C6 IS NOT NULL)
            )
    ) A0
ORDER BY
    4 DESC,
    A0.C4 ASC,
    5 ASC
limit
    100