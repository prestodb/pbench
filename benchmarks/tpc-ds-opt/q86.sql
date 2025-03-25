SELECT
    A0.C0 as `TOTAL_SUM`,
    A0.C1 as `I_CATEGORY`,
    A0.C2 as `I_CLASS`,
    A0.C3 as `LOCHIERARCHY`,
    RANK() OVER(
        PARTITION BY A0.C3,
        A0.C5
        ORDER BY
            A0.C0 DESC
    ) as `RANK_WITHIN_PARENT`
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            (
                CASE
                    WHEN (A1.C5 IS NOT NULL) THEN A1.C3
                    ELSE 1
                END + CASE
                    WHEN (A1.C5 IS NOT NULL) THEN A1.C4
                    ELSE 1
                END
            ) C3,
            CASE
                WHEN (
                    (
                        CASE
                            WHEN (A1.C5 IS NOT NULL) THEN A1.C3
                            ELSE 1
                        END + CASE
                            WHEN (A1.C5 IS NOT NULL) THEN A1.C4
                            ELSE 1
                        END
                    ) = 0
                ) THEN A1.C1
                ELSE NULL
            END C4,
            CASE
                WHEN (
                    CASE
                        WHEN (A1.C5 IS NOT NULL) THEN A1.C4
                        ELSE 1
                    END = 0
                ) THEN A1.C1
                ELSE NULL
            END C5
        FROM
            (
                SELECT
                    A3.C0 C0,
                    A3.C1 C1,
                    A3.C2 C2,
                    CASE
                        WHEN (A3.C3 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A3.C3 < 3) THEN 0
                            ELSE 1
                        END
                    END C3,
                    CASE
                        WHEN (A3.C3 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A3.C3 < 2) THEN 0
                            ELSE 1
                        END
                    END C4,
                    A3.C3 C5,
                    A2.C0 C6
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
                                A4.C0 C1,
                                A4.C1 C2,
                                A4.C3 C3
                            FROM
                                (
                                    SELECT
                                        CASE
                                            WHEN (A9.C0 < 3) THEN A5.C0
                                            ELSE NULL
                                        END C0,
                                        CASE
                                            WHEN (A9.C0 < 2) THEN A5.C1
                                            ELSE NULL
                                        END C1,
                                        A5.C2 C2,
                                        A9.C0 C3
                                    FROM
                                        (
                                            (
                                                SELECT
                                                    A8.I_CATEGORY C0,
                                                    A8.I_CLASS C1,
                                                    SUM(A6.WS_NET_PAID) C2
                                                FROM
                                                    (
                                                        (
                                                            web_sales A6
                                                            INNER JOIN date_dim A7 ON (
                                                                (A7.D_DATE_SK = A6.WS_SOLD_DATE_SK)
                                                                AND (
                                                                    (A6.WS_SOLD_DATE_SK <= 2451910)
                                                                    AND (A6.WS_SOLD_DATE_SK >= 2451545)
                                                                )
                                                                AND (1200 <= A7.D_MONTH_SEQ)
                                                                AND (A7.D_MONTH_SEQ <= 1211)
                                                            )
                                                        )
                                                        INNER JOIN item A8 ON (A8.I_ITEM_SK = A6.WS_ITEM_SK)
                                                    )
                                                GROUP BY
                                                    A8.I_CATEGORY,
                                                    A8.I_CLASS
                                            ) A5
                                            INNER JOIN (
                                                VALUES
                                                    1,
                                                    2,
                                                    3
                                            ) A9 (C0) ON (1 = 1)
                                        )
                                ) A4
                            GROUP BY
                                A4.C3,
                                A4.C0,
                                A4.C1
                        ) A3 ON (A2.C0 = A3.C3)
                    )
            ) A1
        WHERE
            (
                (
                    (A1.C6 = 3)
                    AND (A1.C5 IS NULL)
                )
                OR (A1.C5 IS NOT NULL)
            )
    ) A0
ORDER BY
    4 DESC,
    A0.C4 ASC,
    5 ASC
limit
    100