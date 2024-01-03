SELECT
    A0.C0 "TOTAL_SUM",
    A0.C1 "I_CATEGORY",
    A0.C2 "I_CLASS",
    A0.C3 "LOCHIERARCHY",
    RANK() OVER(
        PARTITION BY A0.C3,
        A0.C5
        ORDER BY
            A0.C0 DESC
    ) "RANK_WITHIN_PARENT",
    A0.C4
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
                    A2.C0 C0,
                    A2.C1 C1,
                    A2.C2 C2,
                    CASE
                        WHEN (A2.C3 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A2.C3 < 3) THEN 0
                            ELSE 1
                        END
                    END C3,
                    CASE
                        WHEN (A2.C3 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A2.C3 < 2) THEN 0
                            ELSE 1
                        END
                    END C4,
                    A2.C3 C5,
                    A8.C0 C6
                FROM
                    (
                        (
                            SELECT
                                SUM(A3.C2) C0,
                                CASE
                                    WHEN (A7.C0 = 1) THEN A3.C0
                                    WHEN (A7.C0 = 2) THEN A3.C0
                                    ELSE NULL
                                END C1,
                                CASE
                                    WHEN (A7.C0 = 1) THEN A3.C1
                                    WHEN (A7.C0 = 2) THEN NULL
                                    ELSE NULL
                                END C2,
                                A7.C0 C3
                            FROM
                                (
                                    (
                                        SELECT
                                            A6.I_CATEGORY C0,
                                            A6.I_CLASS C1,
                                            SUM(A4.WS_NET_PAID) C2
                                        FROM
                                            (
                                                (
                                                    WEB_SALES A4
                                                    INNER JOIN DATE_DIM A5 ON (A5.D_DATE_SK = A4.WS_SOLD_DATE_SK)
                                                )
                                                INNER JOIN ITEM A6 ON (A6.I_ITEM_SK = A4.WS_ITEM_SK)
                                            )
                                        WHERE
                                            (1205 <= A5.D_MONTH_SEQ)
                                            AND (A5.D_MONTH_SEQ <= 1216)
                                        GROUP BY
                                            A6.I_CATEGORY,
                                            A6.I_CLASS
                                    ) A3
                                    INNER JOIN (
                                        VALUES
                                            1,
                                            2,
                                            3
                                    ) A7 (C0) ON (
                                        MOD(LENGTH(A7.C0), 1) = COALESCE(MOD(LENGTH(A3.C0), 1), 0)
                                    )
                                )
                            GROUP BY
                                A7.C0,
                                CASE
                                    WHEN (A7.C0 = 1) THEN A3.C0
                                    WHEN (A7.C0 = 2) THEN A3.C0
                                    ELSE NULL
                                END,
                                CASE
                                    WHEN (A7.C0 = 1) THEN A3.C1
                                    WHEN (A7.C0 = 2) THEN NULL
                                    ELSE NULL
                                END
                        ) A2
                        RIGHT OUTER JOIN (
                            VALUES
                                1,
                                2,
                                3
                        ) A8 (C0) ON (A8.C0 = A2.C3)
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
    6 ASC,
    5 ASC
limit
    100;