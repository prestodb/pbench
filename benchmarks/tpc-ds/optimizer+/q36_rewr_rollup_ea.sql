SELECT
    A0.C0 "GROSS_MARGIN",
    A0.C1 "I_CATEGORY",
    A0.C2 "I_CLASS",
    A0.C3 "LOCHIERARCHY",
    RANK() OVER(
        PARTITION BY A0.C3,
        A0.C5
        ORDER BY
            A0.C0 ASC
    ) "RANK_WITHIN_PARENT",
    A0.C4
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
                    A2.C0 C0,
                    A2.C1 C1,
                    A2.C2 C2,
                    A2.C3 C3,
                    CASE
                        WHEN (A2.C4 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A2.C4 < 3) THEN 0
                            ELSE 1
                        END
                    END C4,
                    CASE
                        WHEN (A2.C4 IS NULL) THEN NULL
                        ELSE CASE
                            WHEN (A2.C4 < 2) THEN 0
                            ELSE 1
                        END
                    END C5,
                    A2.C4 C6,
                    A9.C0 C7
                FROM
                    (
                        (
                            SELECT
                                SUM(A3.C2) C0,
                                SUM(A3.C3) C1,
                                CASE
                                    WHEN (A8.C0 = 1) THEN A3.C0
                                    WHEN (A8.C0 = 2) THEN A3.C0
                                    ELSE NULL
                                END C2,
                                CASE
                                    WHEN (A8.C0 = 1) THEN A3.C1
                                    WHEN (A8.C0 = 2) THEN NULL
                                    ELSE NULL
                                END C3,
                                A8.C0 C4
                            FROM
                                (
                                    (
                                        SELECT
                                            A7.I_CATEGORY C0,
                                            A7.I_CLASS C1,
                                            SUM(A4.SS_NET_PROFIT) C2,
                                            SUM(A4.SS_EXT_SALES_PRICE) C3
                                        FROM
                                            (
                                                (
                                                    (
                                                        STORE_SALES A4
                                                        INNER JOIN DATE_DIM A5 ON (A5.D_DATE_SK = A4.SS_SOLD_DATE_SK)
                                                    )
                                                    INNER JOIN STORE A6 ON (A6.S_STORE_SK = A4.SS_STORE_SK)
                                                )
                                                INNER JOIN ITEM A7 ON (A7.I_ITEM_SK = A4.SS_ITEM_SK)
                                            )
                                        WHERE
                                            (A5.D_YEAR = 2001)
                                            AND (A6.S_STATE = 'TN')
                                        GROUP BY
                                            A7.I_CATEGORY,
                                            A7.I_CLASS
                                    ) A3
                                    INNER JOIN (
                                        VALUES
                                            1,
                                            2,
                                            3
                                    ) A8 (C0) ON (
                                        MOD(LENGTH(A8.C0), 1) = COALESCE(MOD(LENGTH(A3.C0), 1), 0)
                                    )
                                )
                            GROUP BY
                                A8.C0,
                                CASE
                                    WHEN (A8.C0 = 1) THEN A3.C0
                                    WHEN (A8.C0 = 2) THEN A3.C0
                                    ELSE NULL
                                END,
                                CASE
                                    WHEN (A8.C0 = 1) THEN A3.C1
                                    WHEN (A8.C0 = 2) THEN NULL
                                    ELSE NULL
                                END
                        ) A2
                        RIGHT OUTER JOIN (
                            VALUES
                                1,
                                2,
                                3
                        ) A9 (C0) ON (A9.C0 = A2.C4)
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
    6 ASC,
    5 ASC
limit
    100;