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
                    A8.C0 C7
                FROM
                    (
                        (
                            SELECT
                                SUM(A5.SS_NET_PROFIT) C0,
                                SUM(A5.SS_EXT_SALES_PRICE) C1,
                                CASE
                                    WHEN (A4.C0 = 1) THEN A3.I_CATEGORY
                                    WHEN (A4.C0 = 2) THEN A3.I_CATEGORY
                                    ELSE NULL
                                END C2,
                                CASE
                                    WHEN (A4.C0 = 1) THEN A3.I_CLASS
                                    WHEN (A4.C0 = 2) THEN NULL
                                    ELSE NULL
                                END C3,
                                A4.C0 C4
                            FROM
                                (
                                    (
                                        ITEM A3
                                        INNER JOIN (
                                            VALUES
                                                1,
                                                2,
                                                3
                                        ) A4 (C0) ON (
                                            MOD(LENGTH(A4.C0), 1) = COALESCE(MOD(LENGTH(A3.I_CATEGORY), 1), 0)
                                        )
                                    )
                                    INNER JOIN (
                                        (
                                            STORE_SALES A5
                                            INNER JOIN DATE_DIM A6 ON (A6.D_DATE_SK = A5.SS_SOLD_DATE_SK)
                                        )
                                        INNER JOIN STORE A7 ON (A7.S_STORE_SK = A5.SS_STORE_SK)
                                    ) ON (A3.I_ITEM_SK = A5.SS_ITEM_SK)
                                )
                            WHERE
                                (A6.D_YEAR = 2001)
                                AND (A7.S_STATE = 'TN')
                            GROUP BY
                                A4.C0,
                                CASE
                                    WHEN (A4.C0 = 1) THEN A3.I_CATEGORY
                                    WHEN (A4.C0 = 2) THEN A3.I_CATEGORY
                                    ELSE NULL
                                END,
                                CASE
                                    WHEN (A4.C0 = 1) THEN A3.I_CLASS
                                    WHEN (A4.C0 = 2) THEN NULL
                                    ELSE NULL
                                END
                        ) A2
                        RIGHT OUTER JOIN (
                            VALUES
                                1,
                                2,
                                3
                        ) A8 (C0) ON (A8.C0 = A2.C4)
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