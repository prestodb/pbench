SELECT
    A0.C0 "TOTAL_SUM",
    A0.C1 "S_STATE",
    A0.C2 "S_COUNTY",
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
                                CASE
                                    WHEN (A13.C0 = 1) THEN A4.C0
                                    WHEN (A13.C0 = 2) THEN A4.C0
                                    ELSE NULL
                                END C1,
                                CASE
                                    WHEN (A13.C0 = 1) THEN A4.C1
                                    WHEN (A13.C0 = 2) THEN NULL
                                    ELSE NULL
                                END C2,
                                A13.C0 C3
                            FROM
                                (
                                    (
                                        SELECT
                                            A7.S_STATE C0,
                                            A7.S_COUNTY C1,
                                            SUM(A5.SS_NET_PROFIT) C2
                                        FROM
                                            (
                                                (
                                                    STORE_SALES A5
                                                    INNER JOIN DATE_DIM A6 ON (A6.D_DATE_SK = A5.SS_SOLD_DATE_SK)
                                                )
                                                INNER JOIN (
                                                    STORE A7
                                                    INNER JOIN (
                                                        SELECT
                                                            A9.C0 C0,
                                                            RANK() OVER(
                                                                PARTITION BY A9.C0
                                                                ORDER BY
                                                                    A9.C1 DESC
                                                            ) C1
                                                        FROM
                                                            (
                                                                SELECT
                                                                    A12.S_STATE C0,
                                                                    SUM(A10.SS_NET_PROFIT) C1
                                                                FROM
                                                                    (
                                                                        (
                                                                            STORE_SALES A10
                                                                            INNER JOIN DATE_DIM A11 ON (A11.D_DATE_SK = A10.SS_SOLD_DATE_SK)
                                                                        )
                                                                        INNER JOIN STORE A12 ON (A12.S_STORE_SK = A10.SS_STORE_SK)
                                                                    )
                                                                WHERE
                                                                    (1180 <= A11.D_MONTH_SEQ)
                                                                    AND (A11.D_MONTH_SEQ <= 1191)
                                                                GROUP BY
                                                                    A12.S_STATE
                                                            ) A9
                                                    ) A8 ON (A7.S_STATE = A8.C0)
                                                ) ON (A7.S_STORE_SK = A5.SS_STORE_SK)
                                            )
                                        WHERE
                                            (1180 <= A6.D_MONTH_SEQ)
                                            AND (A6.D_MONTH_SEQ <= 1191)
                                            AND (A8.C1 <= 5)
                                        GROUP BY
                                            A7.S_STATE,
                                            A7.S_COUNTY
                                    ) A4
                                    INNER JOIN (
                                        VALUES
                                            1,
                                            2,
                                            3
                                    ) A13 (C0) ON (
                                        MOD(LENGTH(A13.C0), 1) = COALESCE(MOD(LENGTH(A4.C0), 1), 0)
                                    )
                                )
                            GROUP BY
                                A13.C0,
                                CASE
                                    WHEN (A13.C0 = 1) THEN A4.C0
                                    WHEN (A13.C0 = 2) THEN A4.C0
                                    ELSE NULL
                                END,
                                CASE
                                    WHEN (A13.C0 = 1) THEN A4.C1
                                    WHEN (A13.C0 = 2) THEN NULL
                                    ELSE NULL
                                END
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
    6 ASC,
    5 ASC
limit
    100;