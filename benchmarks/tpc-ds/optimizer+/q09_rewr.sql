SELECT
    CASE
        WHEN (A0.C20 > 31002) THEN (A0.C21 / A0.C22)
        ELSE (A0.C23 / A0.C24)
    END "BUCKET1",
    CASE
        WHEN (A0.C15 > 588) THEN (A0.C16 / A0.C17)
        ELSE (A0.C18 / A0.C19)
    END "BUCKET2",
    CASE
        WHEN (A0.C10 > 2456) THEN (A0.C11 / A0.C12)
        ELSE (A0.C13 / A0.C14)
    END "BUCKET3",
    CASE
        WHEN (A0.C5 > 21645) THEN (A0.C6 / A0.C7)
        ELSE (A0.C8 / A0.C9)
    END "BUCKET4",
    CASE
        WHEN (A0.C0 > 20553) THEN (A0.C1 / A0.C2)
        ELSE (A0.C3 / A0.C4)
    END "BUCKET5"
FROM
    (
        SELECT
            COUNT(
                CASE
                    WHEN (
                        (81 <= A1.SS_QUANTITY)
                        AND (A1.SS_QUANTITY <= 100)
                    ) THEN 1
                    ELSE NULL
                END
            ) C0,
            SUM(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (81 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 100)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C1,
            COUNT(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (81 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 100)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C2,
            SUM(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (81 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 100)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C3,
            COUNT(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (81 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 100)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C4,
            COUNT(
                CASE
                    WHEN (
                        (61 <= A1.SS_QUANTITY)
                        AND (A1.SS_QUANTITY <= 80)
                    ) THEN 1
                    ELSE NULL
                END
            ) C5,
            SUM(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (61 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 80)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C6,
            COUNT(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (61 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 80)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C7,
            SUM(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (61 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 80)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C8,
            COUNT(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (61 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 80)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C9,
            COUNT(
                CASE
                    WHEN (
                        (41 <= A1.SS_QUANTITY)
                        AND (A1.SS_QUANTITY <= 60)
                    ) THEN 1
                    ELSE NULL
                END
            ) C10,
            SUM(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (41 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 60)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C11,
            COUNT(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (41 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 60)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C12,
            SUM(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (41 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 60)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C13,
            COUNT(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (41 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 60)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C14,
            COUNT(
                CASE
                    WHEN (
                        (21 <= A1.SS_QUANTITY)
                        AND (A1.SS_QUANTITY <= 40)
                    ) THEN 1
                    ELSE NULL
                END
            ) C15,
            SUM(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (21 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 40)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C16,
            COUNT(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (21 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 40)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C17,
            SUM(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (21 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 40)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C18,
            COUNT(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (21 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 40)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C19,
            COUNT(
                CASE
                    WHEN (
                        (1 <= A1.SS_QUANTITY)
                        AND (A1.SS_QUANTITY <= 20)
                    ) THEN 1
                    ELSE NULL
                END
            ) C20,
            SUM(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (1 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 20)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C21,
            COUNT(
                (
                    A1.SS_EXT_DISCOUNT_AMT * CASE
                        WHEN (
                            (1 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 20)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C22,
            SUM(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (1 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 20)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C23,
            COUNT(
                (
                    A1.SS_NET_PROFIT * CASE
                        WHEN (
                            (1 <= A1.SS_QUANTITY)
                            AND (A1.SS_QUANTITY <= 20)
                        ) THEN 1
                        ELSE NULL
                    END
                )
            ) C24
        FROM
            STORE_SALES A1
        WHERE
            (A1.SS_QUANTITY <= 100)
            AND (1 <= A1.SS_QUANTITY)
    ) A0,
    REASON A2
WHERE
    (A2.R_REASON_SK = 1);