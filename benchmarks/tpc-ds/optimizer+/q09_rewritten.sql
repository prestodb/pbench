WITH A0 AS (
    SELECT
        COUNT(*) C0,
        SUM(A1.SS_EXT_DISCOUNT_AMT) C1,
        COUNT(A1.SS_EXT_DISCOUNT_AMT) C2,
        COUNT(A1.SS_NET_PROFIT) C3,
        SUM(A1.SS_NET_PROFIT) C4
    FROM
        STORE_SALES A1
    WHERE
        (61 <= A1.SS_QUANTITY)
        AND (A1.SS_QUANTITY <= 80)
),
A3 AS (
    SELECT
        COUNT(*) C0,
        COUNT(A4.SS_NET_PROFIT) C1,
        SUM(A4.SS_NET_PROFIT) C2
    FROM
        STORE_SALES A4
    WHERE
        (41 <= A4.SS_QUANTITY)
        AND (A4.SS_QUANTITY <= 60)
),
A7 AS (
    SELECT
        COUNT(*) C0,
        SUM(A8.SS_EXT_DISCOUNT_AMT) C1,
        COUNT(A8.SS_EXT_DISCOUNT_AMT) C2,
        COUNT(A8.SS_NET_PROFIT) C3,
        SUM(A8.SS_NET_PROFIT) C4
    FROM
        STORE_SALES A8
    WHERE
        (81 <= A8.SS_QUANTITY)
        AND (A8.SS_QUANTITY <= 100)
)
SELECT
    CASE
        WHEN (
            (
                SELECT
                    COUNT(*)
                FROM
                    STORE_SALES A15
                WHERE
                    (1 <= A15.SS_QUANTITY)
                    AND (A15.SS_QUANTITY <= 20)
            ) > 31002
        ) THEN (
            (
                SELECT
                    SUM(A16.SS_EXT_DISCOUNT_AMT)
                FROM
                    STORE_SALES A16
                WHERE
                    (1 <= A16.SS_QUANTITY)
                    AND (A16.SS_QUANTITY <= 20)
            ) / (
                SELECT
                    COUNT(A17.SS_EXT_DISCOUNT_AMT)
                FROM
                    STORE_SALES A17
                WHERE
                    (1 <= A17.SS_QUANTITY)
                    AND (A17.SS_QUANTITY <= 20)
            )
        )
        ELSE (
            (
                SELECT
                    SUM(A18.SS_NET_PROFIT)
                FROM
                    STORE_SALES A18
                WHERE
                    (1 <= A18.SS_QUANTITY)
                    AND (A18.SS_QUANTITY <= 20)
            ) / (
                SELECT
                    COUNT(A19.SS_NET_PROFIT)
                FROM
                    STORE_SALES A19
                WHERE
                    (1 <= A19.SS_QUANTITY)
                    AND (A19.SS_QUANTITY <= 20)
            )
        )
    END "BUCKET1",
    CASE
        WHEN (
            (
                SELECT
                    COUNT(*)
                FROM
                    STORE_SALES A20
                WHERE
                    (21 <= A20.SS_QUANTITY)
                    AND (A20.SS_QUANTITY <= 40)
            ) > 588
        ) THEN (
            (
                SELECT
                    SUM(A21.SS_EXT_DISCOUNT_AMT)
                FROM
                    STORE_SALES A21
                WHERE
                    (21 <= A21.SS_QUANTITY)
                    AND (A21.SS_QUANTITY <= 40)
            ) / (
                SELECT
                    COUNT(A22.SS_EXT_DISCOUNT_AMT)
                FROM
                    STORE_SALES A22
                WHERE
                    (21 <= A22.SS_QUANTITY)
                    AND (A22.SS_QUANTITY <= 40)
            )
        )
        ELSE (
            (
                SELECT
                    SUM(A23.SS_NET_PROFIT)
                FROM
                    STORE_SALES A23
                WHERE
                    (21 <= A23.SS_QUANTITY)
                    AND (A23.SS_QUANTITY <= 40)
            ) / (
                SELECT
                    COUNT(A24.SS_NET_PROFIT)
                FROM
                    STORE_SALES A24
                WHERE
                    (21 <= A24.SS_QUANTITY)
                    AND (A24.SS_QUANTITY <= 40)
            )
        )
    END "BUCKET2",
    CASE
        WHEN ("A5".C0 > 2456) THEN (
            (
                SELECT
                    SUM(A25.SS_EXT_DISCOUNT_AMT)
                FROM
                    STORE_SALES A25
                WHERE
                    (41 <= A25.SS_QUANTITY)
                    AND (A25.SS_QUANTITY <= 60)
            ) / (
                SELECT
                    COUNT(A26.SS_EXT_DISCOUNT_AMT)
                FROM
                    STORE_SALES A26
                WHERE
                    (41 <= A26.SS_QUANTITY)
                    AND (A26.SS_QUANTITY <= 60)
            )
        )
        ELSE ("A14".C2 / "A14".C1)
    END "BUCKET3",
    CASE
        WHEN ("A2".C0 > 21645) THEN ("A13".C1 / "A13".C2)
        ELSE ("A12".C4 / "A12".C3)
    END "BUCKET4",
    CASE
        WHEN ("A9".C0 > 20553) THEN ("A11".C1 / "A11".C2)
        ELSE ("A10".C4 / "A10".C3)
    END "BUCKET5"
FROM
    A0 "A2",
    A3 "A5",
    REASON A6,
    A7 "A9",
    A7 "A10",
    A7 "A11",
    A0 "A12",
    A0 "A13",
    A3 "A14"
WHERE
    (A6.R_REASON_SK = 1);