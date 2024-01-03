WITH A3 AS (
    SELECT
        SUM(A4.SS_NET_PROFIT) C0,
        SUM(A4.SS_EXT_SALES_PRICE) C1,
        A7.I_CATEGORY C2,
        A7.I_CLASS C3,
        0 C4,
        0 C5
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
),
A2 AS (
    SELECT
        SUM("A8".C0) C0,
        SUM("A8".C1) C1,
        "A8".C2 C2,
        NULL C3,
        0 C4,
        1 C5
    FROM
        A3 "A8"
    GROUP BY
        "A8".C2
)
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
            (A1.C4 + A1.C5) C3,
            CASE
                WHEN ((A1.C4 + A1.C5) = 0) THEN A1.C2
                ELSE NULL
            END C4,
            CASE
                WHEN (A1.C5 = 0) THEN A1.C2
                ELSE NULL
            END C5
        FROM
            (
                (
                    SELECT
                        SUM("A9".C0) C0,
                        SUM("A9".C1) C1,
                        NULL C2,
                        NULL C3,
                        1 C4,
                        1 C5
                    FROM
                        A2 "A9"
                )
                UNION
                ALL (
                    SELECT
                        "A10".C0 C0,
                        "A10".C1 C1,
                        "A10".C2 C2,
                        "A10".C3 C3,
                        "A10".C4 C4,
                        "A10".C5 C5
                    FROM
                        A2 "A10"
                )
                UNION
                ALL (
                    SELECT
                        "A11".C0 C0,
                        "A11".C1 C1,
                        "A11".C2 C2,
                        "A11".C3 C3,
                        "A11".C4 C4,
                        "A11".C5 C5
                    FROM
                        A3 "A11"
                )
            ) A1
    ) A0
ORDER BY
    4 DESC,
    6 ASC,
    5 ASC
limit
    100;