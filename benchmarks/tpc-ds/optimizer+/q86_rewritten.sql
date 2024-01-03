WITH A3 AS (
    SELECT
        SUM(A4.WS_NET_PAID) C0,
        A6.I_CATEGORY C1,
        A6.I_CLASS C2,
        0 C3,
        0 C4
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
),
A2 AS (
    SELECT
        SUM("A7".C0) C0,
        "A7".C1 C1,
        NULL C2,
        0 C3,
        1 C4
    FROM
        A3 "A7"
    GROUP BY
        "A7".C1
)
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
            (A1.C3 + A1.C4) C3,
            CASE
                WHEN ((A1.C3 + A1.C4) = 0) THEN A1.C1
                ELSE NULL
            END C4,
            CASE
                WHEN (A1.C4 = 0) THEN A1.C1
                ELSE NULL
            END C5
        FROM
            (
                (
                    SELECT
                        SUM("A8".C0) C0,
                        NULL C1,
                        NULL C2,
                        1 C3,
                        1 C4
                    FROM
                        A2 "A8"
                )
                UNION
                ALL (
                    SELECT
                        "A9".C0 C0,
                        "A9".C1 C1,
                        "A9".C2 C2,
                        "A9".C3 C3,
                        "A9".C4 C4
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
                        "A10".C4 C4
                    FROM
                        A3 "A10"
                )
            ) A1
    ) A0
ORDER BY
    4 DESC,
    6 ASC,
    5 ASC
limit
    100;