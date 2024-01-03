WITH A3 AS (
    SELECT
        SUM(A4.SS_NET_PROFIT) C0,
        A6.S_STATE C1,
        A6.S_COUNTY C2,
        0 C3,
        0 C4
    FROM
        (
            (
                STORE_SALES A4
                INNER JOIN DATE_DIM A5 ON (A5.D_DATE_SK = A4.SS_SOLD_DATE_SK)
            )
            INNER JOIN (
                STORE A6
                INNER JOIN (
                    SELECT
                        A8.C0 C0,
                        RANK() OVER(
                            PARTITION BY A8.C0
                            ORDER BY
                                A8.C1 DESC
                        ) C1
                    FROM
                        (
                            SELECT
                                A11.S_STATE C0,
                                SUM(A9.SS_NET_PROFIT) C1
                            FROM
                                (
                                    (
                                        STORE_SALES A9
                                        INNER JOIN DATE_DIM A10 ON (A10.D_DATE_SK = A9.SS_SOLD_DATE_SK)
                                    )
                                    INNER JOIN STORE A11 ON (A11.S_STORE_SK = A9.SS_STORE_SK)
                                )
                            WHERE
                                (1180 <= A10.D_MONTH_SEQ)
                                AND (A10.D_MONTH_SEQ <= 1191)
                            GROUP BY
                                A11.S_STATE
                        ) A8
                ) A7 ON (A6.S_STATE = A7.C0)
            ) ON (A6.S_STORE_SK = A4.SS_STORE_SK)
        )
    WHERE
        (1180 <= A5.D_MONTH_SEQ)
        AND (A5.D_MONTH_SEQ <= 1191)
        AND (A7.C1 <= 5)
    GROUP BY
        A6.S_STATE,
        A6.S_COUNTY
),
A2 AS (
    SELECT
        SUM("A12".C0) C0,
        "A12".C1 C1,
        NULL C2,
        0 C3,
        1 C4
    FROM
        A3 "A12"
    GROUP BY
        "A12".C1
)
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
                        SUM("A13".C0) C0,
                        NULL C1,
                        NULL C2,
                        1 C3,
                        1 C4
                    FROM
                        A2 "A13"
                )
                UNION
                ALL (
                    SELECT
                        "A14".C0 C0,
                        "A14".C1 C1,
                        "A14".C2 C2,
                        "A14".C3 C3,
                        "A14".C4 C4
                    FROM
                        A2 "A14"
                )
                UNION
                ALL (
                    SELECT
                        "A15".C0 C0,
                        "A15".C1 C1,
                        "A15".C2 C2,
                        "A15".C3 C3,
                        "A15".C4 C4
                    FROM
                        A3 "A15"
                )
            ) A1
    ) A0
ORDER BY
    4 DESC,
    6 ASC,
    5 ASC
limit
    100;